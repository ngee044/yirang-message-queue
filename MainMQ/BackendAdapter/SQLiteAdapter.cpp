#include "SQLiteAdapter.h"

#include "Converter.h"
#include "File.h"
#include "Generator.h"

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <format>
#include <set>

using json = nlohmann::json;

namespace
{
	auto replace_all(std::string& source, const std::string& from, const std::string& to) -> void
	{
		if (from.empty())
		{
			return;
		}

		size_t offset = 0;
		while ((offset = source.find(from, offset)) != std::string::npos)
		{
			source.replace(offset, from.length(), to);
			offset += to.length();
		}
	}

	auto current_time_ms() -> int64_t
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
	}

	auto state_to_string(MessageState state) -> std::string
	{
		switch (state)
		{
		case MessageState::Ready: return "ready";
		case MessageState::Inflight: return "inflight";
		case MessageState::Delayed: return "delayed";
		case MessageState::Dlq: return "dlq";
		case MessageState::Archived: return "archived";
		default: return "unknown";
		}
	}

	auto string_to_state(const std::string& state) -> MessageState
	{
		if (state == "ready") return MessageState::Ready;
		if (state == "inflight") return MessageState::Inflight;
		if (state == "delayed") return MessageState::Delayed;
		if (state == "dlq") return MessageState::Dlq;
		if (state == "archived") return MessageState::Archived;
		return MessageState::Ready;
	}
} // namespace

SQLiteAdapter::SQLiteAdapter(const std::string& schema_path)
	: is_open_(false), schema_path_(schema_path)
{
}

SQLiteAdapter::~SQLiteAdapter(void) { close(); }

auto SQLiteAdapter::open(const BackendConfig& config) -> std::tuple<bool, std::optional<std::string>>
{
	if (config.type != BackendType::SQLite)
	{
		return { false, "backend type mismatch" };
	}

	sqlite_config_ = config.sqlite;
	if (sqlite_config_.kv_table.empty())
	{
		sqlite_config_.kv_table = "kv";
	}
	if (sqlite_config_.message_index_table.empty())
	{
		sqlite_config_.message_index_table = "msg_index";
	}
	if (sqlite_config_.db_path.empty())
	{
		return { false, "sqlite db path is empty" };
	}

	std::filesystem::path db_path(sqlite_config_.db_path);
	if (!db_path.parent_path().empty())
	{
		std::error_code error_code;
		std::filesystem::create_directories(db_path.parent_path(), error_code);
		if (error_code)
		{
			return { false, error_code.message() };
		}
	}

	auto [opened, open_message] = db_.open(sqlite_config_.db_path);
	if (!opened)
	{
		return { false, open_message };
	}

	if (sqlite_config_.busy_timeout_ms > 0)
	{
		auto [timeout_ok, timeout_message] = db_.set_busy_timeout(sqlite_config_.busy_timeout_ms);
		if (!timeout_ok)
		{
			return { false, timeout_message };
		}
	}

	auto [pragma_ok, pragma_message] = apply_pragmas();
	if (!pragma_ok)
	{
		return { false, pragma_message };
	}

	auto [schema_ok, schema_message] = ensure_schema();
	if (!schema_ok)
	{
		return { false, schema_message };
	}

	is_open_ = true;
	return { true, std::nullopt };
}

auto SQLiteAdapter::close(void) -> void
{
	is_open_ = false;
	db_.close();
}

auto SQLiteAdapter::enqueue(const MessageEnvelope& message) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto now = current_time_ms();

	// Build envelope JSON for kv table
	json envelope;
	envelope["messageId"] = message.message_id;
	envelope["queue"] = message.queue;
	envelope["payload"] = message.payload_json;
	envelope["attributes"] = message.attributes_json;
	envelope["priority"] = message.priority;
	envelope["attempt"] = message.attempt;
	envelope["createdAt"] = message.created_at_ms > 0 ? message.created_at_ms : now;

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	// Insert into kv table
	std::string kv_sql = std::format(
		"INSERT INTO {} (key, value, value_type, created_at, updated_at) VALUES (?, ?, ?, ?, ?);",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_error] = db_.prepare(kv_sql);
	if (!kv_stmt)
	{
		db_.rollback();
		return { false, kv_error };
	}

	kv_stmt->bind_text(1, message.key);
	kv_stmt->bind_text(2, envelope.dump());
	kv_stmt->bind_text(3, "message");
	kv_stmt->bind_int64(4, now);
	kv_stmt->bind_int64(5, now);

	if (kv_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to insert into kv table" };
	}

	// Insert into msg_index table
	std::string idx_sql = std::format(
		"INSERT INTO {} (queue, state, priority, available_at, attempt, target_consumer_id, message_key, expired_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_error] = db_.prepare(idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, idx_error };
	}

	auto available_at = message.available_at_ms > 0 ? message.available_at_ms : now;
	auto state = available_at > now ? MessageState::Delayed : MessageState::Ready;

	idx_stmt->bind_text(1, message.queue);
	idx_stmt->bind_text(2, state_to_string(state));
	idx_stmt->bind_int(3, message.priority);
	idx_stmt->bind_int64(4, available_at);
	idx_stmt->bind_int(5, message.attempt);
	idx_stmt->bind_text(6, message.target_consumer_id);
	idx_stmt->bind_text(7, message.key);
	idx_stmt->bind_int64(8, message.expired_at_ms);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to insert into msg_index table" };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::lease_next(const std::string& queue, const std::string& consumer_id, const int32_t& visibility_timeout_sec)
	-> LeaseResult
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	LeaseResult result;

	if (!db_.is_open())
	{
		result.error = "database is not open";
		return result;
	}

	auto now = current_time_ms();
	auto lease_until = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		result.error = tx_error;
		return result;
	}

	// Find next ready message (priority DESC, available_at ASC)
	// Filter by target_consumer_id: empty string matches any consumer, otherwise must match exactly
	// Filter expired messages: skip messages where expired_at > 0 AND expired_at <= now
	std::string select_sql = std::format(
		"SELECT message_key, priority, attempt FROM {} "
		"WHERE queue = ? AND state = 'ready' AND available_at <= ? "
		"AND (target_consumer_id = '' OR target_consumer_id = ?) "
		"AND (expired_at = 0 OR expired_at > ?) "
		"ORDER BY priority DESC, available_at ASC LIMIT 1;",
		sqlite_config_.message_index_table
	);

	auto [select_stmt, select_error] = db_.prepare(select_sql);
	if (!select_stmt)
	{
		db_.rollback();
		result.error = select_error;
		return result;
	}

	select_stmt->bind_text(1, queue);
	select_stmt->bind_int64(2, now);
	select_stmt->bind_text(3, consumer_id);
	select_stmt->bind_int64(4, now);

	int step_result = select_stmt->step();
	if (step_result != SQLITE_ROW)
	{
		db_.rollback();
		// No message available - not an error
		return result;
	}

	std::string message_key = select_stmt->column_text(0);
	int32_t priority = select_stmt->column_int(1);
	int32_t attempt = select_stmt->column_int(2);

	// Update state to inflight
	std::string update_sql = std::format(
		"UPDATE {} SET state = 'inflight', lease_until = ?, attempt = ? WHERE message_key = ?;",
		sqlite_config_.message_index_table
	);

	auto [update_stmt, update_error] = db_.prepare(update_sql);
	if (!update_stmt)
	{
		db_.rollback();
		result.error = update_error;
		return result;
	}

	update_stmt->bind_int64(1, lease_until);
	update_stmt->bind_int(2, attempt + 1);
	update_stmt->bind_text(3, message_key);

	if (update_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		result.error = "failed to update message state";
		return result;
	}

	// Fetch message content from kv table
	std::string kv_sql = std::format(
		"SELECT value FROM {} WHERE key = ?;",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_error] = db_.prepare(kv_sql);
	if (!kv_stmt)
	{
		db_.rollback();
		result.error = kv_error;
		return result;
	}

	kv_stmt->bind_text(1, message_key);

	if (kv_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		result.error = "message not found in kv table";
		return result;
	}

	std::string value_json = kv_stmt->column_text(0);

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		result.error = commit_error;
		return result;
	}

	// Parse message envelope
	try
	{
		json envelope = json::parse(value_json);

		MessageEnvelope msg;
		msg.key = message_key;
		msg.message_id = envelope.value("messageId", "");
		msg.queue = envelope.value("queue", queue);
		msg.payload_json = envelope.value("payload", "");
		msg.attributes_json = envelope.value("attributes", "");
		msg.priority = priority;
		msg.attempt = attempt + 1;
		msg.created_at_ms = envelope.value("createdAt", static_cast<int64_t>(0));

		LeaseToken lease;
		lease.lease_id = Utilities::Generator::guid();
		lease.message_key = message_key;
		lease.consumer_id = consumer_id;
		lease.lease_until_ms = lease_until;

		result.leased = true;
		result.message = msg;
		result.lease = lease;
	}
	catch (const json::exception& e)
	{
		result.error = std::format("failed to parse message: {}", e.what());
	}

	return result;
}

auto SQLiteAdapter::ack(const LeaseToken& lease) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	// Delete from msg_index (CASCADE will not delete kv, so delete explicitly)
	std::string idx_sql = std::format(
		"DELETE FROM {} WHERE message_key = ? AND state = 'inflight';",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_error] = db_.prepare(idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, idx_error };
	}

	idx_stmt->bind_text(1, lease.message_key);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to delete from msg_index" };
	}

	// Delete from kv table
	std::string kv_sql = std::format(
		"DELETE FROM {} WHERE key = ?;",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_error] = db_.prepare(kv_sql);
	if (!kv_stmt)
	{
		db_.rollback();
		return { false, kv_error };
	}

	kv_stmt->bind_text(1, lease.message_key);

	if (kv_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to delete from kv table" };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::nack(const LeaseToken& lease, const std::string& reason, const bool& requeue)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto now = current_time_ms();

	if (requeue)
	{
		auto [tx_ok, tx_error] = db_.begin_transaction();
		if (!tx_ok)
		{
			return { false, tx_error };
		}

		// Return to ready state
		std::string sql = std::format(
			"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = ? WHERE message_key = ? AND state = 'inflight';",
			sqlite_config_.message_index_table
		);

		auto [stmt, error] = db_.prepare(sql);
		if (!stmt)
		{
			db_.rollback();
			return { false, error };
		}

		stmt->bind_int64(1, now);
		stmt->bind_text(2, lease.message_key);

		if (stmt->step() != SQLITE_DONE)
		{
			db_.rollback();
			return { false, "failed to requeue message" };
		}

		auto [commit_ok, commit_error] = db_.commit();
		if (!commit_ok)
		{
			db_.rollback();
			return { false, commit_error };
		}
	}
	else
	{
		// Move to DLQ
		auto [tx_ok, tx_error] = db_.begin_transaction();
		if (!tx_ok)
		{
			return { false, tx_error };
		}

		// Update state to dlq
		std::string idx_sql = std::format(
			"UPDATE {} SET state = 'dlq', lease_until = NULL WHERE message_key = ? AND state = 'inflight';",
			sqlite_config_.message_index_table
		);

		auto [idx_stmt, idx_error] = db_.prepare(idx_sql);
		if (!idx_stmt)
		{
			db_.rollback();
			return { false, idx_error };
		}

		idx_stmt->bind_text(1, lease.message_key);

		if (idx_stmt->step() != SQLITE_DONE)
		{
			db_.rollback();
			return { false, "failed to move message to dlq" };
		}

		// Update kv value with nack reason
		std::string kv_select_sql = std::format(
			"SELECT value FROM {} WHERE key = ?;",
			sqlite_config_.kv_table
		);

		auto [kv_select_stmt, kv_select_error] = db_.prepare(kv_select_sql);
		if (!kv_select_stmt)
		{
			db_.rollback();
			return { false, kv_select_error };
		}

		kv_select_stmt->bind_text(1, lease.message_key);

		if (kv_select_stmt->step() == SQLITE_ROW)
		{
			std::string value_json = kv_select_stmt->column_text(0);
			try
			{
				json envelope = json::parse(value_json);
				envelope["dlqReason"] = reason;
				envelope["dlqAt"] = now;

				std::string kv_update_sql = std::format(
					"UPDATE {} SET value = ?, updated_at = ? WHERE key = ?;",
					sqlite_config_.kv_table
				);

				auto [kv_update_stmt, kv_update_error] = db_.prepare(kv_update_sql);
				if (kv_update_stmt)
				{
					kv_update_stmt->bind_text(1, envelope.dump());
					kv_update_stmt->bind_int64(2, now);
					kv_update_stmt->bind_text(3, lease.message_key);
					kv_update_stmt->step();
				}
			}
			catch (const json::exception&)
			{
				// Ignore JSON errors, message still moved to DLQ
			}
		}

		auto [commit_ok, commit_error] = db_.commit();
		if (!commit_ok)
		{
			db_.rollback();
			return { false, commit_error };
		}
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::extend_lease(const LeaseToken& lease, const int32_t& visibility_timeout_sec)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto now = current_time_ms();
	auto new_lease_until = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);

	std::string sql = std::format(
		"UPDATE {} SET lease_until = ? WHERE message_key = ? AND state = 'inflight' AND lease_until > ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		db_.rollback();
		return { false, error };
	}

	stmt->bind_int64(1, new_lease_until);
	stmt->bind_text(2, lease.message_key);
	stmt->bind_int64(3, now);

	if (stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to extend lease" };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::load_policy(const std::string& queue) -> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { std::nullopt, "database is not open" };
	}

	std::string policy_key = "policy:" + queue;
	std::string sql = std::format(
		"SELECT value FROM {} WHERE key = ?;",
		sqlite_config_.kv_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		return { std::nullopt, error };
	}

	stmt->bind_text(1, policy_key);

	if (stmt->step() != SQLITE_ROW)
	{
		return { std::nullopt, std::nullopt }; // Not found, not an error
	}

	std::string value_json = stmt->column_text(0);

	try
	{
		json policy_json = json::parse(value_json);

		QueuePolicy policy;
		policy.visibility_timeout_sec = policy_json.value("visibilityTimeoutSec", 30);
		policy.ttl_sec = policy_json.value("ttlSec", 0);

		if (policy_json.contains("retry"))
		{
			auto& retry = policy_json["retry"];
			policy.retry.limit = retry.value("limit", 5);
			policy.retry.backoff = retry.value("backoff", "exponential");
			policy.retry.initial_delay_sec = retry.value("initialDelaySec", 1);
			policy.retry.max_delay_sec = retry.value("maxDelaySec", 60);
		}

		if (policy_json.contains("dlq"))
		{
			auto& dlq = policy_json["dlq"];
			policy.dlq.enabled = dlq.value("enabled", true);
			policy.dlq.queue = dlq.value("queue", "");
			policy.dlq.retention_days = dlq.value("retentionDays", 14);
		}

		return { policy, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("failed to parse policy: {}", e.what()) };
	}
}

auto SQLiteAdapter::save_policy(const std::string& queue, const QueuePolicy& policy) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	json policy_json;
	policy_json["visibilityTimeoutSec"] = policy.visibility_timeout_sec;
	policy_json["ttlSec"] = policy.ttl_sec;

	policy_json["retry"]["limit"] = policy.retry.limit;
	policy_json["retry"]["backoff"] = policy.retry.backoff;
	policy_json["retry"]["initialDelaySec"] = policy.retry.initial_delay_sec;
	policy_json["retry"]["maxDelaySec"] = policy.retry.max_delay_sec;

	policy_json["dlq"]["enabled"] = policy.dlq.enabled;
	policy_json["dlq"]["queue"] = policy.dlq.queue;
	policy_json["dlq"]["retentionDays"] = policy.dlq.retention_days;

	auto now = current_time_ms();
	std::string policy_key = "policy:" + queue;

	// UPSERT (INSERT OR REPLACE)
	std::string sql = std::format(
		"INSERT OR REPLACE INTO {} (key, value, value_type, created_at, updated_at) VALUES (?, ?, ?, ?, ?);",
		sqlite_config_.kv_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		return { false, error };
	}

	stmt->bind_text(1, policy_key);
	stmt->bind_text(2, policy_json.dump());
	stmt->bind_text(3, "policy");
	stmt->bind_int64(4, now);
	stmt->bind_int64(5, now);

	if (stmt->step() != SQLITE_DONE)
	{
		return { false, "failed to save policy" };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::metrics(const std::string& queue) -> std::tuple<QueueMetrics, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	QueueMetrics metrics;

	if (!db_.is_open())
	{
		return { metrics, "database is not open" };
	}

	std::string sql = std::format(
		"SELECT state, COUNT(*) as count FROM {} WHERE queue = ? GROUP BY state;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		return { metrics, error };
	}

	stmt->bind_text(1, queue);

	while (stmt->step() == SQLITE_ROW)
	{
		std::string state = stmt->column_text(0);
		uint64_t count = static_cast<uint64_t>(stmt->column_int64(1));

		if (state == "ready")
		{
			metrics.ready = count;
		}
		else if (state == "inflight")
		{
			metrics.inflight = count;
		}
		else if (state == "delayed")
		{
			metrics.delayed = count;
		}
		else if (state == "dlq")
		{
			metrics.dlq = count;
		}
	}

	return { metrics, std::nullopt };
}

auto SQLiteAdapter::recover_expired_leases(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { 0, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { 0, tx_error };
	}

	auto now = current_time_ms();

	std::string sql = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL WHERE state = 'inflight' AND lease_until < ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		db_.rollback();
		return { 0, error };
	}

	stmt->bind_int64(1, now);

	if (stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { 0, "failed to recover expired leases" };
	}

	// Get count of affected rows
	std::string count_sql = "SELECT changes();";
	auto [count_stmt, count_error] = db_.prepare(count_sql);
	if (!count_stmt)
	{
		db_.rollback();
		return { 0, count_error };
	}

	int32_t count = 0;
	if (count_stmt->step() == SQLITE_ROW)
	{
		count = count_stmt->column_int(0);
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { 0, commit_error };
	}

	return { count, std::nullopt };
}

auto SQLiteAdapter::process_delayed_messages(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { 0, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { 0, tx_error };
	}

	auto now = current_time_ms();

	std::string sql = std::format(
		"UPDATE {} SET state = 'ready' WHERE state = 'delayed' AND available_at <= ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		db_.rollback();
		return { 0, error };
	}

	stmt->bind_int64(1, now);

	if (stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { 0, "failed to process delayed messages" };
	}

	// Get count of affected rows
	std::string count_sql = "SELECT changes();";
	auto [count_stmt, count_error] = db_.prepare(count_sql);
	if (!count_stmt)
	{
		db_.rollback();
		return { 0, count_error };
	}

	int32_t count = 0;
	if (count_stmt->step() == SQLITE_ROW)
	{
		count = count_stmt->column_int(0);
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { 0, commit_error };
	}

	return { count, std::nullopt };
}

auto SQLiteAdapter::get_expired_inflight_messages(void) -> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	std::vector<ExpiredLeaseInfo> expired_list;

	if (!db_.is_open())
	{
		return { expired_list, "database is not open" };
	}

	auto now = current_time_ms();

	std::string sql = std::format(
		"SELECT message_key, queue, attempt FROM {} WHERE state = 'inflight' AND lease_until < ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		return { expired_list, error };
	}

	stmt->bind_int64(1, now);

	while (stmt->step() == SQLITE_ROW)
	{
		ExpiredLeaseInfo info;
		info.message_key = stmt->column_text(0);
		info.queue = stmt->column_text(1);
		info.attempt = stmt->column_int(2);
		expired_list.push_back(info);
	}

	return { expired_list, std::nullopt };
}

auto SQLiteAdapter::delay_message(const std::string& message_key, int64_t delay_ms) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto now = current_time_ms();
	auto available_at = now + delay_ms;

	std::string sql = std::format(
		"UPDATE {} SET state = 'delayed', lease_until = NULL, available_at = ? WHERE message_key = ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		db_.rollback();
		return { false, error };
	}

	stmt->bind_int64(1, available_at);
	stmt->bind_text(2, message_key);

	if (stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to delay message" };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::move_to_dlq(const std::string& message_key, const std::string& reason) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto now = current_time_ms();

	// Update state to dlq with reason and timestamp in msg_index
	std::string idx_sql = std::format(
		"UPDATE {} SET state = 'dlq', lease_until = NULL, dlq_reason = ?, dlq_at = ? WHERE message_key = ?;",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_error] = db_.prepare(idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, idx_error };
	}

	idx_stmt->bind_text(1, reason);
	idx_stmt->bind_int64(2, now);
	idx_stmt->bind_text(3, message_key);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to move message to dlq" };
	}

	// Also update kv value with dlq reason for full message context
	std::string kv_select_sql = std::format(
		"SELECT value FROM {} WHERE key = ?;",
		sqlite_config_.kv_table
	);

	auto [kv_select_stmt, kv_select_error] = db_.prepare(kv_select_sql);
	if (kv_select_stmt)
	{
		kv_select_stmt->bind_text(1, message_key);

		if (kv_select_stmt->step() == SQLITE_ROW)
		{
			std::string value_json = kv_select_stmt->column_text(0);
			try
			{
				json envelope = json::parse(value_json);
				envelope["dlqReason"] = reason;
				envelope["dlqAt"] = now;

				std::string kv_update_sql = std::format(
					"UPDATE {} SET value = ?, updated_at = ? WHERE key = ?;",
					sqlite_config_.kv_table
				);

				auto [kv_update_stmt, kv_update_error] = db_.prepare(kv_update_sql);
				if (kv_update_stmt)
				{
					kv_update_stmt->bind_text(1, envelope.dump());
					kv_update_stmt->bind_int64(2, now);
					kv_update_stmt->bind_text(3, message_key);
					kv_update_stmt->step();
				}
			}
			catch (const json::exception&)
			{
				// Log but continue - index table already updated
			}
		}
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::apply_pragmas(void) -> std::tuple<bool, std::optional<std::string>>
{
	static const std::set<std::string> valid_journal_modes = {
		"DELETE", "WAL", "MEMORY", "TRUNCATE", "PERSIST", "OFF"
	};
	static const std::set<std::string> valid_synchronous_modes = {
		"OFF", "NORMAL", "FULL", "EXTRA"
	};

	auto [foreign_ok, foreign_message] = db_.execute("PRAGMA foreign_keys = ON;");
	if (!foreign_ok)
	{
		return { false, foreign_message };
	}

	if (!sqlite_config_.journal_mode.empty())
	{
		if (valid_journal_modes.find(sqlite_config_.journal_mode) == valid_journal_modes.end())
		{
			return { false, std::format("invalid journal_mode: {}", sqlite_config_.journal_mode) };
		}

		std::string pragma = "PRAGMA journal_mode=" + sqlite_config_.journal_mode + ";";
		auto [journal_ok, journal_message] = db_.execute(pragma);
		if (!journal_ok)
		{
			return { false, journal_message };
		}
	}

	if (!sqlite_config_.synchronous.empty())
	{
		if (valid_synchronous_modes.find(sqlite_config_.synchronous) == valid_synchronous_modes.end())
		{
			return { false, std::format("invalid synchronous mode: {}", sqlite_config_.synchronous) };
		}

		std::string pragma = "PRAGMA synchronous=" + sqlite_config_.synchronous + ";";
		auto [sync_ok, sync_message] = db_.execute(pragma);
		if (!sync_ok)
		{
			return { false, sync_message };
		}
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::ensure_schema(void) -> std::tuple<bool, std::optional<std::string>>
{
	if (schema_path_.empty())
	{
		return { false, "schema path is empty" };
	}

	auto [sql_text, sql_message] = load_schema_sql();
	if (!sql_text.has_value())
	{
		return { false, sql_message };
	}

	return db_.execute(sql_text.value());
}

auto SQLiteAdapter::load_schema_sql(void) -> std::tuple<std::optional<std::string>, std::optional<std::string>>
{
	Utilities::File source;
	auto [opened, open_message] = source.open(schema_path_, std::ios::in | std::ios::binary);
	if (!opened)
	{
		return { std::nullopt, open_message };
	}

	auto [data, read_message] = source.read_bytes();
	source.close();
	if (data == std::nullopt)
	{
		return { std::nullopt, read_message };
	}

	std::string sql = Utilities::Converter::to_string(data.value());
	if (sql.empty())
	{
		return { std::nullopt, "schema sql is empty" };
	}

	replace_all(sql, "{{kv_table}}", sqlite_config_.kv_table);
	replace_all(sql, "{{msg_index_table}}", sqlite_config_.message_index_table);

	return { sql, std::nullopt };
}

auto SQLiteAdapter::list_dlq_messages(const std::string& queue, int32_t limit)
	-> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	std::vector<DlqMessageInfo> dlq_list;

	if (!db_.is_open())
	{
		return { dlq_list, "database is not open" };
	}

	std::string sql = std::format(
		"SELECT message_key, queue, dlq_reason, dlq_at, attempt FROM {} "
		"WHERE queue = ? AND state = 'dlq' ORDER BY dlq_at DESC LIMIT ?;",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		return { dlq_list, error };
	}

	stmt->bind_text(1, queue);
	stmt->bind_int(2, limit);

	while (stmt->step() == SQLITE_ROW)
	{
		DlqMessageInfo info;
		info.message_key = stmt->column_text(0);
		info.queue = stmt->column_text(1);
		info.reason = stmt->column_text(2);
		info.dlq_at_ms = stmt->column_int64(3);
		info.attempt = stmt->column_int(4);
		dlq_list.push_back(info);
	}

	return { dlq_list, std::nullopt };
}

auto SQLiteAdapter::reprocess_dlq_message(const std::string& message_key)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { false, "database is not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto now = current_time_ms();

	// Reset state to ready, clear DLQ fields, reset attempt
	std::string sql = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = ?, "
		"attempt = 0, dlq_reason = NULL, dlq_at = NULL WHERE message_key = ? AND state = 'dlq';",
		sqlite_config_.message_index_table
	);

	auto [stmt, error] = db_.prepare(sql);
	if (!stmt)
	{
		db_.rollback();
		return { false, error };
	}

	stmt->bind_int64(1, now);
	stmt->bind_text(2, message_key);

	if (stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "failed to reprocess DLQ message" };
	}

	// Also update kv to remove dlq fields
	std::string kv_select_sql = std::format(
		"SELECT value FROM {} WHERE key = ?;",
		sqlite_config_.kv_table
	);

	auto [kv_select_stmt, kv_select_error] = db_.prepare(kv_select_sql);
	if (kv_select_stmt)
	{
		kv_select_stmt->bind_text(1, message_key);

		if (kv_select_stmt->step() == SQLITE_ROW)
		{
			std::string value_json = kv_select_stmt->column_text(0);
			try
			{
				json envelope = json::parse(value_json);
				envelope.erase("dlqReason");
				envelope.erase("dlqAt");
				envelope["attempt"] = 0;

				std::string kv_update_sql = std::format(
					"UPDATE {} SET value = ?, updated_at = ? WHERE key = ?;",
					sqlite_config_.kv_table
				);

				auto [kv_update_stmt, kv_update_error] = db_.prepare(kv_update_sql);
				if (kv_update_stmt)
				{
					kv_update_stmt->bind_text(1, envelope.dump());
					kv_update_stmt->bind_int64(2, now);
					kv_update_stmt->bind_text(3, message_key);
					kv_update_stmt->step();
				}
			}
			catch (const json::exception&)
			{
				// Continue anyway
			}
		}
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto SQLiteAdapter::purge_expired_messages(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!db_.is_open())
	{
		return { 0, "database is not open" };
	}

	auto now = current_time_ms();

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { 0, tx_error };
	}

	// Get expired message keys for kv table cleanup
	std::string select_sql = std::format(
		"SELECT message_key FROM {} WHERE expired_at > 0 AND expired_at <= ? AND state IN ('ready', 'delayed');",
		sqlite_config_.message_index_table
	);

	auto [select_stmt, select_error] = db_.prepare(select_sql);
	if (!select_stmt)
	{
		db_.rollback();
		return { 0, select_error };
	}

	select_stmt->bind_int64(1, now);

	std::vector<std::string> expired_keys;
	while (select_stmt->step() == SQLITE_ROW)
	{
		expired_keys.push_back(select_stmt->column_text(0));
	}

	if (expired_keys.empty())
	{
		db_.rollback();
		return { 0, std::nullopt };
	}

	// Delete from msg_index
	std::string delete_idx_sql = std::format(
		"DELETE FROM {} WHERE expired_at > 0 AND expired_at <= ? AND state IN ('ready', 'delayed');",
		sqlite_config_.message_index_table
	);

	auto [del_idx_stmt, del_idx_error] = db_.prepare(delete_idx_sql);
	if (!del_idx_stmt)
	{
		db_.rollback();
		return { 0, del_idx_error };
	}

	del_idx_stmt->bind_int64(1, now);

	if (del_idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { 0, "failed to delete expired messages from index" };
	}

	// Delete from kv table
	for (const auto& key : expired_keys)
	{
		std::string delete_kv_sql = std::format(
			"DELETE FROM {} WHERE key = ?;",
			sqlite_config_.kv_table
		);

		auto [kv_stmt, kv_error] = db_.prepare(delete_kv_sql);
		if (kv_stmt)
		{
			kv_stmt->bind_text(1, key);
			kv_stmt->step();
		}
	}

	auto [commit_ok2, commit_error2] = db_.commit();
	if (!commit_ok2)
	{
		db_.rollback();
		return { 0, commit_error2 };
	}

	return { static_cast<int32_t>(expired_keys.size()), std::nullopt };
}
