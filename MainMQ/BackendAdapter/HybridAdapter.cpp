#include "HybridAdapter.h"

#include "File.h"
#include "Generator.h"
#include "Logger.h"

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>

using json = nlohmann::json;

namespace
{
	auto current_time_ms_helper() -> int64_t
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
	}
}

HybridAdapter::HybridAdapter(const std::string& schema_path)
	: is_open_(false)
	, schema_path_(schema_path)
	, payload_root_("./data/payloads")
{
}

HybridAdapter::~HybridAdapter(void)
{
	close();
}

auto HybridAdapter::open(const BackendConfig& config) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (is_open_)
	{
		return { false, "already open" };
	}

	sqlite_config_ = config.sqlite;

	std::filesystem::path db_path(sqlite_config_.db_path);
	auto parent_path = db_path.parent_path();
	if (!parent_path.empty())
	{
		std::error_code ec;
		std::filesystem::create_directories(parent_path, ec);
	}

	std::error_code ec;
	std::filesystem::create_directories(payload_root_, ec);

	auto [opened, open_error] = db_.open(sqlite_config_.db_path);
	if (!opened)
	{
		return { false, open_error };
	}

	auto [pragmas_ok, pragmas_error] = apply_pragmas();
	if (!pragmas_ok)
	{
		db_.close();
		return { false, pragmas_error };
	}

	auto [schema_ok, schema_error] = ensure_schema();
	if (!schema_ok)
	{
		db_.close();
		return { false, schema_error };
	}

	is_open_ = true;

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("HybridAdapter opened (db: {}, payloads: {})", sqlite_config_.db_path, payload_root_)
	);

	return { true, std::nullopt };
}

auto HybridAdapter::close(void) -> void
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return;
	}

	db_.close();
	is_open_ = false;
}

auto HybridAdapter::apply_pragmas(void) -> std::tuple<bool, std::optional<std::string>>
{
	auto [fk_ok, fk_error] = db_.execute("PRAGMA foreign_keys = ON;");
	if (!fk_ok)
	{
		return { false, std::format("failed to set foreign_keys: {}", fk_error.value_or("unknown")) };
	}

	std::string journal_pragma = std::format("PRAGMA journal_mode = {};", sqlite_config_.journal_mode);
	auto [j_ok, j_error] = db_.execute(journal_pragma);
	if (!j_ok)
	{
		return { false, std::format("failed to set journal_mode: {}", j_error.value_or("unknown")) };
	}

	std::string sync_pragma = std::format("PRAGMA synchronous = {};", sqlite_config_.synchronous);
	auto [s_ok, s_error] = db_.execute(sync_pragma);
	if (!s_ok)
	{
		return { false, std::format("failed to set synchronous: {}", s_error.value_or("unknown")) };
	}

	std::string timeout_pragma = std::format("PRAGMA busy_timeout = {};", sqlite_config_.busy_timeout_ms);
	auto [t_ok, t_error] = db_.execute(timeout_pragma);
	if (!t_ok)
	{
		return { false, std::format("failed to set busy_timeout: {}", t_error.value_or("unknown")) };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::ensure_schema(void) -> std::tuple<bool, std::optional<std::string>>
{
	auto [schema_sql, load_error] = load_schema_sql();
	if (!schema_sql.has_value())
	{
		return { false, load_error };
	}

	std::string sql = schema_sql.value();
	size_t pos = 0;
	while ((pos = sql.find("{{kv_table}}", pos)) != std::string::npos)
	{
		sql.replace(pos, 12, sqlite_config_.kv_table);
	}
	pos = 0;
	while ((pos = sql.find("{{msg_index_table}}", pos)) != std::string::npos)
	{
		sql.replace(pos, 19, sqlite_config_.message_index_table);
	}

	auto [exec_ok, exec_error] = db_.execute(sql);
	if (!exec_ok)
	{
		return { false, std::format("schema execution failed: {}", exec_error.value_or("unknown")) };
	}

	// Databases created before ownership fencing lack lease_consumer_id; rows left inflight across
	// the upgrade get an empty owner, so their ack is rejected once and the lease sweep then
	// redelivers them. (Defect D-55)
	auto [migrated, migrate_error] = db_.ensure_column(
		sqlite_config_.message_index_table, "lease_consumer_id", "TEXT NOT NULL DEFAULT ''"
	);
	if (!migrated)
	{
		return { false, std::format("lease ownership migration failed: {}", migrate_error.value_or("unknown")) };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::load_schema_sql(void) -> std::tuple<std::optional<std::string>, std::optional<std::string>>
{
	std::ifstream file(schema_path_);
	if (!file.is_open())
	{
		if (!std::filesystem::exists(schema_path_))
		{
			return { std::nullopt, std::format("cannot open schema file: there is no file : {}", schema_path_) };
		}
		return { std::nullopt, std::format("cannot open schema file: {}", schema_path_) };
	}

	std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
	file.close();

	return { content, std::nullopt };
}

auto HybridAdapter::ensure_payload_directories(const std::string& queue) -> std::tuple<bool, std::optional<std::string>>
{
	std::vector<std::string> dirs = {
		std::format("{}/{}/active", payload_root_, queue),
		std::format("{}/{}/archive", payload_root_, queue),
		std::format("{}/{}/dlq", payload_root_, queue)
	};

	for (const auto& dir : dirs)
	{
		std::error_code ec;
		if (!std::filesystem::exists(dir, ec))
		{
			if (!std::filesystem::create_directories(dir, ec))
			{
				return { false, std::format("failed to create directory {}: {}", dir, ec.message()) };
			}
		}
	}

	return { true, std::nullopt };
}

auto HybridAdapter::build_payload_path(const std::string& queue, const std::string& message_id) -> std::string
{
	return std::format("{}/{}/active/{}.json", payload_root_, queue, message_id);
}

auto HybridAdapter::build_archive_path(const std::string& queue, const std::string& message_id) -> std::string
{
	return std::format("{}/{}/archive/{}.json", payload_root_, queue, message_id);
}

auto HybridAdapter::build_dlq_path(const std::string& queue, const std::string& message_id) -> std::string
{
	return std::format("{}/{}/dlq/{}.json", payload_root_, queue, message_id);
}

auto HybridAdapter::enqueue(const MessageEnvelope& message) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto [dirs_ok, dirs_error] = ensure_payload_directories(message.queue);
	if (!dirs_ok)
	{
		return { false, dirs_error };
	}

	auto now = current_time_ms();
	std::string state = (message.available_at_ms > now) ? "delayed" : "ready";

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto payload_path = build_payload_path(message.queue, message.message_id);

	json envelope;
	envelope["messageId"] = message.message_id;
	envelope["queue"] = message.queue;
	envelope["payloadPath"] = payload_path;
	envelope["priority"] = message.priority;
	envelope["attempt"] = message.attempt;
	envelope["createdAt"] = message.created_at_ms;

	std::string insert_kv_sql = std::format(
		"INSERT INTO {} (key, value, value_type, created_at, updated_at) VALUES (?, ?, 'message', ?, ?)",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_prep_error] = db_.prepare(insert_kv_sql);
	if (!kv_stmt)
	{
		db_.rollback();
		return { false, std::format("kv insert prepare failed: {}", kv_prep_error.value_or("unknown")) };
	}
	kv_stmt->bind_text(1, message.key);
	kv_stmt->bind_text(2, envelope.dump());
	kv_stmt->bind_int64(3, now);
	kv_stmt->bind_int64(4, now);

	if (kv_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "kv insert failed" };
	}

	std::string insert_idx_sql = std::format(
		"INSERT INTO {} (queue, state, priority, available_at, lease_until, attempt, target_consumer_id, message_key, expired_at) "
		"VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?)",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_prep_error] = db_.prepare(insert_idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, std::format("index insert prepare failed: {}", idx_prep_error.value_or("unknown")) };
	}
	idx_stmt->bind_text(1, message.queue);
	idx_stmt->bind_text(2, state);
	idx_stmt->bind_int(3, message.priority);
	idx_stmt->bind_int64(4, message.available_at_ms);
	idx_stmt->bind_int(5, message.attempt);
	idx_stmt->bind_text(6, message.target_consumer_id);
	idx_stmt->bind_text(7, message.key);
	idx_stmt->bind_int64(8, message.expired_at_ms);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "index insert failed" };
	}

	// Write payload file AFTER successful SQLite inserts
	json payload_json;
	payload_json["payload"] = message.payload_json;
	payload_json["attributes"] = message.attributes_json;

	auto [write_ok, write_error] = atomic_write(payload_path, payload_json.dump(2));
	if (!write_ok)
	{
		db_.rollback();
		return { false, write_error };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		std::filesystem::remove(payload_path);
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::lease_next(const std::string& queue, const std::string& consumer_id, const int32_t& visibility_timeout_sec)
	-> LeaseResult
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	LeaseResult result;
	result.leased = false;

	if (!is_open_)
	{
		result.error = "adapter not open";
		return result;
	}

	auto now = current_time_ms();
	auto lease_until = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);
	auto lease_id = Utilities::Generator::guid();

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		result.error = tx_error;
		return result;
	}

	std::string select_sql = std::format(
		"SELECT message_key, attempt FROM {} "
		"WHERE queue = ? AND state = 'ready' AND available_at <= ? "
		"AND (target_consumer_id = '' OR target_consumer_id = ?) "
		"AND (expired_at = 0 OR expired_at > ?) "
		"ORDER BY priority DESC, available_at ASC LIMIT 1",
		sqlite_config_.message_index_table
	);

	auto [select_stmt, select_prep_error] = db_.prepare(select_sql);
	if (!select_stmt)
	{
		db_.rollback();
		result.error = select_prep_error;
		return result;
	}
	select_stmt->bind_text(1, queue);
	select_stmt->bind_int64(2, now);
	select_stmt->bind_text(3, consumer_id);
	select_stmt->bind_int64(4, now);

	if (select_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		return result;
	}

	std::string message_key = select_stmt->column_text(0);
	int32_t attempt = select_stmt->column_int(1);

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'inflight', lease_until = ?, attempt = ?, lease_id = ?, lease_consumer_id = ? WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [update_stmt, update_prep_error] = db_.prepare(update_sql);
	if (!update_stmt)
	{
		db_.rollback();
		result.error = update_prep_error;
		return result;
	}
	update_stmt->bind_int64(1, lease_until);
	update_stmt->bind_int(2, attempt + 1);
	update_stmt->bind_text(3, lease_id);
	update_stmt->bind_text(4, consumer_id);
	update_stmt->bind_text(5, message_key);

	if (update_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		result.error = "update inflight failed";
		return result;
	}

	std::string kv_sql = std::format(
		"SELECT value FROM {} WHERE key = ?",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_prep_error] = db_.prepare(kv_sql);
	if (!kv_stmt)
	{
		db_.rollback();
		result.error = kv_prep_error;
		return result;
	}
	kv_stmt->bind_text(1, message_key);

	if (kv_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		result.error = "envelope not found";
		return result;
	}

	std::string envelope_json = kv_stmt->column_text(0);

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		result.error = commit_error;
		return result;
	}

	try
	{
		json envelope = json::parse(envelope_json);

		MessageEnvelope msg;
		msg.key = message_key;
		msg.message_id = envelope.value("messageId", "");
		msg.queue = envelope.value("queue", "");
		msg.priority = envelope.value("priority", 0);
		msg.attempt = attempt + 1;
		msg.created_at_ms = envelope.value("createdAt", static_cast<int64_t>(0));

		auto [payload_content, payload_error] = read_payload(msg.queue, msg.message_id);
		if (payload_content.has_value())
		{
			json payload_json = json::parse(payload_content.value());
			msg.payload_json = payload_json.value("payload", "{}");
			msg.attributes_json = payload_json.value("attributes", "{}");
		}

		LeaseToken lease;
		lease.lease_id = lease_id;
		lease.message_key = message_key;
		lease.consumer_id = consumer_id;
		lease.lease_until_ms = lease_until;

		result.leased = true;
		result.message = msg;
		result.lease = lease;
	}
	catch (const json::exception& e)
	{
		result.error = std::format("envelope parse error: {}", e.what());
	}

	return result;
}

auto HybridAdapter::ack(const LeaseToken& lease) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	std::string check_sql = std::format(
		"SELECT queue FROM {} WHERE message_key = ? AND state = 'inflight' AND (? = '' OR lease_id = ?) AND lease_consumer_id = ?",
		sqlite_config_.message_index_table
	);

	auto [check_stmt, check_prep_error] = db_.prepare(check_sql);
	if (!check_stmt)
	{
		db_.rollback();
		return { false, check_prep_error };
	}
	check_stmt->bind_text(1, lease.message_key);
	check_stmt->bind_text(2, lease.lease_id);
	check_stmt->bind_text(3, lease.lease_id);
	check_stmt->bind_text(4, lease.consumer_id);

	if (check_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		return { false, "ack rejected: message is not inflight, or the lease is held by another consumer" };
	}

	std::string queue = check_stmt->column_text(0);

	std::string delete_idx_sql = std::format(
		"DELETE FROM {} WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [del_idx_stmt, del_idx_prep_error] = db_.prepare(delete_idx_sql);
	if (!del_idx_stmt)
	{
		db_.rollback();
		return { false, del_idx_prep_error };
	}
	del_idx_stmt->bind_text(1, lease.message_key);

	if (del_idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "delete index failed" };
	}

	std::string delete_kv_sql = std::format(
		"DELETE FROM {} WHERE key = ?",
		sqlite_config_.kv_table
	);

	auto [del_kv_stmt, del_kv_prep_error] = db_.prepare(delete_kv_sql);
	if (!del_kv_stmt)
	{
		db_.rollback();
		return { false, del_kv_prep_error };
	}
	del_kv_stmt->bind_text(1, lease.message_key);

	if (del_kv_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "delete kv failed" };
	}

	auto message_id = extract_message_id_from_key(lease.message_key);
	auto [archive_ok, archive_error] = move_payload_to_archive(queue, message_id);
	if (!archive_ok)
	{
		db_.rollback();
		return { false, archive_error.value_or("failed to archive payload") };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::nack(const LeaseToken& lease, const std::string& reason, const bool& requeue, int32_t retry_limit)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto now = current_time_ms();

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	std::string check_sql = std::format(
		"SELECT queue, attempt FROM {} WHERE message_key = ? AND state = 'inflight' AND (? = '' OR lease_id = ?) AND lease_consumer_id = ?",
		sqlite_config_.message_index_table
	);

	auto [check_stmt, check_prep_error] = db_.prepare(check_sql);
	if (!check_stmt)
	{
		db_.rollback();
		return { false, check_prep_error };
	}
	check_stmt->bind_text(1, lease.message_key);
	check_stmt->bind_text(2, lease.lease_id);
	check_stmt->bind_text(3, lease.lease_id);
	check_stmt->bind_text(4, lease.consumer_id);

	if (check_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		return { false, "nack rejected: message is not inflight, or the lease is held by another consumer" };
	}

	std::string queue = check_stmt->column_text(0);
	int32_t attempt = check_stmt->column_int(1);

	// Cap explicit requeue: a message that has reached the retry limit goes to DLQ instead of
	// ready, preventing an infinite nack-requeue loop on a poison message. (Defect D-06)
	bool effective_requeue = requeue;
	if (requeue && retry_limit >= 0 && attempt >= retry_limit)
	{
		effective_requeue = false;
	}

	if (effective_requeue)
	{
		std::string update_sql = std::format(
			"UPDATE {} SET state = 'ready', lease_until = NULL, lease_id = '', lease_consumer_id = '', available_at = ? WHERE message_key = ?",
			sqlite_config_.message_index_table
		);

		auto [update_stmt, update_prep_error] = db_.prepare(update_sql);
		if (!update_stmt)
		{
			db_.rollback();
			return { false, update_prep_error };
		}
		update_stmt->bind_int64(1, now);
		update_stmt->bind_text(2, lease.message_key);

		if (update_stmt->step() != SQLITE_DONE)
		{
			db_.rollback();
			return { false, "update to ready failed" };
		}
	}
	else
	{
		std::string update_sql = std::format(
			"UPDATE {} SET state = 'dlq', lease_until = NULL, lease_consumer_id = '', dlq_reason = ?, dlq_at = ? WHERE message_key = ?",
			sqlite_config_.message_index_table
		);

		auto [update_stmt, update_prep_error] = db_.prepare(update_sql);
		if (!update_stmt)
		{
			db_.rollback();
			return { false, update_prep_error };
		}
		update_stmt->bind_text(1, reason);
		update_stmt->bind_int64(2, current_time_ms());
		update_stmt->bind_text(3, lease.message_key);

		if (update_stmt->step() != SQLITE_DONE)
		{
			db_.rollback();
			return { false, "update to dlq failed" };
		}

		std::string update_kv_sql = std::format(
			"UPDATE {} SET value = json_set(value, '$.dlqReason', ?, '$.dlqAt', ?), updated_at = ? WHERE key = ?",
			sqlite_config_.kv_table
		);

		auto [kv_stmt, kv_prep_error] = db_.prepare(update_kv_sql);
		if (kv_stmt)
		{
			kv_stmt->bind_text(1, reason);
			kv_stmt->bind_int64(2, now);
			kv_stmt->bind_int64(3, now);
			kv_stmt->bind_text(4, lease.message_key);
			kv_stmt->step();
		}

		auto message_id = extract_message_id_from_key(lease.message_key);
		auto [dlq_ok, dlq_error] = move_payload_to_dlq(queue, message_id);
		if (!dlq_ok)
		{
			db_.rollback();
			return { false, dlq_error.value_or("failed to move payload to dlq") };
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

auto HybridAdapter::extend_lease(const LeaseToken& lease, const int32_t& visibility_timeout_sec)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto now = current_time_ms();
	auto new_lease_until = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);

	std::string update_sql = std::format(
		"UPDATE {} SET lease_until = ? WHERE message_key = ? AND state = 'inflight' AND lease_until > ? AND (? = '' OR lease_id = ?) AND lease_consumer_id = ?",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(update_sql);
	if (!stmt)
	{
		return { false, prep_error };
	}
	stmt->bind_int64(1, new_lease_until);
	stmt->bind_text(2, lease.message_key);
	stmt->bind_int64(3, now);
	stmt->bind_text(4, lease.lease_id);
	stmt->bind_text(5, lease.lease_id);
	stmt->bind_text(6, lease.consumer_id);

	if (stmt->step() != SQLITE_DONE)
	{
		return { false, "extend lease failed" };
	}

	auto [changes_stmt, changes_error] = db_.prepare("SELECT changes();");
	int32_t changed = 0;
	if (changes_stmt && changes_stmt->step() == SQLITE_ROW)
	{
		changed = changes_stmt->column_int(0);
	}
	if (changed == 0)
	{
		return { false, "extend_lease rejected: lease not held, already expired, or held by another consumer" };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::load_policy(const std::string& queue)
	-> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { std::nullopt, "adapter not open" };
	}

	std::string policy_key = std::format("policy:{}", queue);
	std::string select_sql = std::format(
		"SELECT value FROM {} WHERE key = ?",
		sqlite_config_.kv_table
	);

	auto [stmt, prep_error] = db_.prepare(select_sql);
	if (!stmt)
	{
		return { std::nullopt, prep_error };
	}
	stmt->bind_text(1, policy_key);

	if (stmt->step() != SQLITE_ROW)
	{
		return { std::nullopt, std::nullopt };
	}

	std::string value_json = stmt->column_text(0);

	try
	{
		json j = json::parse(value_json);

		QueuePolicy policy;
		policy.visibility_timeout_sec = j.value("visibilityTimeoutSec", 30);
		policy.ttl_sec = j.value("ttlSec", 0);

		if (j.contains("retry") && j["retry"].is_object())
		{
			auto& r = j["retry"];
			policy.retry.limit = r.value("limit", 5);
			policy.retry.backoff = r.value("backoff", "exponential");
			policy.retry.initial_delay_sec = r.value("initialDelaySec", 1);
			policy.retry.max_delay_sec = r.value("maxDelaySec", 60);
		}

		if (j.contains("dlq") && j["dlq"].is_object())
		{
			auto& d = j["dlq"];
			policy.dlq.enabled = d.value("enabled", true);
			policy.dlq.queue = d.value("queue", "");
			policy.dlq.retention_days = d.value("retentionDays", 14);
		}

		return { policy, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("policy parse error: {}", e.what()) };
	}
}

auto HybridAdapter::save_policy(const std::string& queue, const QueuePolicy& policy)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	ensure_payload_directories(queue);

	std::string policy_key = std::format("policy:{}", queue);
	auto now = current_time_ms();

	json j;
	j["visibilityTimeoutSec"] = policy.visibility_timeout_sec;
	j["ttlSec"] = policy.ttl_sec;
	j["retry"] = {
		{ "limit", policy.retry.limit },
		{ "backoff", policy.retry.backoff },
		{ "initialDelaySec", policy.retry.initial_delay_sec },
		{ "maxDelaySec", policy.retry.max_delay_sec }
	};
	j["dlq"] = {
		{ "enabled", policy.dlq.enabled },
		{ "queue", policy.dlq.queue },
		{ "retentionDays", policy.dlq.retention_days }
	};

	std::string upsert_sql = std::format(
		"INSERT INTO {} (key, value, value_type, created_at, updated_at) VALUES (?, ?, 'policy', ?, ?) "
		"ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
		sqlite_config_.kv_table
	);

	auto [stmt, prep_error] = db_.prepare(upsert_sql);
	if (!stmt)
	{
		return { false, prep_error };
	}
	stmt->bind_text(1, policy_key);
	stmt->bind_text(2, j.dump());
	stmt->bind_int64(3, now);
	stmt->bind_int64(4, now);

	if (stmt->step() != SQLITE_DONE)
	{
		return { false, "save policy failed" };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::metrics(const std::string& queue) -> std::tuple<QueueMetrics, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	QueueMetrics m;

	if (!is_open_)
	{
		return { m, "adapter not open" };
	}

	std::string sql = std::format(
		"SELECT state, COUNT(*) as cnt FROM {} WHERE queue = ? GROUP BY state",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(sql);
	if (!stmt)
	{
		return { m, prep_error };
	}
	stmt->bind_text(1, queue);

	while (stmt->step() == SQLITE_ROW)
	{
		std::string state = stmt->column_text(0);
		uint64_t count = static_cast<uint64_t>(stmt->column_int64(1));

		if (state == "ready")
		{
			m.ready = count;
		}
		else if (state == "inflight")
		{
			m.inflight = count;
		}
		else if (state == "delayed")
		{
			m.delayed = count;
		}
		else if (state == "dlq")
		{
			m.dlq = count;
		}
	}

	return { m, std::nullopt };
}

auto HybridAdapter::recover_expired_leases(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto now = current_time_ms();

	// First count how many will be affected
	std::string count_sql = std::format(
		"SELECT COUNT(*) FROM {} WHERE state = 'inflight' AND lease_until < ?",
		sqlite_config_.message_index_table
	);

	auto [count_stmt, count_prep_error] = db_.prepare(count_sql);
	if (!count_stmt)
	{
		return { 0, count_prep_error };
	}
	count_stmt->bind_int64(1, now);

	int32_t count = 0;
	if (count_stmt->step() == SQLITE_ROW)
	{
		count = count_stmt->column_int(0);
	}

	if (count == 0)
	{
		return { 0, std::nullopt };
	}

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = ? "
		"WHERE state = 'inflight' AND lease_until < ?",
		sqlite_config_.message_index_table
	);

	auto [update_stmt, update_prep_error] = db_.prepare(update_sql);
	if (!update_stmt)
	{
		return { 0, update_prep_error };
	}
	update_stmt->bind_int64(1, now);
	update_stmt->bind_int64(2, now);

	if (update_stmt->step() != SQLITE_DONE)
	{
		return { 0, "recover expired leases failed" };
	}

	return { count, std::nullopt };
}

auto HybridAdapter::process_delayed_messages(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto now = current_time_ms();

	// First count how many will be affected
	std::string count_sql = std::format(
		"SELECT COUNT(*) FROM {} WHERE state = 'delayed' AND available_at <= ?",
		sqlite_config_.message_index_table
	);

	auto [count_stmt, count_prep_error] = db_.prepare(count_sql);
	if (!count_stmt)
	{
		return { 0, count_prep_error };
	}
	count_stmt->bind_int64(1, now);

	int32_t count = 0;
	if (count_stmt->step() == SQLITE_ROW)
	{
		count = count_stmt->column_int(0);
	}

	if (count == 0)
	{
		return { 0, std::nullopt };
	}

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'ready' WHERE state = 'delayed' AND available_at <= ?",
		sqlite_config_.message_index_table
	);

	auto [update_stmt, update_prep_error] = db_.prepare(update_sql);
	if (!update_stmt)
	{
		return { 0, update_prep_error };
	}
	update_stmt->bind_int64(1, now);

	if (update_stmt->step() != SQLITE_DONE)
	{
		return { 0, "process delayed messages failed" };
	}

	return { count, std::nullopt };
}

auto HybridAdapter::get_expired_inflight_messages(void)
	-> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	std::vector<ExpiredLeaseInfo> expired;

	if (!is_open_)
	{
		return { expired, "adapter not open" };
	}

	auto now = current_time_ms();

	std::string sql = std::format(
		"SELECT message_key, queue, attempt FROM {} WHERE state = 'inflight' AND lease_until < ?",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(sql);
	if (!stmt)
	{
		return { expired, prep_error };
	}
	stmt->bind_int64(1, now);

	while (stmt->step() == SQLITE_ROW)
	{
		ExpiredLeaseInfo info;
		info.message_key = stmt->column_text(0);
		info.queue = stmt->column_text(1);
		info.attempt = stmt->column_int(2);
		expired.push_back(info);
	}

	return { expired, std::nullopt };
}

auto HybridAdapter::delay_message(const std::string& message_key, int64_t delay_ms)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto now = current_time_ms();
	auto available_at = now + delay_ms;

	std::string new_state = (delay_ms > 0) ? "delayed" : "ready";

	std::string update_sql = std::format(
		"UPDATE {} SET state = ?, lease_until = NULL, available_at = ? WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(update_sql);
	if (!stmt)
	{
		return { false, prep_error };
	}
	stmt->bind_text(1, new_state);
	stmt->bind_int64(2, available_at);
	stmt->bind_text(3, message_key);

	if (stmt->step() != SQLITE_DONE)
	{
		return { false, "delay message failed" };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::move_to_dlq(const std::string& message_key, const std::string& reason)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto now = current_time_ms();

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	std::string check_sql = std::format(
		"SELECT queue FROM {} WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [check_stmt, check_prep_error] = db_.prepare(check_sql);
	if (!check_stmt)
	{
		db_.rollback();
		return { false, check_prep_error };
	}
	check_stmt->bind_text(1, message_key);

	std::string queue;
	if (check_stmt->step() == SQLITE_ROW)
	{
		queue = check_stmt->column_text(0);
	}

	std::string update_idx_sql = std::format(
		"UPDATE {} SET state = 'dlq', lease_until = NULL, dlq_reason = ?, dlq_at = ? WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_prep_error] = db_.prepare(update_idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, idx_prep_error };
	}
	idx_stmt->bind_text(1, reason);
	idx_stmt->bind_int64(2, current_time_ms());
	idx_stmt->bind_text(3, message_key);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "update to dlq failed" };
	}

	std::string update_kv_sql = std::format(
		"UPDATE {} SET value = json_set(value, '$.dlqReason', ?, '$.dlqAt', ?), updated_at = ? WHERE key = ?",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_prep_error] = db_.prepare(update_kv_sql);
	if (kv_stmt)
	{
		kv_stmt->bind_text(1, reason);
		kv_stmt->bind_int64(2, now);
		kv_stmt->bind_int64(3, now);
		kv_stmt->bind_text(4, message_key);
		kv_stmt->step();
	}

	if (!queue.empty())
	{
		auto message_id = extract_message_id_from_key(message_key);
		auto [dlq_ok, dlq_error] = move_payload_to_dlq(queue, message_id);
		if (!dlq_ok)
		{
			db_.rollback();
			return { false, dlq_error.value_or("failed to move payload to dlq") };
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

auto HybridAdapter::write_payload(const std::string& queue, const std::string& message_id, const std::string& payload)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto path = build_payload_path(queue, message_id);
	return atomic_write(path, payload);
}

auto HybridAdapter::read_payload(const std::string& queue, const std::string& message_id)
	-> std::tuple<std::optional<std::string>, std::optional<std::string>>
{
	auto path = build_payload_path(queue, message_id);

	std::ifstream file(path);
	if (!file.is_open())
	{
		return { std::nullopt, std::format("cannot open payload file: {}", path) };
	}

	std::string content((std::istreambuf_iterator<char>(file)),
		std::istreambuf_iterator<char>());
	file.close();

	return { content, std::nullopt };
}

auto HybridAdapter::move_payload_to_archive(const std::string& queue, const std::string& message_id)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto src = build_payload_path(queue, message_id);
	auto dest = build_archive_path(queue, message_id);

	std::error_code ec;
	std::filesystem::rename(src, dest, ec);
	if (ec)
	{
		return { false, ec.message() };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::move_payload_to_dlq(const std::string& queue, const std::string& message_id)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto src = build_payload_path(queue, message_id);
	auto dest = build_dlq_path(queue, message_id);

	std::error_code ec;
	std::filesystem::rename(src, dest, ec);
	if (ec)
	{
		return { false, ec.message() };
	}

	return { true, std::nullopt };
}

auto HybridAdapter::atomic_write(const std::string& target_path, const std::string& content)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto temp_path = target_path + ".tmp";

	// Durable, fail-closed write: abort before rename if the payload temp cannot be fully
	// written (e.g. ENOSPC), so a partial payload never replaces a valid one. (Defect D-02)
	auto [write_ok, write_error] = Utilities::write_file_durable(temp_path, content);
	if (!write_ok)
	{
		std::error_code ec;
		std::filesystem::remove(temp_path, ec);
		return { false, write_error };
	}

	std::error_code ec;
	std::filesystem::rename(temp_path, target_path, ec);
	if (ec)
	{
		std::filesystem::remove(temp_path, ec);
		return { false, std::format("rename failed: {}", ec.message()) };
	}

	Utilities::fsync_parent_directory(target_path);

	return { true, std::nullopt };
}

auto HybridAdapter::current_time_ms(void) -> int64_t
{
	return current_time_ms_helper();
}

auto HybridAdapter::extract_message_id_from_key(const std::string& message_key) -> std::string
{
	auto pos = message_key.rfind(':');
	if (pos == std::string::npos)
	{
		return message_key;
	}
	return message_key.substr(pos + 1);
}

auto HybridAdapter::extract_queue_from_key(const std::string& message_key) -> std::string
{
	auto first = message_key.find(':');
	if (first == std::string::npos)
	{
		return "";
	}

	auto second = message_key.find(':', first + 1);
	if (second == std::string::npos)
	{
		return "";
	}

	return message_key.substr(first + 1, second - first - 1);
}

auto HybridAdapter::list_dlq_messages(const std::string& queue, int32_t limit)
	-> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	std::vector<DlqMessageInfo> dlq_list;

	if (!is_open_)
	{
		return { dlq_list, "adapter not open" };
	}

	std::string sql = std::format(
		"SELECT message_key, queue, dlq_reason, dlq_at, attempt FROM {} "
		"WHERE queue = ? AND state = 'dlq' ORDER BY dlq_at DESC LIMIT ?",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(sql);
	if (!stmt)
	{
		return { dlq_list, prep_error };
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

auto HybridAdapter::reprocess_dlq_message(const std::string& message_key)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { false, tx_error };
	}

	auto now = current_time_ms();

	// Get queue info first
	std::string check_sql = std::format(
		"SELECT queue FROM {} WHERE message_key = ? AND state = 'dlq'",
		sqlite_config_.message_index_table
	);

	auto [check_stmt, check_prep_error] = db_.prepare(check_sql);
	if (!check_stmt)
	{
		db_.rollback();
		return { false, check_prep_error };
	}
	check_stmt->bind_text(1, message_key);

	if (check_stmt->step() != SQLITE_ROW)
	{
		db_.rollback();
		return { false, "DLQ message not found" };
	}

	std::string queue = check_stmt->column_text(0);

	// Reset state to ready
	std::string update_idx_sql = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = ?, "
		"attempt = 0, dlq_reason = NULL, dlq_at = NULL WHERE message_key = ?",
		sqlite_config_.message_index_table
	);

	auto [idx_stmt, idx_prep_error] = db_.prepare(update_idx_sql);
	if (!idx_stmt)
	{
		db_.rollback();
		return { false, idx_prep_error };
	}
	idx_stmt->bind_int64(1, now);
	idx_stmt->bind_text(2, message_key);

	if (idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { false, "update index for reprocess failed" };
	}

	// Update KV to remove DLQ fields
	std::string update_kv_sql = std::format(
		"UPDATE {} SET value = json_remove(json_set(value, '$.attempt', 0), '$.dlqReason', '$.dlqAt'), "
		"updated_at = ? WHERE key = ?",
		sqlite_config_.kv_table
	);

	auto [kv_stmt, kv_prep_error] = db_.prepare(update_kv_sql);
	if (kv_stmt)
	{
		kv_stmt->bind_int64(1, now);
		kv_stmt->bind_text(2, message_key);
		kv_stmt->step();
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	// Move payload from DLQ back to active
	if (!queue.empty())
	{
		auto message_id = extract_message_id_from_key(message_key);
		auto dlq_path = build_dlq_path(queue, message_id);
		auto active_path = build_payload_path(queue, message_id);

		std::error_code ec;
		if (std::filesystem::exists(dlq_path, ec))
		{
			std::filesystem::rename(dlq_path, active_path, ec);
		}
	}

	return { true, std::nullopt };
}

auto HybridAdapter::get_all_queues(void) -> std::vector<std::string>
{
	std::vector<std::string> queues;

	std::string sql = std::format(
		"SELECT DISTINCT queue FROM {}",
		sqlite_config_.message_index_table
	);

	auto [stmt, prep_error] = db_.prepare(sql);
	if (stmt)
	{
		while (stmt->step() == SQLITE_ROW)
		{
			std::string q = stmt->column_text(0);
			if (!q.empty())
			{
				queues.push_back(q);
			}
		}
	}

	// Also check filesystem for queues
	std::error_code ec;
	if (std::filesystem::exists(payload_root_, ec))
	{
		for (const auto& entry : std::filesystem::directory_iterator(payload_root_, ec))
		{
			if (entry.is_directory())
			{
				std::string queue_name = entry.path().filename().string();
				if (std::find(queues.begin(), queues.end(), queue_name) == queues.end())
				{
					queues.push_back(queue_name);
				}
			}
		}
	}

	return queues;
}

auto HybridAdapter::list_payload_files(const std::string& queue, const std::string& subdir)
	-> std::tuple<std::vector<std::string>, std::optional<std::string>>
{
	std::vector<std::string> message_ids;

	std::string dir_path = std::format("{}/{}/{}", payload_root_, queue, subdir);

	std::error_code ec;
	if (!std::filesystem::exists(dir_path, ec))
	{
		return { message_ids, std::nullopt };
	}

	for (const auto& entry : std::filesystem::directory_iterator(dir_path, ec))
	{
		if (entry.is_regular_file())
		{
			std::string filename = entry.path().filename().string();
			// Remove .json extension
			if (filename.size() > 5 && filename.substr(filename.size() - 5) == ".json")
			{
				message_ids.push_back(filename.substr(0, filename.size() - 5));
			}
		}
	}

	return { message_ids, std::nullopt };
}

auto HybridAdapter::get_indexed_message_ids(const std::string& queue, const std::string& state)
	-> std::tuple<std::vector<std::string>, std::optional<std::string>>
{
	std::vector<std::string> message_ids;

	if (state.empty())
	{
		std::string sql = std::format(
			"SELECT message_key FROM {} WHERE queue = ?",
			sqlite_config_.message_index_table
		);

		auto [stmt, prep_error] = db_.prepare(sql);
		if (!stmt)
		{
			return { message_ids, prep_error };
		}
		stmt->bind_text(1, queue);

		while (stmt->step() == SQLITE_ROW)
		{
			std::string key = stmt->column_text(0);
			if (!key.empty())
			{
				auto message_id = extract_message_id_from_key(key);
				message_ids.push_back(message_id);
			}
		}
	}
	else
	{
		std::string sql = std::format(
			"SELECT message_key FROM {} WHERE queue = ? AND state = ?",
			sqlite_config_.message_index_table
		);

		auto [stmt, prep_error] = db_.prepare(sql);
		if (!stmt)
		{
			return { message_ids, prep_error };
		}
		stmt->bind_text(1, queue);
		stmt->bind_text(2, state);

		while (stmt->step() == SQLITE_ROW)
		{
			std::string key = stmt->column_text(0);
			if (!key.empty())
			{
				auto message_id = extract_message_id_from_key(key);
				message_ids.push_back(message_id);
			}
		}
	}

	return { message_ids, std::nullopt };
}

auto HybridAdapter::check_consistency(const std::string& queue)
	-> std::tuple<ConsistencyReport, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	ConsistencyReport report;

	if (!is_open_)
	{
		return { report, "adapter not open" };
	}

	std::vector<std::string> queues_to_check;
	if (queue.empty())
	{
		queues_to_check = get_all_queues();
	}
	else
	{
		queues_to_check.push_back(queue);
	}

	for (const auto& q : queues_to_check)
	{
		// Get indexed messages (ready, inflight, delayed states have payload in active/)
		auto [indexed_active, idx_error] = get_indexed_message_ids(q, "");
		if (idx_error.has_value())
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Error,
				std::format("Failed to get indexed messages for queue {}: {}", q, idx_error.value())
			);
			continue;
		}

		// Filter by state: ready, inflight, delayed should have payload in active/
		std::string active_sql = std::format(
			"SELECT message_key FROM {} WHERE queue = ? AND state IN ('ready', 'inflight', 'delayed')",
			sqlite_config_.message_index_table
		);

		auto [active_stmt, active_prep_error] = db_.prepare(active_sql);
		std::vector<std::string> indexed_active_ids;
		if (active_stmt)
		{
			active_stmt->bind_text(1, q);

			while (active_stmt->step() == SQLITE_ROW)
			{
				std::string key = active_stmt->column_text(0);
				if (!key.empty())
				{
					indexed_active_ids.push_back(extract_message_id_from_key(key));
				}
			}
		}

		// Get DLQ indexed messages
		std::string dlq_sql = std::format(
			"SELECT message_key FROM {} WHERE queue = ? AND state = 'dlq'",
			sqlite_config_.message_index_table
		);

		auto [dlq_stmt, dlq_prep_error] = db_.prepare(dlq_sql);
		std::vector<std::string> indexed_dlq_ids;
		if (dlq_stmt)
		{
			dlq_stmt->bind_text(1, q);

			while (dlq_stmt->step() == SQLITE_ROW)
			{
				std::string key = dlq_stmt->column_text(0);
				if (!key.empty())
				{
					indexed_dlq_ids.push_back(extract_message_id_from_key(key));
				}
			}
		}

		// Get actual payload files
		auto [active_files, active_files_error] = list_payload_files(q, "active");
		auto [dlq_files, dlq_files_error] = list_payload_files(q, "dlq");
		auto [archive_files, archive_files_error] = list_payload_files(q, "archive");

		// Check for orphan payloads in active/ (file exists but no index)
		for (const auto& file_id : active_files)
		{
			if (std::find(indexed_active_ids.begin(), indexed_active_ids.end(), file_id) == indexed_active_ids.end())
			{
				ConsistencyIssue issue;
				issue.type = ConsistencyIssueType::OrphanPayload;
				issue.queue = q;
				issue.message_key = std::format("msg:{}:{}", q, file_id);
				issue.payload_path = build_payload_path(q, file_id);
				issue.description = std::format("Orphan payload in active/: {}", file_id);
				report.issues.push_back(issue);
				report.orphan_payloads++;
			}
		}

		// Check for orphan payloads in dlq/ (file exists but no index)
		for (const auto& file_id : dlq_files)
		{
			if (std::find(indexed_dlq_ids.begin(), indexed_dlq_ids.end(), file_id) == indexed_dlq_ids.end())
			{
				ConsistencyIssue issue;
				issue.type = ConsistencyIssueType::OrphanPayload;
				issue.queue = q;
				issue.message_key = std::format("msg:{}:{}", q, file_id);
				issue.payload_path = build_dlq_path(q, file_id);
				issue.description = std::format("Orphan payload in dlq/: {}", file_id);
				report.issues.push_back(issue);
				report.orphan_payloads++;
			}
		}

		// Check for missing payloads (index exists but no file)
		for (const auto& idx_id : indexed_active_ids)
		{
			if (std::find(active_files.begin(), active_files.end(), idx_id) == active_files.end())
			{
				ConsistencyIssue issue;
				issue.type = ConsistencyIssueType::MissingPayload;
				issue.queue = q;
				issue.message_key = std::format("msg:{}:{}", q, idx_id);
				issue.payload_path = build_payload_path(q, idx_id);
				issue.description = std::format("Missing payload for indexed message: {}", idx_id);
				report.issues.push_back(issue);
				report.missing_payloads++;
			}
		}

		for (const auto& idx_id : indexed_dlq_ids)
		{
			if (std::find(dlq_files.begin(), dlq_files.end(), idx_id) == dlq_files.end())
			{
				ConsistencyIssue issue;
				issue.type = ConsistencyIssueType::MissingPayload;
				issue.queue = q;
				issue.message_key = std::format("msg:{}:{}", q, idx_id);
				issue.payload_path = build_dlq_path(q, idx_id);
				issue.description = std::format("Missing DLQ payload for indexed message: {}", idx_id);
				report.issues.push_back(issue);
				report.missing_payloads++;
			}
		}

		// Check for stale archives (older than retention period, e.g., 7 days)
		auto retention_ms = static_cast<int64_t>(7 * 24 * 60 * 60 * 1000); // 7 days

		for (const auto& archive_id : archive_files)
		{
			auto archive_path = build_archive_path(q, archive_id);

			std::error_code ec;
			auto last_write = std::filesystem::last_write_time(archive_path, ec);
			if (!ec)
			{
				// Age must be computed with the file clock's own now(): subtracting a
				// file_clock timestamp from a system_clock one mixes epochs (they differ on
				// libstdc++) and previously mis-flagged every archive as stale. (Defect D-08)
				auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
					std::filesystem::file_time_type::clock::now() - last_write
				).count();

				if (age_ms > retention_ms)
				{
					ConsistencyIssue issue;
					issue.type = ConsistencyIssueType::StaleArchive;
					issue.queue = q;
					issue.message_key = std::format("msg:{}:{}", q, archive_id);
					issue.payload_path = archive_path;
					issue.description = std::format("Stale archive (older than 7 days): {}", archive_id);
					report.issues.push_back(issue);
					report.stale_archives++;
				}
			}
		}
	}

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("Consistency check completed: {} orphan payloads, {} missing payloads, {} stale archives",
			report.orphan_payloads, report.missing_payloads, report.stale_archives)
	);

	return { report, std::nullopt };
}

auto HybridAdapter::repair_consistency(const ConsistencyReport& report)
	-> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	int32_t repaired = 0;
	auto now = current_time_ms();

	for (const auto& issue : report.issues)
	{
		switch (issue.type)
		{
		case ConsistencyIssueType::OrphanPayload:
		{
			// Create index entry for orphan payload
			std::ifstream file(issue.payload_path);
			if (file.is_open())
			{
				std::string content((std::istreambuf_iterator<char>(file)),
					std::istreambuf_iterator<char>());
				file.close();

				try
				{
					json payload_json = json::parse(content);
					std::string message_id = extract_message_id_from_key(issue.message_key);

					// Create envelope in KV
					json envelope;
					envelope["messageId"] = message_id;
					envelope["queue"] = issue.queue;
					envelope["payloadPath"] = issue.payload_path;
					envelope["priority"] = 0;
					envelope["attempt"] = 0;
					envelope["createdAt"] = now;
					envelope["repairedAt"] = now;

					std::string insert_kv_sql = std::format(
						"INSERT OR IGNORE INTO {} (key, value, value_type, created_at, updated_at) "
						"VALUES (?, ?, 'message', ?, ?)",
						sqlite_config_.kv_table
					);

					auto [kv_stmt, kv_prep_error] = db_.prepare(insert_kv_sql);
					bool kv_ok = false;
					if (kv_stmt)
					{
						kv_stmt->bind_text(1, issue.message_key);
						kv_stmt->bind_text(2, envelope.dump());
						kv_stmt->bind_int64(3, now);
						kv_stmt->bind_int64(4, now);
						kv_ok = (kv_stmt->step() == SQLITE_DONE);
					}

					// Create index entry
					std::string insert_idx_sql = std::format(
						"INSERT OR IGNORE INTO {} (queue, state, priority, available_at, attempt, message_key) "
						"VALUES (?, 'ready', 0, ?, 0, ?)",
						sqlite_config_.message_index_table
					);

					auto [idx_stmt, idx_prep_error] = db_.prepare(insert_idx_sql);
					bool idx_ok = false;
					if (idx_stmt)
					{
						idx_stmt->bind_text(1, issue.queue);
						idx_stmt->bind_int64(2, now);
						idx_stmt->bind_text(3, issue.message_key);
						idx_ok = (idx_stmt->step() == SQLITE_DONE);
					}

					if (kv_ok || idx_ok)
					{
						repaired++;
						Utilities::Logger::handle().write(
							Utilities::LogTypes::Information,
							std::format("Repaired orphan payload: {}", issue.message_key)
						);
					}
				}
				catch (const json::exception& e)
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to repair orphan payload {}: {}", issue.message_key, e.what())
					);
				}
			}
			break;
		}

		case ConsistencyIssueType::MissingPayload:
		{
			// Move to DLQ or delete index entry
			std::string update_sql = std::format(
				"UPDATE {} SET state = 'dlq', dlq_reason = 'missing_payload', dlq_at = ? "
				"WHERE message_key = ?",
				sqlite_config_.message_index_table
			);

			auto [stmt, prep_error] = db_.prepare(update_sql);
			if (stmt)
			{
				stmt->bind_int64(1, now);
				stmt->bind_text(2, issue.message_key);

				if (stmt->step() == SQLITE_DONE)
				{
					repaired++;
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Information,
						std::format("Moved message with missing payload to DLQ: {}", issue.message_key)
					);
				}
			}
			break;
		}

		case ConsistencyIssueType::StaleArchive:
		{
			// Delete stale archive files
			std::error_code ec;
			if (std::filesystem::remove(issue.payload_path, ec))
			{
				repaired++;
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Information,
					std::format("Deleted stale archive: {}", issue.payload_path)
				);
			}
			break;
		}

		case ConsistencyIssueType::InvalidState:
		{
			// Reset to ready state
			std::string update_sql = std::format(
				"UPDATE {} SET state = 'ready', available_at = ? WHERE message_key = ?",
				sqlite_config_.message_index_table
			);

			auto [stmt, prep_error] = db_.prepare(update_sql);
			if (stmt)
			{
				stmt->bind_int64(1, now);
				stmt->bind_text(2, issue.message_key);

				if (stmt->step() == SQLITE_DONE)
				{
					repaired++;
				}
			}
			break;
		}
		}
	}

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("Consistency repair completed: {} issues repaired", repaired)
	);

	return { repaired, std::nullopt };
}

auto HybridAdapter::purge_expired_messages(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto now = current_time_ms();

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { 0, tx_error };
	}

	// Get expired message keys and their queues for payload file cleanup
	std::string select_sql = std::format(
		"SELECT message_key, queue FROM {} WHERE expired_at > 0 AND expired_at <= ? AND state IN ('ready', 'delayed')",
		sqlite_config_.message_index_table
	);

	auto [select_stmt, select_prep_error] = db_.prepare(select_sql);
	if (!select_stmt)
	{
		db_.rollback();
		return { 0, select_prep_error };
	}
	select_stmt->bind_int64(1, now);

	struct ExpiredInfo
	{
		std::string message_key;
		std::string queue;
	};

	std::vector<ExpiredInfo> expired_list;
	while (select_stmt->step() == SQLITE_ROW)
	{
		expired_list.push_back({ select_stmt->column_text(0), select_stmt->column_text(1) });
	}

	if (expired_list.empty())
	{
		db_.rollback();
		return { 0, std::nullopt };
	}

	// Delete from msg_index
	std::string delete_idx_sql = std::format(
		"DELETE FROM {} WHERE expired_at > 0 AND expired_at <= ? AND state IN ('ready', 'delayed')",
		sqlite_config_.message_index_table
	);

	auto [del_idx_stmt, del_idx_prep_error] = db_.prepare(delete_idx_sql);
	if (!del_idx_stmt)
	{
		db_.rollback();
		return { 0, del_idx_prep_error };
	}
	del_idx_stmt->bind_int64(1, now);

	if (del_idx_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { 0, "delete expired index entries failed" };
	}

	// Delete from kv table
	std::string delete_kv_sql = std::format(
		"DELETE FROM {} WHERE key = ?",
		sqlite_config_.kv_table
	);

	for (const auto& info : expired_list)
	{
		auto [kv_stmt, kv_prep_error] = db_.prepare(delete_kv_sql);
		if (kv_stmt)
		{
			kv_stmt->bind_text(1, info.message_key);
			kv_stmt->step();
		}
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { 0, commit_error };
	}

	// Delete payload files (outside transaction)
	for (const auto& info : expired_list)
	{
		auto message_id = extract_message_id_from_key(info.message_key);
		auto payload_path = build_payload_path(info.queue, message_id);
		std::error_code ec;
		std::filesystem::remove(payload_path, ec);
	}

	return { static_cast<int32_t>(expired_list.size()), std::nullopt };
}

auto HybridAdapter::purge_dlq_messages(const std::string& queue, int64_t older_than_ms) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(db_mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto [tx_ok, tx_error] = db_.begin_transaction();
	if (!tx_ok)
	{
		return { 0, tx_error };
	}

	// DLQ entries with a recorded dlq_at at or before the cutoff are past retention. (D-07)
	std::string select_sql = std::format(
		"SELECT message_key FROM {} WHERE queue = ? AND state = 'dlq' AND dlq_at > 0 AND dlq_at <= ?",
		sqlite_config_.message_index_table
	);

	auto [select_stmt, select_error] = db_.prepare(select_sql);
	if (!select_stmt)
	{
		db_.rollback();
		return { 0, select_error };
	}
	select_stmt->bind_text(1, queue);
	select_stmt->bind_int64(2, older_than_ms);

	std::vector<std::string> keys;
	while (select_stmt->step() == SQLITE_ROW)
	{
		keys.push_back(select_stmt->column_text(0));
	}

	if (keys.empty())
	{
		db_.rollback();
		return { 0, std::nullopt };
	}

	std::string delete_idx_sql = std::format(
		"DELETE FROM {} WHERE queue = ? AND state = 'dlq' AND dlq_at > 0 AND dlq_at <= ?",
		sqlite_config_.message_index_table
	);

	auto [del_stmt, del_error] = db_.prepare(delete_idx_sql);
	if (!del_stmt)
	{
		db_.rollback();
		return { 0, del_error };
	}
	del_stmt->bind_text(1, queue);
	del_stmt->bind_int64(2, older_than_ms);

	if (del_stmt->step() != SQLITE_DONE)
	{
		db_.rollback();
		return { 0, "failed to delete dlq index rows" };
	}

	for (const auto& key : keys)
	{
		std::string delete_kv_sql = std::format("DELETE FROM {} WHERE key = ?", sqlite_config_.kv_table);
		auto [kv_stmt, kv_error] = db_.prepare(delete_kv_sql);
		if (kv_stmt)
		{
			kv_stmt->bind_text(1, key);
			kv_stmt->step();
		}
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { 0, commit_error };
	}

	// Best-effort removal of the DLQ payload files after the DB commit.
	for (const auto& key : keys)
	{
		auto message_id = extract_message_id_from_key(key);
		std::error_code ec;
		std::filesystem::remove(build_dlq_path(queue, message_id), ec);
	}

	return { static_cast<int32_t>(keys.size()), std::nullopt };
}
