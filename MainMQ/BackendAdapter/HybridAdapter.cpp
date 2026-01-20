#include "HybridAdapter.h"

#include "File.h"
#include "Generator.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

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

	return { true, std::nullopt };
}

auto HybridAdapter::load_schema_sql(void) -> std::tuple<std::optional<std::string>, std::optional<std::string>>
{
	Utilities::File file;
	auto [opened, open_error] = file.open(schema_path_, std::ios::in);
	if (!opened)
	{
		return { std::nullopt, std::format("cannot open schema file: {}", open_error.value_or("unknown")) };
	}

	auto [data, read_error] = file.read_bytes();
	file.close();

	if (!data.has_value())
	{
		return { std::nullopt, std::format("cannot read schema file: {}", read_error.value_or("unknown")) };
	}

	return { std::string(data->begin(), data->end()), std::nullopt };
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
	json payload_json;
	payload_json["payload"] = message.payload_json;
	payload_json["attributes"] = message.attributes_json;

	auto [write_ok, write_error] = atomic_write(payload_path, payload_json.dump(2));
	if (!write_ok)
	{
		db_.rollback();
		return { false, write_error };
	}

	json envelope;
	envelope["messageId"] = message.message_id;
	envelope["queue"] = message.queue;
	envelope["payloadPath"] = payload_path;
	envelope["priority"] = message.priority;
	envelope["attempt"] = message.attempt;
	envelope["createdAt"] = message.created_at_ms;

	std::string insert_kv = std::format(
		"INSERT INTO {} (key, value, value_type, created_at, updated_at) VALUES ('{}', '{}', 'message', {}, {})",
		sqlite_config_.kv_table,
		message.key,
		envelope.dump(),
		now,
		now
	);

	auto [kv_ok, kv_error] = db_.execute(insert_kv);
	if (!kv_ok)
	{
		db_.rollback();
		std::filesystem::remove(payload_path);
		return { false, std::format("kv insert failed: {}", kv_error.value_or("unknown")) };
	}

	std::string insert_idx = std::format(
		"INSERT INTO {} (queue, state, priority, available_at, lease_until, attempt, message_key) "
		"VALUES ('{}', '{}', {}, {}, NULL, {}, '{}')",
		sqlite_config_.message_index_table,
		message.queue,
		state,
		message.priority,
		message.available_at_ms,
		message.attempt,
		message.key
	);

	auto [idx_ok, idx_error] = db_.execute(insert_idx);
	if (!idx_ok)
	{
		db_.rollback();
		std::filesystem::remove(payload_path);
		return { false, std::format("index insert failed: {}", idx_error.value_or("unknown")) };
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
		"WHERE queue = '{}' AND state = 'ready' AND available_at <= {} "
		"ORDER BY priority DESC, available_at ASC LIMIT 1",
		sqlite_config_.message_index_table,
		queue,
		now
	);

	auto [query_result, query_error] = db_.query(select_sql);
	if (!query_result.has_value() || query_result->rows.empty())
	{
		db_.rollback();
		return result;
	}

	std::string message_key = query_result->rows[0][0];
	int32_t attempt = std::stoi(query_result->rows[0][1]);

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'inflight', lease_until = {}, attempt = {} WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		lease_until,
		attempt + 1,
		message_key
	);

	auto [update_ok, update_error] = db_.execute(update_sql);
	if (!update_ok)
	{
		db_.rollback();
		result.error = update_error;
		return result;
	}

	std::string kv_sql = std::format(
		"SELECT value FROM {} WHERE key = '{}'",
		sqlite_config_.kv_table,
		message_key
	);

	auto [kv_result, kv_error] = db_.query(kv_sql);
	if (!kv_result.has_value() || kv_result->rows.empty())
	{
		db_.rollback();
		result.error = "envelope not found";
		return result;
	}

	std::string envelope_json = kv_result->rows[0][0];

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
		"SELECT queue FROM {} WHERE message_key = '{}' AND state = 'inflight'",
		sqlite_config_.message_index_table,
		lease.message_key
	);

	auto [check_result, check_error] = db_.query(check_sql);
	if (!check_result.has_value() || check_result->rows.empty())
	{
		db_.rollback();
		return { false, "message not found or not inflight" };
	}

	std::string queue = check_result->rows[0][0];

	std::string delete_idx = std::format(
		"DELETE FROM {} WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		lease.message_key
	);

	auto [del_idx_ok, del_idx_error] = db_.execute(delete_idx);
	if (!del_idx_ok)
	{
		db_.rollback();
		return { false, del_idx_error };
	}

	std::string delete_kv = std::format(
		"DELETE FROM {} WHERE key = '{}'",
		sqlite_config_.kv_table,
		lease.message_key
	);

	auto [del_kv_ok, del_kv_error] = db_.execute(delete_kv);
	if (!del_kv_ok)
	{
		db_.rollback();
		return { false, del_kv_error };
	}

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	auto message_id = extract_message_id_from_key(lease.message_key);
	move_payload_to_archive(queue, message_id);

	return { true, std::nullopt };
}

auto HybridAdapter::nack(const LeaseToken& lease, const std::string& reason, const bool& requeue)
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
		"SELECT queue FROM {} WHERE message_key = '{}' AND state = 'inflight'",
		sqlite_config_.message_index_table,
		lease.message_key
	);

	auto [check_result, check_error] = db_.query(check_sql);
	if (!check_result.has_value() || check_result->rows.empty())
	{
		db_.rollback();
		return { false, "message not found or not inflight" };
	}

	std::string queue = check_result->rows[0][0];

	if (requeue)
	{
		std::string update_sql = std::format(
			"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = {} WHERE message_key = '{}'",
			sqlite_config_.message_index_table,
			now,
			lease.message_key
		);

		auto [update_ok, update_error] = db_.execute(update_sql);
		if (!update_ok)
		{
			db_.rollback();
			return { false, update_error };
		}
	}
	else
	{
		std::string update_sql = std::format(
			"UPDATE {} SET state = 'dlq', lease_until = NULL WHERE message_key = '{}'",
			sqlite_config_.message_index_table,
			lease.message_key
		);

		auto [update_ok, update_error] = db_.execute(update_sql);
		if (!update_ok)
		{
			db_.rollback();
			return { false, update_error };
		}

		std::string update_kv = std::format(
			"UPDATE {} SET value = json_set(value, '$.dlqReason', '{}', '$.dlqAt', {}), updated_at = {} WHERE key = '{}'",
			sqlite_config_.kv_table,
			reason,
			now,
			now,
			lease.message_key
		);

		db_.execute(update_kv);

		auto message_id = extract_message_id_from_key(lease.message_key);
		move_payload_to_dlq(queue, message_id);
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
		"UPDATE {} SET lease_until = {} WHERE message_key = '{}' AND state = 'inflight' AND lease_until > {}",
		sqlite_config_.message_index_table,
		new_lease_until,
		lease.message_key,
		now
	);

	return db_.execute(update_sql);
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
		"SELECT value FROM {} WHERE key = '{}'",
		sqlite_config_.kv_table,
		policy_key
	);

	auto [query_result, query_error] = db_.query(select_sql);
	if (!query_result.has_value() || query_result->rows.empty())
	{
		return { std::nullopt, std::nullopt };
	}

	std::string value_json = query_result->rows[0][0];

	try
	{
		json j = json::parse(value_json);

		QueuePolicy policy;
		policy.visibility_timeout_sec = j.value("visibilityTimeoutSec", 30);

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
		"INSERT INTO {} (key, value, value_type, created_at, updated_at) VALUES ('{}', '{}', 'policy', {}, {}) "
		"ON CONFLICT(key) DO UPDATE SET value = '{}', updated_at = {}",
		sqlite_config_.kv_table,
		policy_key,
		j.dump(),
		now,
		now,
		j.dump(),
		now
	);

	return db_.execute(upsert_sql);
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
		"SELECT state, COUNT(*) as cnt FROM {} WHERE queue = '{}' GROUP BY state",
		sqlite_config_.message_index_table,
		queue
	);

	auto [query_result, query_error] = db_.query(sql);
	if (!query_result.has_value())
	{
		return { m, query_error };
	}

	for (const auto& row : query_result->rows)
	{
		if (row.size() >= 2)
		{
			std::string state = row[0];
			uint64_t count = std::stoull(row[1]);

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
		"SELECT COUNT(*) FROM {} WHERE state = 'inflight' AND lease_until < {}",
		sqlite_config_.message_index_table,
		now
	);

	auto [count_result, count_error] = db_.query(count_sql);
	int32_t count = 0;
	if (count_result.has_value() && !count_result->rows.empty())
	{
		count = std::stoi(count_result->rows[0][0]);
	}

	if (count == 0)
	{
		return { 0, std::nullopt };
	}

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = {} "
		"WHERE state = 'inflight' AND lease_until < {}",
		sqlite_config_.message_index_table,
		now,
		now
	);

	auto [ok, error] = db_.execute(update_sql);
	if (!ok)
	{
		return { 0, error };
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
		"SELECT COUNT(*) FROM {} WHERE state = 'delayed' AND available_at <= {}",
		sqlite_config_.message_index_table,
		now
	);

	auto [count_result, count_error] = db_.query(count_sql);
	int32_t count = 0;
	if (count_result.has_value() && !count_result->rows.empty())
	{
		count = std::stoi(count_result->rows[0][0]);
	}

	if (count == 0)
	{
		return { 0, std::nullopt };
	}

	std::string update_sql = std::format(
		"UPDATE {} SET state = 'ready' WHERE state = 'delayed' AND available_at <= {}",
		sqlite_config_.message_index_table,
		now
	);

	auto [ok, error] = db_.execute(update_sql);
	if (!ok)
	{
		return { 0, error };
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
		"SELECT message_key, queue, attempt FROM {} WHERE state = 'inflight' AND lease_until < {}",
		sqlite_config_.message_index_table,
		now
	);

	auto [query_result, query_error] = db_.query(sql);
	if (!query_result.has_value())
	{
		return { expired, query_error };
	}

	for (const auto& row : query_result->rows)
	{
		if (row.size() >= 3)
		{
			ExpiredLeaseInfo info;
			info.message_key = row[0];
			info.queue = row[1];
			info.attempt = std::stoi(row[2]);
			expired.push_back(info);
		}
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
		"UPDATE {} SET state = '{}', lease_until = NULL, available_at = {} WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		new_state,
		available_at,
		message_key
	);

	return db_.execute(update_sql);
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
		"SELECT queue FROM {} WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		message_key
	);

	auto [check_result, check_error] = db_.query(check_sql);
	std::string queue;
	if (check_result.has_value() && !check_result->rows.empty())
	{
		queue = check_result->rows[0][0];
	}

	std::string update_idx = std::format(
		"UPDATE {} SET state = 'dlq', lease_until = NULL WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		message_key
	);

	auto [idx_ok, idx_error] = db_.execute(update_idx);
	if (!idx_ok)
	{
		db_.rollback();
		return { false, idx_error };
	}

	std::string update_kv = std::format(
		"UPDATE {} SET value = json_set(value, '$.dlqReason', '{}', '$.dlqAt', {}), updated_at = {} WHERE key = '{}'",
		sqlite_config_.kv_table,
		reason,
		now,
		now,
		message_key
	);

	db_.execute(update_kv);

	auto [commit_ok, commit_error] = db_.commit();
	if (!commit_ok)
	{
		db_.rollback();
		return { false, commit_error };
	}

	if (!queue.empty())
	{
		auto message_id = extract_message_id_from_key(message_key);
		move_payload_to_dlq(queue, message_id);
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

	std::ofstream file(temp_path, std::ios::out | std::ios::trunc);
	if (!file.is_open())
	{
		return { false, std::format("cannot create temp file: {}", temp_path) };
	}

	file << content;
	file.flush();
	file.close();

	std::error_code ec;
	std::filesystem::rename(temp_path, target_path, ec);
	if (ec)
	{
		std::filesystem::remove(temp_path, ec);
		return { false, std::format("rename failed: {}", ec.message()) };
	}

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
		"WHERE queue = '{}' AND state = 'dlq' ORDER BY dlq_at DESC LIMIT {}",
		sqlite_config_.message_index_table,
		queue,
		limit
	);

	auto [result, error] = db_.query(sql);
	if (!result.has_value())
	{
		return { dlq_list, error };
	}

	for (const auto& row : result->rows)
	{
		if (row.size() < 5)
		{
			continue;
		}

		DlqMessageInfo info;
		info.message_key = row[0];
		info.queue = row[1];
		info.reason = row[2];
		info.dlq_at_ms = row[3].empty() ? 0 : std::stoll(row[3]);
		info.attempt = row[4].empty() ? 0 : std::stoi(row[4]);
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
		"SELECT queue FROM {} WHERE message_key = '{}' AND state = 'dlq'",
		sqlite_config_.message_index_table,
		message_key
	);

	auto [check_result, check_error] = db_.query(check_sql);
	if (!check_result.has_value() || check_result->rows.empty())
	{
		db_.rollback();
		return { false, "DLQ message not found" };
	}

	std::string queue = check_result->rows[0][0];

	// Reset state to ready
	std::string update_idx = std::format(
		"UPDATE {} SET state = 'ready', lease_until = NULL, available_at = {}, "
		"attempt = 0, dlq_reason = NULL, dlq_at = NULL WHERE message_key = '{}'",
		sqlite_config_.message_index_table,
		now,
		message_key
	);

	auto [idx_ok, idx_error] = db_.execute(update_idx);
	if (!idx_ok)
	{
		db_.rollback();
		return { false, idx_error };
	}

	// Update KV to remove DLQ fields
	std::string update_kv = std::format(
		"UPDATE {} SET value = json_remove(json_set(value, '$.attempt', 0), '$.dlqReason', '$.dlqAt'), "
		"updated_at = {} WHERE key = '{}'",
		sqlite_config_.kv_table,
		now,
		message_key
	);
	db_.execute(update_kv);

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

	auto [result, error] = db_.query(sql);
	if (result.has_value())
	{
		for (const auto& row : result->rows)
		{
			if (!row.empty() && !row[0].empty())
			{
				queues.push_back(row[0]);
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

	std::string sql;
	if (state.empty())
	{
		sql = std::format(
			"SELECT message_key FROM {} WHERE queue = '{}'",
			sqlite_config_.message_index_table,
			queue
		);
	}
	else
	{
		sql = std::format(
			"SELECT message_key FROM {} WHERE queue = '{}' AND state = '{}'",
			sqlite_config_.message_index_table,
			queue,
			state
		);
	}

	auto [result, error] = db_.query(sql);
	if (!result.has_value())
	{
		return { message_ids, error };
	}

	for (const auto& row : result->rows)
	{
		if (!row.empty() && !row[0].empty())
		{
			// Extract message_id from message_key (format: msg:queue:message_id)
			auto message_id = extract_message_id_from_key(row[0]);
			message_ids.push_back(message_id);
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
			"SELECT message_key FROM {} WHERE queue = '{}' AND state IN ('ready', 'inflight', 'delayed')",
			sqlite_config_.message_index_table,
			q
		);

		auto [active_result, active_error] = db_.query(active_sql);
		std::vector<std::string> indexed_active_ids;
		if (active_result.has_value())
		{
			for (const auto& row : active_result->rows)
			{
				if (!row.empty())
				{
					indexed_active_ids.push_back(extract_message_id_from_key(row[0]));
				}
			}
		}

		// Get DLQ indexed messages
		std::string dlq_sql = std::format(
			"SELECT message_key FROM {} WHERE queue = '{}' AND state = 'dlq'",
			sqlite_config_.message_index_table,
			q
		);

		auto [dlq_result, dlq_error] = db_.query(dlq_sql);
		std::vector<std::string> indexed_dlq_ids;
		if (dlq_result.has_value())
		{
			for (const auto& row : dlq_result->rows)
			{
				if (!row.empty())
				{
					indexed_dlq_ids.push_back(extract_message_id_from_key(row[0]));
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
		auto now = current_time_ms();
		auto retention_ms = static_cast<int64_t>(7 * 24 * 60 * 60 * 1000); // 7 days

		for (const auto& archive_id : archive_files)
		{
			auto archive_path = build_archive_path(q, archive_id);

			std::error_code ec;
			auto last_write = std::filesystem::last_write_time(archive_path, ec);
			if (!ec)
			{
				auto file_time = std::chrono::duration_cast<std::chrono::milliseconds>(
					last_write.time_since_epoch()
				).count();

				// Convert to comparable time (approximate since different clocks)
				auto age_ms = now - file_time;
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

					std::string insert_kv = std::format(
						"INSERT OR IGNORE INTO {} (key, value, value_type, created_at, updated_at) "
						"VALUES ('{}', '{}', 'message', {}, {})",
						sqlite_config_.kv_table,
						issue.message_key,
						envelope.dump(),
						now,
						now
					);

					auto [kv_ok, kv_error] = db_.execute(insert_kv);

					// Create index entry
					std::string insert_idx = std::format(
						"INSERT OR IGNORE INTO {} (queue, state, priority, available_at, attempt, message_key) "
						"VALUES ('{}', 'ready', 0, {}, 0, '{}')",
						sqlite_config_.message_index_table,
						issue.queue,
						now,
						issue.message_key
					);

					auto [idx_ok, idx_error] = db_.execute(insert_idx);

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
				"UPDATE {} SET state = 'dlq', dlq_reason = 'missing_payload', dlq_at = {} "
				"WHERE message_key = '{}'",
				sqlite_config_.message_index_table,
				now,
				issue.message_key
			);

			auto [ok, error] = db_.execute(update_sql);
			if (ok)
			{
				repaired++;
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Information,
					std::format("Moved message with missing payload to DLQ: {}", issue.message_key)
				);
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
				"UPDATE {} SET state = 'ready', available_at = {} WHERE message_key = '{}'",
				sqlite_config_.message_index_table,
				now,
				issue.message_key
			);

			auto [ok, error] = db_.execute(update_sql);
			if (ok)
			{
				repaired++;
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
