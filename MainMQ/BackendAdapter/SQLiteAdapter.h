#pragma once

#include "BackendAdapter.h"

#include "SQLite.h"

#include <mutex>
#include <string>

class SQLiteAdapter : public BackendAdapter
{
public:
	SQLiteAdapter(const std::string& schema_path);
	~SQLiteAdapter(void) override;

	auto open(const BackendConfig& config) -> std::tuple<bool, std::optional<std::string>> override;
	auto close(void) -> void override;

	auto enqueue(const MessageEnvelope& message) -> std::tuple<bool, std::optional<std::string>> override;
	auto lease_next(const std::string& queue, const std::string& consumer_id, const int32_t& visibility_timeout_sec)
		-> LeaseResult override;
	auto ack(const LeaseToken& lease) -> std::tuple<bool, std::optional<std::string>> override;
	auto nack(const LeaseToken& lease, const std::string& reason, const bool& requeue)
		-> std::tuple<bool, std::optional<std::string>> override;
	auto extend_lease(const LeaseToken& lease, const int32_t& visibility_timeout_sec)
		-> std::tuple<bool, std::optional<std::string>> override;

	auto load_policy(const std::string& queue) -> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>> override;
	auto save_policy(const std::string& queue, const QueuePolicy& policy) -> std::tuple<bool, std::optional<std::string>> override;

	auto metrics(const std::string& queue) -> std::tuple<QueueMetrics, std::optional<std::string>> override;

	auto recover_expired_leases(void) -> std::tuple<int32_t, std::optional<std::string>> override;
	auto process_delayed_messages(void) -> std::tuple<int32_t, std::optional<std::string>> override;

	auto get_expired_inflight_messages(void) -> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>> override;
	auto delay_message(const std::string& message_key, int64_t delay_ms) -> std::tuple<bool, std::optional<std::string>> override;
	auto move_to_dlq(const std::string& message_key, const std::string& reason) -> std::tuple<bool, std::optional<std::string>> override;

	auto list_dlq_messages(const std::string& queue, int32_t limit) -> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>> override;
	auto reprocess_dlq_message(const std::string& message_key) -> std::tuple<bool, std::optional<std::string>> override;

	auto purge_expired_messages(void) -> std::tuple<int32_t, std::optional<std::string>> override;

private:
	auto apply_pragmas(void) -> std::tuple<bool, std::optional<std::string>>;
	auto ensure_schema(void) -> std::tuple<bool, std::optional<std::string>>;
	auto load_schema_sql(void) -> std::tuple<std::optional<std::string>, std::optional<std::string>>;

private:
	bool is_open_;
	std::string schema_path_;
	SQLiteConfig sqlite_config_;
	DataBase::SQLite db_;
	mutable std::mutex db_mutex_;
};
