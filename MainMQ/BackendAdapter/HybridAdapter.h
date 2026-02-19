#pragma once

#include "BackendAdapter.h"

#include "SQLite.h"

#include <mutex>
#include <string>

// Hybrid Backend: Payload stored in files, state/index in SQLite
class HybridAdapter : public BackendAdapter
{
public:
	HybridAdapter(const std::string& schema_path);
	~HybridAdapter(void) override;

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

	// Consistency check and repair
	auto check_consistency(const std::string& queue = "")
		-> std::tuple<ConsistencyReport, std::optional<std::string>> override;
	auto repair_consistency(const ConsistencyReport& report)
		-> std::tuple<int32_t, std::optional<std::string>> override;

private:
	// Database operations
	auto apply_pragmas(void) -> std::tuple<bool, std::optional<std::string>>;
	auto ensure_schema(void) -> std::tuple<bool, std::optional<std::string>>;
	auto load_schema_sql(void) -> std::tuple<std::optional<std::string>, std::optional<std::string>>;

	// File operations for payload
	auto ensure_payload_directories(const std::string& queue) -> std::tuple<bool, std::optional<std::string>>;
	auto build_payload_path(const std::string& queue, const std::string& message_id) -> std::string;
	auto build_archive_path(const std::string& queue, const std::string& message_id) -> std::string;
	auto build_dlq_path(const std::string& queue, const std::string& message_id) -> std::string;

	auto write_payload(const std::string& queue, const std::string& message_id, const std::string& payload) -> std::tuple<bool, std::optional<std::string>>;
	auto read_payload(const std::string& queue, const std::string& message_id) -> std::tuple<std::optional<std::string>, std::optional<std::string>>;
	auto move_payload_to_archive(const std::string& queue, const std::string& message_id) -> std::tuple<bool, std::optional<std::string>>;
	auto move_payload_to_dlq(const std::string& queue, const std::string& message_id) -> std::tuple<bool, std::optional<std::string>>;

	auto atomic_write(const std::string& target_path, const std::string& content) -> std::tuple<bool, std::optional<std::string>>;

	// Utilities
	auto current_time_ms(void) -> int64_t;
	auto extract_message_id_from_key(const std::string& message_key) -> std::string;
	auto extract_queue_from_key(const std::string& message_key) -> std::string;

	// Consistency helpers
	auto get_all_queues(void) -> std::vector<std::string>;
	auto list_payload_files(const std::string& queue, const std::string& subdir)
		-> std::tuple<std::vector<std::string>, std::optional<std::string>>;
	auto get_indexed_message_ids(const std::string& queue, const std::string& state)
		-> std::tuple<std::vector<std::string>, std::optional<std::string>>;

private:
	bool is_open_;
	std::string schema_path_;
	std::string payload_root_;
	SQLiteConfig sqlite_config_;
	DataBase::SQLite db_;
	mutable std::mutex db_mutex_;
};
