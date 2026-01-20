#pragma once

#include "BackendAdapter.h"

#include <map>
#include <mutex>
#include <string>

class FileSystemAdapter : public BackendAdapter
{
public:
	FileSystemAdapter(void);
	~FileSystemAdapter(void) override;

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

private:
	// Directory structure
	auto ensure_directories(void) -> std::tuple<bool, std::optional<std::string>>;
	auto ensure_queue_directories(const std::string& queue) -> std::tuple<bool, std::optional<std::string>>;
	auto build_queue_path(const std::string& queue, const std::string& sub_dir, const std::string& filename = "") -> std::string;
	auto build_meta_path(const std::string& filename = "") -> std::string;

	// File operations (atomic write)
	auto atomic_write(const std::string& target_path, const std::string& content) -> std::tuple<bool, std::optional<std::string>>;
	auto read_file(const std::string& file_path) -> std::tuple<std::optional<std::string>, std::optional<std::string>>;
	auto move_file(const std::string& src, const std::string& dest) -> std::tuple<bool, std::optional<std::string>>;
	auto delete_file(const std::string& file_path) -> std::tuple<bool, std::optional<std::string>>;
	auto list_json_files(const std::string& dir_path) -> std::vector<std::string>;

	// Message serialization
	auto serialize_envelope(const MessageEnvelope& envelope) -> std::string;
	auto deserialize_envelope(const std::string& json_content, const std::string& file_path) -> std::tuple<std::optional<MessageEnvelope>, std::optional<std::string>>;

	// Lease meta operations
	struct LeaseMeta
	{
		std::string consumer_id;
		std::string lease_id;
		int64_t lease_until_ms = 0;
		int32_t attempt = 0;
		std::string queue;
	};

	auto write_lease_meta(const std::string& message_key, const LeaseMeta& meta) -> std::tuple<bool, std::optional<std::string>>;
	auto read_lease_meta(const std::string& message_key) -> std::tuple<std::optional<LeaseMeta>, std::optional<std::string>>;
	auto delete_lease_meta(const std::string& message_key) -> std::tuple<bool, std::optional<std::string>>;

	// Delayed message meta
	struct DelayedMeta
	{
		std::string message_key;
		std::string queue;
		int64_t available_at_ms = 0;
		int32_t attempt = 0;
	};

	auto write_delayed_meta(const std::string& message_key, const DelayedMeta& meta) -> std::tuple<bool, std::optional<std::string>>;
	auto delete_delayed_meta(const std::string& message_key) -> std::tuple<bool, std::optional<std::string>>;

	// Utilities
	auto current_time_ms(void) -> int64_t;
	auto generate_uuid(void) -> std::string;
	auto extract_queue_from_key(const std::string& message_key) -> std::string;

private:
	bool is_open_;
	FileSystemConfig fs_config_;
	std::map<std::string, QueuePolicy> policies_;
	mutable std::mutex mutex_;
};
