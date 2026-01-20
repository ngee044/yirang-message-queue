#include "FileSystemAdapter.h"

#include "Generator.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>

using json = nlohmann::json;

FileSystemAdapter::FileSystemAdapter(void)
	: is_open_(false)
{
}

FileSystemAdapter::~FileSystemAdapter(void)
{
	close();
}

auto FileSystemAdapter::open(const BackendConfig& config) -> std::tuple<bool, std::optional<std::string>>
{
	if (is_open_)
	{
		return { false, "already open" };
	}

	fs_config_ = config.filesystem;

	auto [ok, error] = ensure_directories();
	if (!ok)
	{
		return { false, error };
	}

	is_open_ = true;

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("FileSystemAdapter opened (root: {})", fs_config_.root)
	);

	return { true, std::nullopt };
}

auto FileSystemAdapter::close(void) -> void
{
	if (!is_open_)
	{
		return;
	}

	is_open_ = false;
	policies_.clear();
}

auto FileSystemAdapter::ensure_directories(void) -> std::tuple<bool, std::optional<std::string>>
{
	std::vector<std::string> dirs = {
		fs_config_.root,
		build_meta_path()
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

auto FileSystemAdapter::ensure_queue_directories(const std::string& queue) -> std::tuple<bool, std::optional<std::string>>
{
	std::vector<std::string> dirs = {
		build_queue_path(queue, fs_config_.inbox_dir),
		build_queue_path(queue, fs_config_.processing_dir),
		build_queue_path(queue, fs_config_.archive_dir),
		build_queue_path(queue, fs_config_.dlq_dir),
		build_queue_path(queue, "delayed")
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

auto FileSystemAdapter::build_queue_path(const std::string& queue, const std::string& sub_dir, const std::string& filename) -> std::string
{
	std::filesystem::path path = fs_config_.root;
	path /= queue;
	path /= sub_dir;
	if (!filename.empty())
	{
		path /= filename;
	}
	return path.string();
}

auto FileSystemAdapter::build_meta_path(const std::string& filename) -> std::string
{
	std::filesystem::path path = fs_config_.root;
	path /= fs_config_.meta_dir;
	if (!filename.empty())
	{
		path /= filename;
	}
	return path.string();
}

auto FileSystemAdapter::enqueue(const MessageEnvelope& message) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto [dirs_ok, dirs_error] = ensure_queue_directories(message.queue);
	if (!dirs_ok)
	{
		return { false, dirs_error };
	}

	auto now = current_time_ms();
	auto filename = std::format("{}.json", message.message_id);

	// Determine destination based on available_at
	std::string target_path;
	if (message.available_at_ms > now)
	{
		// Delayed message
		target_path = build_queue_path(message.queue, "delayed", filename);

		// Write delayed meta
		DelayedMeta meta;
		meta.message_key = message.key;
		meta.queue = message.queue;
		meta.available_at_ms = message.available_at_ms;
		meta.attempt = message.attempt;
		write_delayed_meta(message.key, meta);
	}
	else
	{
		// Ready message - goes to inbox
		target_path = build_queue_path(message.queue, fs_config_.inbox_dir, filename);
	}

	auto content = serialize_envelope(message);
	return atomic_write(target_path, content);
}

auto FileSystemAdapter::lease_next(const std::string& queue, const std::string& consumer_id, const int32_t& visibility_timeout_sec)
	-> LeaseResult
{
	std::lock_guard<std::mutex> lock(mutex_);

	LeaseResult result;
	result.leased = false;

	if (!is_open_)
	{
		result.error = "adapter not open";
		return result;
	}

	auto inbox_dir = build_queue_path(queue, fs_config_.inbox_dir);
	auto files = list_json_files(inbox_dir);

	if (files.empty())
	{
		return result;
	}

	// Get the first file (oldest)
	auto& file_path = files.front();

	// Read and parse the message
	auto [content, read_error] = read_file(file_path);
	if (!content.has_value())
	{
		result.error = read_error;
		return result;
	}

	auto [envelope_opt, parse_error] = deserialize_envelope(content.value(), file_path);
	if (!envelope_opt.has_value())
	{
		result.error = parse_error;
		return result;
	}

	auto& envelope = envelope_opt.value();

	// Move to processing
	std::filesystem::path src_path(file_path);
	auto filename = src_path.filename().string();
	auto processing_path = build_queue_path(queue, fs_config_.processing_dir, filename);

	auto [moved, move_error] = move_file(file_path, processing_path);
	if (!moved)
	{
		result.error = move_error;
		return result;
	}

	// Create lease token
	auto lease_id = generate_uuid();
	auto now = current_time_ms();
	auto lease_until = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);

	LeaseToken lease;
	lease.lease_id = lease_id;
	lease.message_key = envelope.key;
	lease.consumer_id = consumer_id;
	lease.lease_until_ms = lease_until;

	// Write lease meta
	LeaseMeta meta;
	meta.consumer_id = consumer_id;
	meta.lease_id = lease_id;
	meta.lease_until_ms = lease_until;
	meta.attempt = envelope.attempt + 1;
	meta.queue = queue;

	auto [meta_ok, meta_error] = write_lease_meta(envelope.key, meta);
	if (!meta_ok)
	{
		// Rollback: move back to inbox
		move_file(processing_path, file_path);
		result.error = meta_error;
		return result;
	}

	// Update attempt count in envelope file
	envelope.attempt = meta.attempt;
	atomic_write(processing_path, serialize_envelope(envelope));

	result.leased = true;
	result.message = envelope;
	result.lease = lease;

	return result;
}

auto FileSystemAdapter::ack(const LeaseToken& lease) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	// Read lease meta
	auto [meta_opt, meta_error] = read_lease_meta(lease.message_key);
	if (!meta_opt.has_value())
	{
		return { false, std::format("lease not found: {}", meta_error.value_or("unknown")) };
	}

	auto& meta = meta_opt.value();

	// Verify lease
	auto now = current_time_ms();
	if (now > meta.lease_until_ms)
	{
		return { false, "lease expired" };
	}

	if (meta.consumer_id != lease.consumer_id)
	{
		return { false, "consumer_id mismatch" };
	}

	// Extract filename from message key
	auto parts = lease.message_key.rfind(':');
	if (parts == std::string::npos)
	{
		return { false, "invalid message key format" };
	}
	auto message_id = lease.message_key.substr(parts + 1);
	auto filename = std::format("{}.json", message_id);

	// Move from processing to archive
	auto processing_path = build_queue_path(meta.queue, fs_config_.processing_dir, filename);
	auto archive_path = build_queue_path(meta.queue, fs_config_.archive_dir, filename);

	auto [moved, move_error] = move_file(processing_path, archive_path);
	if (!moved)
	{
		return { false, move_error };
	}

	// Delete lease meta
	delete_lease_meta(lease.message_key);

	return { true, std::nullopt };
}

auto FileSystemAdapter::nack(const LeaseToken& lease, const std::string& reason, const bool& requeue)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	// Read lease meta
	auto [meta_opt, meta_error] = read_lease_meta(lease.message_key);
	if (!meta_opt.has_value())
	{
		return { false, std::format("lease not found: {}", meta_error.value_or("unknown")) };
	}

	auto& meta = meta_opt.value();

	// Extract filename from message key
	auto parts = lease.message_key.rfind(':');
	if (parts == std::string::npos)
	{
		return { false, "invalid message key format" };
	}
	auto message_id = lease.message_key.substr(parts + 1);
	auto filename = std::format("{}.json", message_id);

	auto processing_path = build_queue_path(meta.queue, fs_config_.processing_dir, filename);

	if (requeue)
	{
		// Move back to inbox
		auto inbox_path = build_queue_path(meta.queue, fs_config_.inbox_dir, filename);
		auto [moved, move_error] = move_file(processing_path, inbox_path);
		if (!moved)
		{
			return { false, move_error };
		}
	}
	else
	{
		// Move to DLQ
		auto dlq_path = build_queue_path(meta.queue, fs_config_.dlq_dir, filename);

		// Read and update message with DLQ info
		auto [content, read_error] = read_file(processing_path);
		if (content.has_value())
		{
			try
			{
				json envelope = json::parse(content.value());
				envelope["dlqReason"] = reason;
				envelope["dlqAt"] = current_time_ms();
				atomic_write(processing_path, envelope.dump(2));
			}
			catch (...)
			{
				// Continue even if update fails
			}
		}

		auto [moved, move_error] = move_file(processing_path, dlq_path);
		if (!moved)
		{
			return { false, move_error };
		}
	}

	// Delete lease meta
	delete_lease_meta(lease.message_key);

	return { true, std::nullopt };
}

auto FileSystemAdapter::extend_lease(const LeaseToken& lease, const int32_t& visibility_timeout_sec)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	// Read lease meta
	auto [meta_opt, meta_error] = read_lease_meta(lease.message_key);
	if (!meta_opt.has_value())
	{
		return { false, std::format("lease not found: {}", meta_error.value_or("unknown")) };
	}

	auto& meta = meta_opt.value();

	// Verify consumer
	if (meta.consumer_id != lease.consumer_id)
	{
		return { false, "consumer_id mismatch" };
	}

	// Check if lease is still valid
	auto now = current_time_ms();
	if (now > meta.lease_until_ms)
	{
		return { false, "lease expired" };
	}

	// Extend lease
	meta.lease_until_ms = now + (static_cast<int64_t>(visibility_timeout_sec) * 1000);

	return write_lease_meta(lease.message_key, meta);
}

auto FileSystemAdapter::load_policy(const std::string& queue) -> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	auto it = policies_.find(queue);
	if (it != policies_.end())
	{
		return { it->second, std::nullopt };
	}

	// Try to load from file
	auto policy_file = build_meta_path(std::format("policy_{}.json", queue));
	auto [content, read_error] = read_file(policy_file);

	if (!content.has_value())
	{
		return { std::nullopt, std::nullopt };
	}

	try
	{
		json j = json::parse(content.value());

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

		policies_[queue] = policy;
		return { policy, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("policy parse error: {}", e.what()) };
	}
}

auto FileSystemAdapter::save_policy(const std::string& queue, const QueuePolicy& policy) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	policies_[queue] = policy;

	// Ensure queue directories exist
	ensure_queue_directories(queue);

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

	auto policy_file = build_meta_path(std::format("policy_{}.json", queue));
	return atomic_write(policy_file, j.dump(2));
}

auto FileSystemAdapter::metrics(const std::string& queue) -> std::tuple<QueueMetrics, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	QueueMetrics m;

	if (!is_open_)
	{
		return { m, "adapter not open" };
	}

	m.ready = list_json_files(build_queue_path(queue, fs_config_.inbox_dir)).size();
	m.inflight = list_json_files(build_queue_path(queue, fs_config_.processing_dir)).size();
	m.delayed = list_json_files(build_queue_path(queue, "delayed")).size();
	m.dlq = list_json_files(build_queue_path(queue, fs_config_.dlq_dir)).size();

	return { m, std::nullopt };
}

auto FileSystemAdapter::recover_expired_leases(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto now = current_time_ms();
	int32_t recovered = 0;

	// Scan all lease meta files
	auto meta_dir = build_meta_path("leases");
	std::error_code ec;
	if (!std::filesystem::exists(meta_dir, ec))
	{
		return { 0, std::nullopt };
	}

	for (const auto& entry : std::filesystem::directory_iterator(meta_dir, ec))
	{
		if (!entry.is_regular_file())
		{
			continue;
		}

		auto [content, read_error] = read_file(entry.path().string());
		if (!content.has_value())
		{
			continue;
		}

		try
		{
			json j = json::parse(content.value());
			int64_t lease_until = j.value("leaseUntil", static_cast<int64_t>(0));

			if (now > lease_until)
			{
				// Lease expired - move message back to inbox
				std::string message_key = j.value("messageKey", "");
				std::string queue_name = j.value("queue", "");

				if (message_key.empty() || queue_name.empty())
				{
					continue;
				}

				auto parts = message_key.rfind(':');
				if (parts == std::string::npos)
				{
					continue;
				}
				auto message_id = message_key.substr(parts + 1);
				auto filename = std::format("{}.json", message_id);

				auto processing_path = build_queue_path(queue_name, fs_config_.processing_dir, filename);
				auto inbox_path = build_queue_path(queue_name, fs_config_.inbox_dir, filename);

				if (std::filesystem::exists(processing_path, ec))
				{
					std::filesystem::rename(processing_path, inbox_path, ec);
					if (!ec)
					{
						recovered++;
					}
				}

				// Delete lease meta
				std::filesystem::remove(entry.path(), ec);
			}
		}
		catch (...)
		{
			continue;
		}
	}

	return { recovered, std::nullopt };
}

auto FileSystemAdapter::process_delayed_messages(void) -> std::tuple<int32_t, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { 0, "adapter not open" };
	}

	auto now = current_time_ms();
	int32_t processed = 0;

	// Scan all queue directories for delayed messages
	std::error_code ec;
	for (const auto& entry : std::filesystem::directory_iterator(fs_config_.root, ec))
	{
		if (!entry.is_directory())
		{
			continue;
		}

		auto queue_name = entry.path().filename().string();
		if (queue_name == fs_config_.meta_dir)
		{
			continue;
		}

		auto delayed_dir = build_queue_path(queue_name, "delayed");
		if (!std::filesystem::exists(delayed_dir, ec))
		{
			continue;
		}

		auto files = list_json_files(delayed_dir);
		for (const auto& file_path : files)
		{
			auto [content, read_error] = read_file(file_path);
			if (!content.has_value())
			{
				continue;
			}

			try
			{
				json envelope = json::parse(content.value());
				int64_t available_at = envelope.value("availableAt", static_cast<int64_t>(0));

				if (now >= available_at)
				{
					// Move to inbox
					std::filesystem::path src_path(file_path);
					auto filename = src_path.filename().string();
					auto inbox_path = build_queue_path(queue_name, fs_config_.inbox_dir, filename);

					std::filesystem::rename(file_path, inbox_path, ec);
					if (!ec)
					{
						processed++;

						// Delete delayed meta if exists
						std::string message_key = envelope.value("key", "");
						if (!message_key.empty())
						{
							delete_delayed_meta(message_key);
						}
					}
				}
			}
			catch (...)
			{
				continue;
			}
		}
	}

	return { processed, std::nullopt };
}

auto FileSystemAdapter::get_expired_inflight_messages(void) -> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	std::vector<ExpiredLeaseInfo> expired;

	if (!is_open_)
	{
		return { expired, "adapter not open" };
	}

	auto now = current_time_ms();
	auto meta_dir = build_meta_path("leases");

	std::error_code ec;
	if (!std::filesystem::exists(meta_dir, ec))
	{
		return { expired, std::nullopt };
	}

	for (const auto& entry : std::filesystem::directory_iterator(meta_dir, ec))
	{
		if (!entry.is_regular_file())
		{
			continue;
		}

		auto [content, read_error] = read_file(entry.path().string());
		if (!content.has_value())
		{
			continue;
		}

		try
		{
			json j = json::parse(content.value());
			int64_t lease_until = j.value("leaseUntil", static_cast<int64_t>(0));

			if (now > lease_until)
			{
				ExpiredLeaseInfo info;
				info.message_key = j.value("messageKey", "");
				info.queue = j.value("queue", "");
				info.attempt = j.value("attempt", 0);
				expired.push_back(info);
			}
		}
		catch (...)
		{
			continue;
		}
	}

	return { expired, std::nullopt };
}

auto FileSystemAdapter::delay_message(const std::string& message_key, int64_t delay_ms) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	// Read lease meta to get queue info
	auto [meta_opt, meta_error] = read_lease_meta(message_key);
	if (!meta_opt.has_value())
	{
		return { false, meta_error };
	}

	auto& meta = meta_opt.value();
	auto queue = meta.queue;

	auto parts = message_key.rfind(':');
	if (parts == std::string::npos)
	{
		return { false, "invalid message key format" };
	}
	auto message_id = message_key.substr(parts + 1);
	auto filename = std::format("{}.json", message_id);

	auto processing_path = build_queue_path(queue, fs_config_.processing_dir, filename);

	if (delay_ms <= 0)
	{
		// Move directly to inbox
		auto inbox_path = build_queue_path(queue, fs_config_.inbox_dir, filename);
		auto [moved, move_error] = move_file(processing_path, inbox_path);
		if (!moved)
		{
			return { false, move_error };
		}
	}
	else
	{
		// Move to delayed folder
		auto delayed_path = build_queue_path(queue, "delayed", filename);

		// Update available_at in the message
		auto [content, read_error] = read_file(processing_path);
		if (content.has_value())
		{
			try
			{
				json envelope = json::parse(content.value());
				envelope["availableAt"] = current_time_ms() + delay_ms;
				atomic_write(processing_path, envelope.dump(2));
			}
			catch (...)
			{
			}
		}

		auto [moved, move_error] = move_file(processing_path, delayed_path);
		if (!moved)
		{
			return { false, move_error };
		}

		// Write delayed meta
		DelayedMeta delayed_meta;
		delayed_meta.message_key = message_key;
		delayed_meta.queue = queue;
		delayed_meta.available_at_ms = current_time_ms() + delay_ms;
		delayed_meta.attempt = meta.attempt;
		write_delayed_meta(message_key, delayed_meta);
	}

	// Delete lease meta
	delete_lease_meta(message_key);

	return { true, std::nullopt };
}

auto FileSystemAdapter::move_to_dlq(const std::string& message_key, const std::string& reason) -> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto [meta_opt, meta_error] = read_lease_meta(message_key);
	if (!meta_opt.has_value())
	{
		return { false, meta_error };
	}

	auto& meta = meta_opt.value();

	auto parts = message_key.rfind(':');
	if (parts == std::string::npos)
	{
		return { false, "invalid message key format" };
	}
	auto message_id = message_key.substr(parts + 1);
	auto filename = std::format("{}.json", message_id);

	auto processing_path = build_queue_path(meta.queue, fs_config_.processing_dir, filename);
	auto dlq_path = build_queue_path(meta.queue, fs_config_.dlq_dir, filename);

	// Update message with DLQ info
	auto [content, read_error] = read_file(processing_path);
	if (content.has_value())
	{
		try
		{
			json envelope = json::parse(content.value());
			envelope["dlqReason"] = reason;
			envelope["dlqAt"] = current_time_ms();
			atomic_write(processing_path, envelope.dump(2));
		}
		catch (...)
		{
		}
	}

	auto [moved, move_error] = move_file(processing_path, dlq_path);
	if (!moved)
	{
		return { false, move_error };
	}

	delete_lease_meta(message_key);

	return { true, std::nullopt };
}

auto FileSystemAdapter::atomic_write(const std::string& target_path, const std::string& content)
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

auto FileSystemAdapter::read_file(const std::string& file_path)
	-> std::tuple<std::optional<std::string>, std::optional<std::string>>
{
	std::ifstream file(file_path);
	if (!file.is_open())
	{
		return { std::nullopt, std::format("cannot open file: {}", file_path) };
	}

	std::string content((std::istreambuf_iterator<char>(file)),
		std::istreambuf_iterator<char>());
	file.close();

	return { content, std::nullopt };
}

auto FileSystemAdapter::move_file(const std::string& src, const std::string& dest)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::error_code ec;
	std::filesystem::rename(src, dest, ec);
	if (ec)
	{
		return { false, std::format("move failed: {}", ec.message()) };
	}
	return { true, std::nullopt };
}

auto FileSystemAdapter::delete_file(const std::string& file_path)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::error_code ec;
	std::filesystem::remove(file_path, ec);
	if (ec)
	{
		return { false, std::format("delete failed: {}", ec.message()) };
	}
	return { true, std::nullopt };
}

auto FileSystemAdapter::list_json_files(const std::string& dir_path) -> std::vector<std::string>
{
	std::vector<std::string> files;

	std::error_code ec;
	if (!std::filesystem::exists(dir_path, ec))
	{
		return files;
	}

	for (const auto& entry : std::filesystem::directory_iterator(dir_path, ec))
	{
		if (entry.is_regular_file())
		{
			auto path = entry.path();
			if (path.extension() == ".json")
			{
				files.push_back(path.string());
			}
		}
	}

	// Sort by filename (oldest first)
	std::sort(files.begin(), files.end());

	return files;
}

auto FileSystemAdapter::serialize_envelope(const MessageEnvelope& envelope) -> std::string
{
	json j;
	j["key"] = envelope.key;
	j["messageId"] = envelope.message_id;
	j["queue"] = envelope.queue;
	j["payload"] = envelope.payload_json;
	j["attributes"] = envelope.attributes_json;
	j["priority"] = envelope.priority;
	j["attempt"] = envelope.attempt;
	j["createdAt"] = envelope.created_at_ms;
	j["availableAt"] = envelope.available_at_ms;
	return j.dump(2);
}

auto FileSystemAdapter::deserialize_envelope(const std::string& json_content, const std::string& file_path)
	-> std::tuple<std::optional<MessageEnvelope>, std::optional<std::string>>
{
	try
	{
		json j = json::parse(json_content);

		MessageEnvelope envelope;
		envelope.key = j.value("key", "");
		envelope.message_id = j.value("messageId", "");
		envelope.queue = j.value("queue", "");
		envelope.payload_json = j.value("payload", "{}");
		envelope.attributes_json = j.value("attributes", "{}");
		envelope.priority = j.value("priority", 0);
		envelope.attempt = j.value("attempt", 0);
		envelope.created_at_ms = j.value("createdAt", static_cast<int64_t>(0));
		envelope.available_at_ms = j.value("availableAt", static_cast<int64_t>(0));

		return { envelope, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("envelope parse error: {}", e.what()) };
	}
}

auto FileSystemAdapter::write_lease_meta(const std::string& message_key, const LeaseMeta& meta)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto leases_dir = build_meta_path("leases");
	std::error_code ec;
	if (!std::filesystem::exists(leases_dir, ec))
	{
		std::filesystem::create_directories(leases_dir, ec);
	}

	// Use URL-safe encoding for filename
	std::string safe_key = message_key;
	std::replace(safe_key.begin(), safe_key.end(), ':', '_');
	auto meta_file = build_meta_path(std::format("leases/{}.json", safe_key));

	json j;
	j["messageKey"] = message_key;
	j["consumerId"] = meta.consumer_id;
	j["leaseId"] = meta.lease_id;
	j["leaseUntil"] = meta.lease_until_ms;
	j["attempt"] = meta.attempt;
	j["queue"] = meta.queue;

	return atomic_write(meta_file, j.dump(2));
}

auto FileSystemAdapter::read_lease_meta(const std::string& message_key)
	-> std::tuple<std::optional<LeaseMeta>, std::optional<std::string>>
{
	std::string safe_key = message_key;
	std::replace(safe_key.begin(), safe_key.end(), ':', '_');
	auto meta_file = build_meta_path(std::format("leases/{}.json", safe_key));

	auto [content, read_error] = read_file(meta_file);
	if (!content.has_value())
	{
		return { std::nullopt, read_error };
	}

	try
	{
		json j = json::parse(content.value());

		LeaseMeta meta;
		meta.consumer_id = j.value("consumerId", "");
		meta.lease_id = j.value("leaseId", "");
		meta.lease_until_ms = j.value("leaseUntil", static_cast<int64_t>(0));
		meta.attempt = j.value("attempt", 0);
		meta.queue = j.value("queue", "");

		return { meta, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("lease meta parse error: {}", e.what()) };
	}
}

auto FileSystemAdapter::delete_lease_meta(const std::string& message_key)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::string safe_key = message_key;
	std::replace(safe_key.begin(), safe_key.end(), ':', '_');
	auto meta_file = build_meta_path(std::format("leases/{}.json", safe_key));

	return delete_file(meta_file);
}

auto FileSystemAdapter::write_delayed_meta(const std::string& message_key, const DelayedMeta& meta)
	-> std::tuple<bool, std::optional<std::string>>
{
	auto delayed_dir = build_meta_path("delayed");
	std::error_code ec;
	if (!std::filesystem::exists(delayed_dir, ec))
	{
		std::filesystem::create_directories(delayed_dir, ec);
	}

	std::string safe_key = message_key;
	std::replace(safe_key.begin(), safe_key.end(), ':', '_');
	auto meta_file = build_meta_path(std::format("delayed/{}.json", safe_key));

	json j;
	j["messageKey"] = message_key;
	j["queue"] = meta.queue;
	j["availableAt"] = meta.available_at_ms;
	j["attempt"] = meta.attempt;

	return atomic_write(meta_file, j.dump(2));
}

auto FileSystemAdapter::delete_delayed_meta(const std::string& message_key)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::string safe_key = message_key;
	std::replace(safe_key.begin(), safe_key.end(), ':', '_');
	auto meta_file = build_meta_path(std::format("delayed/{}.json", safe_key));

	return delete_file(meta_file);
}

auto FileSystemAdapter::current_time_ms(void) -> int64_t
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
}

auto FileSystemAdapter::generate_uuid(void) -> std::string
{
	return Utilities::Generator::guid();
}

auto FileSystemAdapter::extract_queue_from_key(const std::string& message_key) -> std::string
{
	// Format: msg:{queue}:{message_id}
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

auto FileSystemAdapter::list_dlq_messages(const std::string& queue, int32_t limit)
	-> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	std::vector<DlqMessageInfo> dlq_list;

	if (!is_open_)
	{
		return { dlq_list, "adapter not open" };
	}

	auto dlq_dir = build_queue_path(queue, fs_config_.dlq_dir);
	auto files = list_json_files(dlq_dir);

	int32_t count = 0;
	for (const auto& file_path : files)
	{
		if (count >= limit)
		{
			break;
		}

		auto [content, read_error] = read_file(file_path);
		if (!content.has_value())
		{
			continue;
		}

		try
		{
			json j = json::parse(content.value());

			DlqMessageInfo info;
			info.message_key = j.value("key", "");
			info.queue = j.value("queue", queue);
			info.reason = j.value("dlqReason", "");
			info.dlq_at_ms = j.value("dlqAt", static_cast<int64_t>(0));
			info.attempt = j.value("attempt", 0);

			dlq_list.push_back(info);
			count++;
		}
		catch (...)
		{
			continue;
		}
	}

	return { dlq_list, std::nullopt };
}

auto FileSystemAdapter::reprocess_dlq_message(const std::string& message_key)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::lock_guard<std::mutex> lock(mutex_);

	if (!is_open_)
	{
		return { false, "adapter not open" };
	}

	auto queue = extract_queue_from_key(message_key);
	if (queue.empty())
	{
		return { false, "invalid message key format" };
	}

	auto parts = message_key.rfind(':');
	if (parts == std::string::npos)
	{
		return { false, "invalid message key format" };
	}
	auto message_id = message_key.substr(parts + 1);
	auto filename = std::format("{}.json", message_id);

	auto dlq_path = build_queue_path(queue, fs_config_.dlq_dir, filename);
	auto inbox_path = build_queue_path(queue, fs_config_.inbox_dir, filename);

	// Read and update message
	auto [content, read_error] = read_file(dlq_path);
	if (!content.has_value())
	{
		return { false, std::format("DLQ message not found: {}", read_error.value_or("unknown")) };
	}

	try
	{
		json envelope = json::parse(content.value());
		envelope.erase("dlqReason");
		envelope.erase("dlqAt");
		envelope["attempt"] = 0;
		envelope["availableAt"] = current_time_ms();

		// Write updated content
		auto [write_ok, write_error] = atomic_write(dlq_path, envelope.dump(2));
		if (!write_ok)
		{
			return { false, write_error };
		}
	}
	catch (const json::exception& e)
	{
		return { false, std::format("failed to parse DLQ message: {}", e.what()) };
	}

	// Move from DLQ to inbox
	auto [moved, move_error] = move_file(dlq_path, inbox_path);
	if (!moved)
	{
		return { false, move_error };
	}

	return { true, std::nullopt };
}
