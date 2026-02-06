#pragma once

#include "BackendAdapter.h"
#include "Logger.h"

#include <filesystem>
#include <format>
#include <random>
#include <string>

// RAII TempDir: creates a unique temporary directory, removes on destruction
class TempDir
{
public:
	TempDir(const std::string& prefix = "yirangmq_test_")
	{
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

		path_ = std::filesystem::temp_directory_path()
			/ std::format("{}{}_{}", prefix, std::to_string(dist(gen)), std::to_string(dist(gen)));

		std::filesystem::create_directories(path_);
	}

	~TempDir()
	{
		std::error_code ec;
		std::filesystem::remove_all(path_, ec);
	}

	TempDir(const TempDir&) = delete;
	TempDir& operator=(const TempDir&) = delete;

	auto path() const -> std::string { return path_.string(); }
	auto path_obj() const -> std::filesystem::path { return path_; }

private:
	std::filesystem::path path_;
};

// Helper: create a MessageEnvelope for testing
inline auto make_envelope(
	const std::string& queue = "test-queue",
	const std::string& payload = R"({"data":"test"})",
	int32_t priority = 0,
	const std::string& target_consumer_id = ""
) -> MessageEnvelope
{
	static int counter = 0;
	counter++;

	MessageEnvelope envelope;
	envelope.message_id = std::format("msg-id-{}", counter);
	envelope.key = std::format("msg:{}:{}", queue, envelope.message_id);
	envelope.queue = queue;
	envelope.payload_json = payload;
	envelope.attributes_json = "{}";
	envelope.priority = priority;
	envelope.attempt = 0;
	envelope.created_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
	envelope.available_at_ms = envelope.created_at_ms;
	envelope.target_consumer_id = target_consumer_id;

	return envelope;
}

// Helper: create a SQLite BackendConfig pointing to a temp directory
inline auto make_sqlite_config(const std::string& temp_dir) -> BackendConfig
{
	BackendConfig config;
	config.type = BackendType::SQLite;
	config.sqlite.db_path = temp_dir + "/test.db";
	config.sqlite.kv_table = "kv";
	config.sqlite.message_index_table = "msg_index";
	config.sqlite.busy_timeout_ms = 5000;
	config.sqlite.journal_mode = "WAL";
	config.sqlite.synchronous = "NORMAL";
	return config;
}

// Helper: create a FileSystem BackendConfig pointing to a temp directory
inline auto make_fs_config(const std::string& temp_dir) -> BackendConfig
{
	BackendConfig config;
	config.type = BackendType::FileSystem;
	config.filesystem.root = temp_dir + "/fs";
	config.filesystem.inbox_dir = "inbox";
	config.filesystem.processing_dir = "processing";
	config.filesystem.archive_dir = "archive";
	config.filesystem.dlq_dir = "dlq";
	config.filesystem.meta_dir = "meta";
	return config;
}

// Helper: create a Hybrid BackendConfig pointing to a temp directory
inline auto make_hybrid_config(const std::string& temp_dir) -> BackendConfig
{
	BackendConfig config;
	config.type = BackendType::Hybrid;
	config.sqlite.db_path = temp_dir + "/hybrid.db";
	config.sqlite.kv_table = "kv";
	config.sqlite.message_index_table = "msg_index";
	config.sqlite.busy_timeout_ms = 5000;
	config.sqlite.journal_mode = "WAL";
	config.sqlite.synchronous = "NORMAL";
	config.filesystem.root = temp_dir + "/payload";
	config.filesystem.inbox_dir = "inbox";
	config.filesystem.processing_dir = "processing";
	config.filesystem.archive_dir = "archive";
	config.filesystem.dlq_dir = "dlq";
	config.filesystem.meta_dir = "meta";
	return config;
}

// Helper: initialize logger for tests (silent mode)
inline auto init_test_logger() -> void
{
	static bool initialized = false;
	if (!initialized)
	{
		Utilities::Logger::handle().console_mode(Utilities::LogTypes::None);
		Utilities::Logger::handle().file_mode(Utilities::LogTypes::None);
		initialized = true;
	}
}
