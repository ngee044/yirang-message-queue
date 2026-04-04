#include "Configurations.h"
#include "FileSystemAdapter.h"
#include "HybridAdapter.h"
#include "Logger.h"
#include "MailboxHandler.h"
#include "QueueManager.h"
#include "SQLiteAdapter.h"

#include <atomic>
#include <csignal>
#include <format>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

using namespace Utilities;

void register_signal(void);
void deregister_signal(void);
void signal_callback(int32_t signum);

auto create_backend_adapter(Configurations& config) -> std::tuple<std::shared_ptr<BackendAdapter>, std::optional<std::string>>;

std::shared_ptr<Configurations> configurations_ = nullptr;
std::shared_ptr<QueueManager> queue_manager_ = nullptr;
std::shared_ptr<MailboxHandler> mailbox_handler_ = nullptr;
std::atomic<bool> shutdown_requested_ = false;

auto main(int argc, char* argv[]) -> int
{
	// Initialize Configurations
	configurations_ = std::make_shared<Configurations>(ArgumentParser(argc, argv));

	// Setup Logger from configurations
	Logger::handle().file_mode(configurations_->write_file());
	Logger::handle().console_mode(configurations_->write_console());
	Logger::handle().log_root(configurations_->log_root());
	Logger::handle().start("MainMQ");

	Logger::handle().write(LogTypes::Information, "Yi-Rang MQ starting...");
	Logger::handle().write(LogTypes::Information, std::format("Backend: {}",
		configurations_->backend_type() == BackendType::SQLite ? "SQLite" :
		configurations_->backend_type() == BackendType::FileSystem ? "FileSystem" : "Hybrid"));
	Logger::handle().write(LogTypes::Information, std::format("Node ID: {}", configurations_->node_id()));

	// Initialize backend adapter using factory
	auto [adapter, factory_error] = create_backend_adapter(*configurations_);
	if (!adapter)
	{
		Logger::handle().write(LogTypes::Error, std::format("Failed to create backend adapter: {}",
			factory_error.value_or("unknown error")));
		Logger::handle().stop();
		Logger::destroy();
		return 1;
	}

	auto [opened, open_error] = adapter->open(configurations_->backend_config());
	if (!opened)
	{
		Logger::handle().write(LogTypes::Error, std::format("Failed to initialize backend: {}",
			open_error.value_or("unknown error")));
		Logger::handle().stop();
		Logger::destroy();
		return 1;
	}

	Logger::handle().write(LogTypes::Information, "Backend initialized successfully");

	// Initialize QueueManager
	QueueManagerConfig mgr_config;
	mgr_config.lease_sweep_interval_ms = configurations_->lease_sweep_interval_ms();
	mgr_config.retry_sweep_interval_ms = 1000; // Default 1s

	queue_manager_ = std::make_shared<QueueManager>(adapter, mgr_config);

	// Register queues with manager
	for (auto& queue_config : configurations_->queues())
	{
		queue_manager_->register_queue(queue_config.name, queue_config.policy);
	}

	// Start QueueManager
	auto [started, start_error] = queue_manager_->start();
	if (!started)
	{
		Logger::handle().write(LogTypes::Error, std::format("Failed to start QueueManager: {}",
			start_error.value_or("unknown error")));
		queue_manager_.reset();
		configurations_.reset();
		Logger::handle().stop();
		Logger::destroy();
		return 1;
	}

	// Initialize and start MailboxHandler
	mailbox_handler_ = std::make_shared<MailboxHandler>(
		adapter,
		queue_manager_,
		configurations_->mailbox_config()
	);

	// Register message schemas from configuration
	for (auto& queue_config : configurations_->queues())
	{
		if (queue_config.message_schema.has_value())
		{
			mailbox_handler_->register_schema(queue_config.name, queue_config.message_schema.value());
		}
	}

	auto [mailbox_started, mailbox_error] = mailbox_handler_->start();
	if (!mailbox_started)
	{
		Logger::handle().write(LogTypes::Error, std::format("Failed to start MailboxHandler: {}",
			mailbox_error.value_or("unknown error")));
		queue_manager_->stop();
		queue_manager_.reset();
		configurations_.reset();
		Logger::handle().stop();
		Logger::destroy();
		return 1;
	}

	Logger::handle().write(LogTypes::Information, std::format("Operation mode: {}",
		configurations_->operation_mode() == OperationMode::MailboxSqlite ? "mailbox_sqlite" :
		configurations_->operation_mode() == OperationMode::MailboxFileSystem ? "mailbox_fs" : "hybrid"));

	// Setup signal handlers
	register_signal();

	Logger::handle().write(LogTypes::Information, "Yi-Rang MQ is running. Press Ctrl+C to stop.");

	// Keep running until shutdown requested
	while (!shutdown_requested_.load())
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	// Stop MailboxHandler gracefully
	if (mailbox_handler_)
	{
		mailbox_handler_->stop();
		mailbox_handler_.reset();
	}

	// Stop QueueManager gracefully
	if (queue_manager_)
	{
		queue_manager_->stop();
		queue_manager_.reset();
	}

	// Cleanup
	configurations_.reset();

	Logger::handle().write(LogTypes::Information, "Yi-Rang MQ stopped.");
	Logger::handle().stop();
	Logger::destroy();

	return 0;
}

void register_signal(void)
{
	signal(SIGINT, signal_callback);
	signal(SIGTERM, signal_callback);
}

void deregister_signal(void)
{
	signal(SIGINT, nullptr);
	signal(SIGTERM, nullptr);
}

void signal_callback(int32_t signum)
{
	deregister_signal();

	// Use atomic flag for async-signal-safe shutdown request
	shutdown_requested_.store(true);
}

auto create_backend_adapter(Configurations& config) -> std::tuple<std::shared_ptr<BackendAdapter>, std::optional<std::string>>
{
	switch (config.backend_type())
	{
	case BackendType::SQLite:
		return { std::make_shared<SQLiteAdapter>(config.sqlite_schema_path()), std::nullopt };

	case BackendType::FileSystem:
		return { std::make_shared<FileSystemAdapter>(), std::nullopt };

	case BackendType::Hybrid:
		return { std::make_shared<HybridAdapter>(config.sqlite_schema_path()), std::nullopt };

	default:
		return { nullptr, "Unknown backend type" };
	}
}
