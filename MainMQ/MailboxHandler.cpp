#include "MailboxHandler.h"

#include "File.h"
#include "Folder.h"
#include "Generator.h"
#include "Job.h"
#include "Logger.h"
#include "QueueManager.h"
#include "ThreadWorker.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>
#include <thread>

using json = nlohmann::json;

MailboxHandler::MailboxHandler(
	std::shared_ptr<BackendAdapter> backend,
	std::shared_ptr<QueueManager> queue_manager,
	const MailboxConfig& config
)
	: running_(false)
	, config_(config)
	, backend_(backend)
	, queue_manager_(queue_manager)
	, use_folder_watcher_(config.use_folder_watcher)
{
	thread_pool_ = std::make_shared<Thread::ThreadPool>("MailboxHandler");
}

MailboxHandler::~MailboxHandler(void)
{
	stop();
}

auto MailboxHandler::register_schema(const std::string& queue, const MessageSchema& schema) -> void
{
	auto resolved_schema = schema;
	validator_.resolve_custom_validators(resolved_schema);
	validator_.register_schema(queue, resolved_schema);
	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("Registered message schema '{}' for queue '{}'", schema.name, queue)
	);
}

auto MailboxHandler::unregister_schema(const std::string& queue) -> void
{
	validator_.unregister_schema(queue);
}

auto MailboxHandler::register_custom_validator(const std::string& name, std::function<bool(const std::string&)> validator) -> void
{
	validator_.register_custom_validator(name, std::move(validator));
}

auto MailboxHandler::start(void) -> std::tuple<bool, std::optional<std::string>>
{
	if (running_.load())
	{
		return { false, "already running" };
	}

	auto [dirs_ok, dirs_error] = ensure_directories();
	if (!dirs_ok)
	{
		return { false, dirs_error };
	}

	// Add ThreadWorkers for LongTerm jobs
	auto request_worker = std::make_shared<Thread::ThreadWorker>(
		std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
		"MailboxRequestWorker"
	);
	thread_pool_->push(request_worker);

	auto cleanup_worker = std::make_shared<Thread::ThreadWorker>(
		std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
		"MailboxCleanupWorker"
	);
	thread_pool_->push(cleanup_worker);

	auto [started, start_error] = thread_pool_->start();
	if (!started)
	{
		return { false, start_error };
	}

	// Initialize metrics
	{
		std::lock_guard<std::mutex> lock(metrics_mutex_);
		metrics_ = MailboxMetrics{};
		metrics_.start_time_ms = current_time_ms();
	}

	running_.store(true);

	// Setup FolderWatcher if enabled
	if (use_folder_watcher_)
	{
		auto& watcher = Utilities::FolderWatcher::handle();
		watcher.set_callback([this](const std::string& dir, const std::string& filename, efsw::Action action, const std::string& old_filename)
		{
			on_file_changed(dir, filename, action, old_filename);
		});

		auto request_dir = build_path(config_.requests_dir);
		watcher.start({ { request_dir, false } });

		Utilities::Logger::handle().write(
			Utilities::LogTypes::Information,
			std::format("FolderWatcher started on: {}", request_dir)
		);
	}

	// Launch request processing worker
	auto request_job = std::make_shared<Thread::Job>(
		Thread::JobPriorities::LongTerm,
		[this]() -> std::tuple<bool, std::optional<std::string>>
		{
			request_processing_worker();
			return { true, std::nullopt };
		},
		"MailboxRequestWorker"
	);
	thread_pool_->push(request_job);

	// Launch stale cleanup worker
	auto cleanup_job = std::make_shared<Thread::Job>(
		Thread::JobPriorities::LongTerm,
		[this]() -> std::tuple<bool, std::optional<std::string>>
		{
			stale_cleanup_worker();
			return { true, std::nullopt };
		},
		"MailboxCleanupWorker"
	);
	thread_pool_->push(cleanup_job);

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		std::format("MailboxHandler started (root: {}, mode: {})", config_.root, use_folder_watcher_ ? "event-driven" : "polling")
	);

	return { true, std::nullopt };
}

auto MailboxHandler::stop(void) -> void
{
	if (!running_.load())
	{
		return;
	}

	running_.store(false);

	// Notify waiting threads
	pending_cv_.notify_all();

	// Stop and destroy FolderWatcher
	if (use_folder_watcher_)
	{
		Utilities::FolderWatcher::handle().stop();
		Utilities::FolderWatcher::destroy();
	}

	thread_pool_->stop(true);

	Utilities::Logger::handle().write(
		Utilities::LogTypes::Information,
		"MailboxHandler stopped"
	);
}

auto MailboxHandler::ensure_directories(void) -> std::tuple<bool, std::optional<std::string>>
{
	std::vector<std::string> dirs = {
		build_path(config_.requests_dir),
		build_path(config_.processing_dir),
		build_path(config_.responses_dir),
		build_path(config_.dead_dir)
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

auto MailboxHandler::build_path(const std::string& sub_dir, const std::string& filename) -> std::string
{
	std::filesystem::path path = config_.root;
	path /= sub_dir;
	if (!filename.empty())
	{
		path /= filename;
	}
	return path.string();
}

auto MailboxHandler::build_response_path(const std::string& client_id, const std::string& filename) -> std::string
{
	std::filesystem::path path = config_.root;
	path /= config_.responses_dir;
	path /= client_id;

	std::error_code ec;
	if (!std::filesystem::exists(path, ec))
	{
		std::filesystem::create_directories(path, ec);
	}

	if (!filename.empty())
	{
		path /= filename;
	}
	return path.string();
}

auto MailboxHandler::on_file_changed(const std::string& dir, const std::string& filename, efsw::Action action, const std::string& old_filename) -> void
{
	// Only process new JSON files
	if (action != efsw::Action::Add)
	{
		return;
	}

	if (filename.size() < 5 || filename.substr(filename.size() - 5) != ".json")
	{
		return;
	}

	std::filesystem::path file_path = dir;
	file_path /= filename;

	{
		std::lock_guard<std::mutex> lock(pending_mutex_);
		pending_requests_.push(file_path.string());
	}
	pending_cv_.notify_one();
}

auto MailboxHandler::process_pending_requests(void) -> void
{
	std::vector<std::string> requests_to_process;

	{
		std::lock_guard<std::mutex> lock(pending_mutex_);
		while (!pending_requests_.empty())
		{
			requests_to_process.push_back(pending_requests_.front());
			pending_requests_.pop();
		}
	}

	for (const auto& file_path : requests_to_process)
	{
		if (!running_.load())
		{
			break;
		}

		std::lock_guard<std::mutex> lock(processing_mutex_);

		// Check if file still exists
		std::error_code ec;
		if (!std::filesystem::exists(file_path, ec))
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Information,
				std::format("Request file disappeared before processing: {}", file_path)
			);
			continue;
		}

		// Move to processing
		auto [processing_path, move_error] = move_to_processing(file_path);
		if (move_error.has_value())
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Error,
				std::format("Failed to move request to processing: {}", move_error.value())
			);
			continue;
		}

		// Read and parse request
		auto [request_opt, read_error] = read_request_file(processing_path);
		if (!request_opt.has_value())
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Error,
				std::format("Failed to read request: {}", read_error.value_or("unknown"))
			);
			move_to_dead(processing_path, read_error.value_or("parse error"));
			continue;
		}

		auto& request = request_opt.value();

		// Start metrics timing
		auto start_time = record_request_start();

		// Check deadline
		auto now = current_time_ms();
		if (request.deadline_ms > 0 && now > request.deadline_ms)
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Information,
				std::format("Request {} expired (deadline: {}, now: {})",
					request.request_id, request.deadline_ms, now)
			);
			move_to_dead(processing_path, "deadline exceeded");
			record_request_end(request.command, false, MailboxErrorCode::TIMEOUT, start_time);
			continue;
		}

		// Handle request
		auto response = handle_request(request);

		// Record metrics
		record_request_end(request.command, response.ok, response.error_code, start_time);

		// Write response
		auto [write_ok, write_error] = write_response_file(request.client_id, response);
		if (!write_ok)
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Error,
				std::format("Failed to write response for request {}: {}",
					request.request_id, write_error.value_or("unknown"))
			);
		}

		// Delete processed file
		delete_processed(processing_path);
	}
}

auto MailboxHandler::request_processing_worker(void) -> void
{
	// Process any existing files on startup
	auto request_dir = build_path(config_.requests_dir);
	auto existing_files = list_files(request_dir);
	for (const auto& file_path : existing_files)
	{
		std::lock_guard<std::mutex> lock(pending_mutex_);
		pending_requests_.push(file_path);
	}

	while (running_.load())
	{
		if (use_folder_watcher_)
		{
			// Event-driven mode: wait for notification or timeout
			std::unique_lock<std::mutex> lock(pending_mutex_);
			pending_cv_.wait_for(lock, std::chrono::milliseconds(config_.poll_interval_ms), [this]()
			{
				return !pending_requests_.empty() || !running_.load();
			});
			lock.unlock();

			if (!running_.load())
			{
				break;
			}

			process_pending_requests();
		}
		else
		{
			// Polling mode (fallback)
			auto files = list_files(request_dir);

			for (const auto& file_path : files)
			{
				if (!running_.load())
				{
					break;
				}

				std::lock_guard<std::mutex> lock(processing_mutex_);

				// Move to processing
				auto [processing_path, move_error] = move_to_processing(file_path);
				if (move_error.has_value())
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to move request to processing: {}", move_error.value())
					);
					continue;
				}

				// Read and parse request
				auto [request_opt, read_error] = read_request_file(processing_path);
				if (!request_opt.has_value())
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to read request: {}", read_error.value_or("unknown"))
					);
					move_to_dead(processing_path, read_error.value_or("parse error"));
					continue;
				}

				auto& request = request_opt.value();

				// Start metrics timing
				auto start_time = record_request_start();

				// Check deadline
				auto now = current_time_ms();
				if (request.deadline_ms > 0 && now > request.deadline_ms)
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Information,
						std::format("Request {} expired (deadline: {}, now: {})",
							request.request_id, request.deadline_ms, now)
					);
					move_to_dead(processing_path, "deadline exceeded");
					record_request_end(request.command, false, MailboxErrorCode::TIMEOUT, start_time);
					continue;
				}

				// Handle request
				auto response = handle_request(request);

				// Record metrics
				record_request_end(request.command, response.ok, response.error_code, start_time);

				// Write response
				auto [write_ok, write_error] = write_response_file(request.client_id, response);
				if (!write_ok)
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to write response for request {}: {}",
							request.request_id, write_error.value_or("unknown"))
					);
				}

				// Delete processed file
				delete_processed(processing_path);
			}

			std::this_thread::sleep_for(std::chrono::milliseconds(config_.poll_interval_ms));
		}
	}
}

auto MailboxHandler::stale_cleanup_worker(void) -> void
{
	while (running_.load())
	{
		cleanup_stale_requests();
		cleanup_stale_responses();

		// Use condition variable so stop() can wake us immediately
		std::unique_lock<std::mutex> lock(pending_mutex_);
		pending_cv_.wait_for(lock, std::chrono::seconds(10), [this]()
		{
			return !running_.load();
		});
	}
}

auto MailboxHandler::read_request_file(const std::string& file_path)
	-> std::tuple<std::optional<MailboxRequest>, std::optional<std::string>>
{
	std::ifstream file(file_path);
	if (!file.is_open())
	{
		return { std::nullopt, std::format("cannot open file: {}", file_path) };
	}

	std::string content((std::istreambuf_iterator<char>(file)),
		std::istreambuf_iterator<char>());
	file.close();

	return parse_request(content, file_path);
}

auto MailboxHandler::write_response_file(const std::string& client_id, const MailboxResponse& response)
	-> std::tuple<bool, std::optional<std::string>>
{
	json response_json;
	response_json["requestId"] = response.request_id;
	response_json["ok"] = response.ok;

	if (response.ok)
	{
		try
		{
			response_json["data"] = json::parse(response.data_json);
		}
		catch (...)
		{
			response_json["data"] = json::object();
		}
	}
	else
	{
		response_json["error"] = {
			{ "code", response.error_code },
			{ "message", response.error_message }
		};
	}

	auto filename = std::format("{}.json", response.request_id);
	auto target_path = build_response_path(client_id, filename);

	return atomic_write(target_path, response_json.dump(2));
}

auto MailboxHandler::atomic_write(const std::string& target_path, const std::string& content)
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

auto MailboxHandler::move_to_processing(const std::string& request_file)
	-> std::tuple<std::string, std::optional<std::string>>
{
	std::filesystem::path src_path(request_file);
	auto filename = src_path.filename().string();
	auto dest_path = build_path(config_.processing_dir, filename);

	std::error_code ec;
	std::filesystem::rename(request_file, dest_path, ec);
	if (ec)
	{
		return { "", std::format("move failed: {}", ec.message()) };
	}

	return { dest_path, std::nullopt };
}

auto MailboxHandler::move_to_dead(const std::string& processing_file, const std::string& reason)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::filesystem::path src_path(processing_file);
	auto filename = src_path.filename().string();
	auto dest_path = build_path(config_.dead_dir, filename);

	std::error_code ec;
	std::filesystem::rename(processing_file, dest_path, ec);
	if (ec)
	{
		return { false, std::format("move to dead failed: {}", ec.message()) };
	}

	// Write reason file
	auto reason_path = dest_path + ".reason";
	json reason_json;
	reason_json["reason"] = reason;
	reason_json["movedAt"] = current_time_ms();

	std::ofstream reason_file(reason_path);
	if (reason_file.is_open())
	{
		reason_file << reason_json.dump(2);
		reason_file.close();
	}

	return { true, std::nullopt };
}

auto MailboxHandler::delete_processed(const std::string& processing_file)
	-> std::tuple<bool, std::optional<std::string>>
{
	std::error_code ec;
	std::filesystem::remove(processing_file, ec);
	if (ec)
	{
		return { false, std::format("delete failed: {}", ec.message()) };
	}
	return { true, std::nullopt };
}

auto MailboxHandler::parse_request(const std::string& json_content, const std::string& file_path)
	-> std::tuple<std::optional<MailboxRequest>, std::optional<std::string>>
{
	try
	{
		json req_json = json::parse(json_content);

		MailboxRequest request;
		request.file_path = file_path;

		if (!req_json.contains("requestId") || !req_json["requestId"].is_string())
		{
			return { std::nullopt, "missing requestId" };
		}
		request.request_id = req_json["requestId"].get<std::string>();

		if (!req_json.contains("clientId") || !req_json["clientId"].is_string())
		{
			return { std::nullopt, "missing clientId" };
		}
		request.client_id = req_json["clientId"].get<std::string>();

		if (!req_json.contains("command") || !req_json["command"].is_string())
		{
			return { std::nullopt, "missing command" };
		}
		request.command = parse_command(req_json["command"].get<std::string>());

		request.timestamp_ms = req_json.value("timestampMs", static_cast<int64_t>(0));
		request.deadline_ms = req_json.value("deadlineMs", static_cast<int64_t>(0));

		if (req_json.contains("payload") && req_json["payload"].is_object())
		{
			request.payload_json = req_json["payload"].dump();
		}
		else
		{
			request.payload_json = "{}";
		}

		return { request, std::nullopt };
	}
	catch (const json::exception& e)
	{
		return { std::nullopt, std::format("JSON parse error: {}", e.what()) };
	}
}

auto MailboxHandler::parse_command(const std::string& command_str) -> MailboxCommand
{
	std::string cmd = command_str;
	std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);

	if (cmd == "publish")
	{
		return MailboxCommand::Publish;
	}
	else if (cmd == "consume_next" || cmd == "consumenext" || cmd == "consume")
	{
		return MailboxCommand::ConsumeNext;
	}
	else if (cmd == "ack")
	{
		return MailboxCommand::Ack;
	}
	else if (cmd == "nack")
	{
		return MailboxCommand::Nack;
	}
	else if (cmd == "extend_lease" || cmd == "extendlease" || cmd == "extend")
	{
		return MailboxCommand::ExtendLease;
	}
	else if (cmd == "status")
	{
		return MailboxCommand::Status;
	}
	else if (cmd == "health")
	{
		return MailboxCommand::Health;
	}
	else if (cmd == "metrics")
	{
		return MailboxCommand::Metrics;
	}
	else if (cmd == "list_dlq" || cmd == "listdlq" || cmd == "dlq")
	{
		return MailboxCommand::ListDlq;
	}
	else if (cmd == "reprocess_dlq" || cmd == "reprocessdlq" || cmd == "reprocess")
	{
		return MailboxCommand::ReprocessDlq;
	}

	return MailboxCommand::Unknown;
}

auto MailboxHandler::handle_request(const MailboxRequest& request) -> MailboxResponse
{
	switch (request.command)
	{
	case MailboxCommand::Publish:
		return handle_publish(request);
	case MailboxCommand::ConsumeNext:
		return handle_consume_next(request);
	case MailboxCommand::Ack:
		return handle_ack(request);
	case MailboxCommand::Nack:
		return handle_nack(request);
	case MailboxCommand::ExtendLease:
		return handle_extend_lease(request);
	case MailboxCommand::Status:
		return handle_status(request);
	case MailboxCommand::Health:
		return handle_health(request);
	case MailboxCommand::Metrics:
		return handle_metrics(request);
	case MailboxCommand::ListDlq:
		return handle_list_dlq(request);
	case MailboxCommand::ReprocessDlq:
		return handle_reprocess_dlq(request);
	default:
		return build_error_response(request.request_id, MailboxErrorCode::UNKNOWN_COMMAND, "unknown command");
	}
}

auto MailboxHandler::handle_publish(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("queue") || !payload["queue"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing queue in payload");
		}

		MessageEnvelope envelope;
		envelope.message_id = generate_uuid();
		envelope.key = std::format("msg:{}:{}", payload["queue"].get<std::string>(), envelope.message_id);
		envelope.queue = payload["queue"].get<std::string>();
		envelope.payload_json = payload.value("message", "{}");
		envelope.attributes_json = payload.value("attributes", "{}");
		envelope.priority = payload.value("priority", 0);
		envelope.created_at_ms = current_time_ms();
		envelope.target_consumer_id = payload.value("targetConsumerId", "");

		int64_t delay_ms = payload.value("delayMs", static_cast<int64_t>(0));
		envelope.available_at_ms = envelope.created_at_ms + delay_ms;

		// Calculate TTL expiration from queue policy
		if (queue_manager_)
		{
			auto policy_opt = queue_manager_->get_policy(envelope.queue);
			if (policy_opt.has_value() && policy_opt->ttl_sec > 0)
			{
				envelope.expired_at_ms = envelope.created_at_ms + (static_cast<int64_t>(policy_opt->ttl_sec) * 1000);
			}
		}

		// Validate message if schema is registered for this queue
		if (validator_.has_schema(envelope.queue))
		{
			auto validation = validator_.validate(envelope);
			if (!validation.valid)
			{
				std::string errors_str;
				for (const auto& err : validation.errors)
				{
					if (!errors_str.empty()) errors_str += "; ";
					errors_str += err;
				}
				return build_error_response(request.request_id, MailboxErrorCode::VALIDATION_FAILED, errors_str);
			}
		}

		auto [ok, error] = backend_->enqueue(envelope);
		if (!ok)
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value_or("enqueue failed"));
		}

		json result;
		result["messageId"] = envelope.message_id;
		result["messageKey"] = envelope.key;

		return build_success_response(request.request_id, result.dump());
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_consume_next(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("queue") || !payload["queue"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing queue in payload");
		}

		std::string queue = payload["queue"].get<std::string>();
		std::string consumer_id = payload.value("consumerId", request.client_id);
		int32_t visibility_timeout = payload.value("visibilityTimeoutSec", 30);

		auto result = backend_->lease_next(queue, consumer_id, visibility_timeout);

		if (!result.leased)
		{
			if (result.error.has_value())
			{
				return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, result.error.value());
			}

			json empty_result;
			empty_result["message"] = nullptr;
			return build_success_response(request.request_id, empty_result.dump());
		}

		json response_data;
		if (result.message.has_value())
		{
			auto& msg = result.message.value();
			response_data["message"] = {
				{ "messageId", msg.message_id },
				{ "messageKey", msg.key },
				{ "queue", msg.queue },
				{ "payload", msg.payload_json },
				{ "attributes", msg.attributes_json },
				{ "priority", msg.priority },
				{ "attempt", msg.attempt },
				{ "createdAt", msg.created_at_ms }
			};
		}

		if (result.lease.has_value())
		{
			auto& lease = result.lease.value();
			response_data["lease"] = {
				{ "leaseId", lease.lease_id },
				{ "messageKey", lease.message_key },
				{ "consumerId", lease.consumer_id },
				{ "leaseUntil", lease.lease_until_ms }
			};
		}

		return build_success_response(request.request_id, response_data.dump());
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_ack(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("messageKey") || !payload["messageKey"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing messageKey in payload");
		}

		LeaseToken lease;
		lease.lease_id = payload.value("leaseId", "");
		lease.message_key = payload["messageKey"].get<std::string>();
		lease.consumer_id = payload.value("consumerId", request.client_id);

		auto [ok, error] = backend_->ack(lease);
		if (!ok)
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value_or("ack failed"));
		}

		return build_success_response(request.request_id);
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_nack(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("messageKey") || !payload["messageKey"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing messageKey in payload");
		}

		LeaseToken lease;
		lease.lease_id = payload.value("leaseId", "");
		lease.message_key = payload["messageKey"].get<std::string>();
		lease.consumer_id = payload.value("consumerId", request.client_id);

		std::string reason = payload.value("reason", "");
		bool requeue = payload.value("requeue", false);

		auto [ok, error] = backend_->nack(lease, reason, requeue);
		if (!ok)
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value_or("nack failed"));
		}

		return build_success_response(request.request_id);
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_extend_lease(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("messageKey") || !payload["messageKey"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing messageKey in payload");
		}

		LeaseToken lease;
		lease.lease_id = payload.value("leaseId", "");
		lease.message_key = payload["messageKey"].get<std::string>();
		lease.consumer_id = payload.value("consumerId", request.client_id);

		int32_t visibility_timeout = payload.value("visibilityTimeoutSec", 30);

		auto [ok, error] = backend_->extend_lease(lease, visibility_timeout);
		if (!ok)
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value_or("extend_lease failed"));
		}

		return build_success_response(request.request_id);
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_status(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("queue") || !payload["queue"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing queue in payload");
		}

		std::string queue = payload["queue"].get<std::string>();

		auto [metrics_data, error] = backend_->metrics(queue);
		if (error.has_value())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value());
		}

		json result;
		result["queue"] = queue;
		result["metrics"] = {
			{ "ready", metrics_data.ready },
			{ "inflight", metrics_data.inflight },
			{ "delayed", metrics_data.delayed },
			{ "dlq", metrics_data.dlq }
		};

		// Add policy if available
		if (queue_manager_)
		{
			auto policy_opt = queue_manager_->get_policy(queue);
			if (policy_opt.has_value())
			{
				auto& p = policy_opt.value();
				result["policy"] = {
					{ "visibilityTimeoutSec", p.visibility_timeout_sec },
					{ "retry", {
						{ "limit", p.retry.limit },
						{ "backoff", p.retry.backoff },
						{ "initialDelaySec", p.retry.initial_delay_sec },
						{ "maxDelaySec", p.retry.max_delay_sec }
					}},
					{ "dlq", {
						{ "enabled", p.dlq.enabled },
						{ "queue", p.dlq.queue },
						{ "retentionDays", p.dlq.retention_days }
					}}
				};
			}
		}

		return build_success_response(request.request_id, result.dump());
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_health(const MailboxRequest& request) -> MailboxResponse
{
	json result;
	result["status"] = "ok";
	result["timestamp"] = current_time_ms();

	return build_success_response(request.request_id, result.dump());
}

auto MailboxHandler::build_success_response(const std::string& request_id, const std::string& data_json) -> MailboxResponse
{
	MailboxResponse response;
	response.request_id = request_id;
	response.ok = true;
	response.data_json = data_json;
	return response;
}

auto MailboxHandler::build_error_response(const std::string& request_id, const std::string& error_code, const std::string& error_message) -> MailboxResponse
{
	MailboxResponse response;
	response.request_id = request_id;
	response.ok = false;
	response.error_code = error_code;
	response.error_message = error_message;
	return response;
}

auto MailboxHandler::cleanup_stale_requests(void) -> void
{
	auto now = current_time_ms();
	auto processing_dir = build_path(config_.processing_dir);
	auto files = list_files(processing_dir);

	for (const auto& file_path : files)
	{
		std::error_code ec;
		auto last_write = std::filesystem::last_write_time(file_path, ec);
		if (ec)
		{
			continue;
		}

		auto file_age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::filesystem::file_time_type::clock::now() - last_write
		).count();

		if (file_age_ms > config_.stale_timeout_ms)
		{
			Utilities::Logger::handle().write(
				Utilities::LogTypes::Information,
				std::format("Moving stale request to dead: {}", file_path)
			);
			move_to_dead(file_path, "stale timeout");
		}
	}
}

auto MailboxHandler::cleanup_stale_responses(void) -> void
{
	auto response_base = build_path(config_.responses_dir);

	std::error_code ec;
	for (const auto& entry : std::filesystem::directory_iterator(response_base, ec))
	{
		if (!entry.is_directory())
		{
			continue;
		}

		auto client_dir = entry.path().string();
		auto files = list_files(client_dir);

		for (const auto& file_path : files)
		{
			std::error_code file_ec;
			auto last_write = std::filesystem::last_write_time(file_path, file_ec);
			if (file_ec)
			{
				continue;
			}

			auto file_age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
				std::filesystem::file_time_type::clock::now() - last_write
			).count();

			// Remove responses older than 5 minutes
			if (file_age_ms > 300000)
			{
				std::filesystem::remove(file_path, file_ec);
			}
		}
	}
}

auto MailboxHandler::current_time_ms(void) -> int64_t
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
}

auto MailboxHandler::generate_uuid(void) -> std::string
{
	return Utilities::Generator::guid();
}

auto MailboxHandler::list_files(const std::string& dir_path) -> std::vector<std::string>
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

auto MailboxHandler::handle_list_dlq(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("queue") || !payload["queue"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing queue in payload");
		}

		std::string queue = payload["queue"].get<std::string>();
		int32_t limit = payload.value("limit", 100);

		auto [dlq_messages, error] = backend_->list_dlq_messages(queue, limit);

		if (error.has_value())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INTERNAL_ERROR, error.value());
		}

		json response_data;
		response_data["queue"] = queue;
		response_data["count"] = dlq_messages.size();
		response_data["messages"] = json::array();

		for (const auto& msg : dlq_messages)
		{
			json msg_obj;
			msg_obj["messageKey"] = msg.message_key;
			msg_obj["queue"] = msg.queue;
			msg_obj["reason"] = msg.reason;
			msg_obj["dlqAt"] = msg.dlq_at_ms;
			msg_obj["attempt"] = msg.attempt;
			response_data["messages"].push_back(msg_obj);
		}

		return build_success_response(request.request_id, response_data.dump());
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_reprocess_dlq(const MailboxRequest& request) -> MailboxResponse
{
	try
	{
		json payload = json::parse(request.payload_json);

		if (!payload.contains("messageKey") || !payload["messageKey"].is_string())
		{
			return build_error_response(request.request_id, MailboxErrorCode::INVALID_REQUEST, "missing messageKey in payload");
		}

		std::string message_key = payload["messageKey"].get<std::string>();

		auto [ok, error] = backend_->reprocess_dlq_message(message_key);

		if (!ok)
		{
			return build_error_response(request.request_id, MailboxErrorCode::DLQ_NOT_FOUND, error.value_or("failed to reprocess DLQ message"));
		}

		json response_data;
		response_data["messageKey"] = message_key;
		response_data["reprocessed"] = true;

		return build_success_response(request.request_id, response_data.dump());
	}
	catch (const json::exception& e)
	{
		return build_error_response(request.request_id, MailboxErrorCode::PARSE_ERROR, e.what());
	}
}

auto MailboxHandler::handle_metrics(const MailboxRequest& request) -> MailboxResponse
{
	std::lock_guard<std::mutex> lock(metrics_mutex_);

	auto now = current_time_ms();
	auto uptime_ms = now - metrics_.start_time_ms;

	json result;
	result["timestamp"] = now;
	result["uptimeMs"] = uptime_ms;

	// Request counters
	result["requests"] = {
		{ "total", metrics_.total_requests },
		{ "success", metrics_.success_count },
		{ "error", metrics_.error_count }
	};

	// Per-command counters
	result["commands"] = {
		{ "publish", metrics_.publish_count },
		{ "consume", metrics_.consume_count },
		{ "ack", metrics_.ack_count },
		{ "nack", metrics_.nack_count },
		{ "status", metrics_.status_count },
		{ "health", metrics_.health_count },
		{ "dlq", metrics_.dlq_count }
	};

	// Error type counters
	result["errors"] = {
		{ "parse", metrics_.parse_errors },
		{ "validation", metrics_.validation_errors },
		{ "timeout", metrics_.timeout_errors },
		{ "internal", metrics_.internal_errors }
	};

	// Timing
	double avg_processing_ms = 0.0;
	if (metrics_.total_requests > 0)
	{
		avg_processing_ms = static_cast<double>(metrics_.total_processing_time_ms) / metrics_.total_requests;
	}

	result["timing"] = {
		{ "totalProcessingMs", metrics_.total_processing_time_ms },
		{ "avgProcessingMs", avg_processing_ms },
		{ "lastRequestMs", metrics_.last_request_time_ms }
	};

	return build_success_response(request.request_id, result.dump());
}

auto MailboxHandler::record_request_start(void) -> int64_t
{
	return current_time_ms();
}

auto MailboxHandler::record_request_end(MailboxCommand command, bool success, const std::string& error_code, int64_t start_time) -> void
{
	std::lock_guard<std::mutex> lock(metrics_mutex_);

	auto now = current_time_ms();
	auto processing_time = now - start_time;

	metrics_.total_requests++;
	metrics_.total_processing_time_ms += processing_time;
	metrics_.last_request_time_ms = now;

	if (success)
	{
		metrics_.success_count++;
	}
	else
	{
		metrics_.error_count++;

		// Categorize error
		if (error_code == MailboxErrorCode::PARSE_ERROR || error_code == MailboxErrorCode::INVALID_REQUEST)
		{
			metrics_.parse_errors++;
		}
		else if (error_code == MailboxErrorCode::VALIDATION_FAILED)
		{
			metrics_.validation_errors++;
		}
		else if (error_code == MailboxErrorCode::TIMEOUT)
		{
			metrics_.timeout_errors++;
		}
		else
		{
			metrics_.internal_errors++;
		}
	}

	// Per-command counters
	switch (command)
	{
	case MailboxCommand::Publish:
		metrics_.publish_count++;
		break;
	case MailboxCommand::ConsumeNext:
		metrics_.consume_count++;
		break;
	case MailboxCommand::Ack:
		metrics_.ack_count++;
		break;
	case MailboxCommand::Nack:
		metrics_.nack_count++;
		break;
	case MailboxCommand::Status:
		metrics_.status_count++;
		break;
	case MailboxCommand::Health:
		metrics_.health_count++;
		break;
	case MailboxCommand::ListDlq:
	case MailboxCommand::ReprocessDlq:
		metrics_.dlq_count++;
		break;
	default:
		break;
	}
}
