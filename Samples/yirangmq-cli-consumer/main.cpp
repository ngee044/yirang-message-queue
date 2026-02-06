#include "Configurations.h"
#include "Generator.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <format>
#include <string>
#include <thread>

using json = nlohmann::json;
using namespace Utilities;

auto current_time_ms() -> int64_t
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
}

auto atomic_write(const std::string& target_path, const std::string& content) -> bool
{
	auto temp_path = target_path + ".tmp";

	std::ofstream file(temp_path, std::ios::out | std::ios::trunc);
	if (!file.is_open())
	{
		Logger::handle().write(LogTypes::Error, std::format("Cannot create temp file: {}", temp_path));
		return false;
	}

	file << content;
	file.flush();
	file.close();

	std::error_code ec;
	std::filesystem::rename(temp_path, target_path, ec);
	if (ec)
	{
		std::filesystem::remove(temp_path, ec);
		Logger::handle().write(LogTypes::Error, std::format("Rename failed: {}", ec.message()));
		return false;
	}

	return true;
}

auto send_request(const MailboxConfig& config, const std::string& client_id, const std::string& command, const json& payload, int32_t timeout_ms)
	-> std::tuple<bool, json>
{
	auto request_id = Generator::guid();
	auto now = current_time_ms();
	auto deadline = now + timeout_ms;

	// Build request JSON
	json request;
	request["requestId"] = request_id;
	request["clientId"] = client_id;
	request["command"] = command;
	request["timestampMs"] = now;
	request["deadlineMs"] = deadline;
	request["payload"] = payload;

	// Ensure directories exist
	std::filesystem::path requests_path = config.root;
	requests_path /= config.requests_dir;

	std::filesystem::path responses_path = config.root;
	responses_path /= config.responses_dir;
	responses_path /= client_id;

	std::error_code ec;
	std::filesystem::create_directories(requests_path, ec);
	std::filesystem::create_directories(responses_path, ec);

	// Write request file
	auto request_file = (requests_path / std::format("{}.json", request_id)).string();
	if (!atomic_write(request_file, request.dump(2)))
	{
		return { false, { { "error", "failed to write request" } } };
	}

	// Wait for response
	auto response_file = (responses_path / std::format("{}.json", request_id)).string();
	auto start_time = current_time_ms();

	while (current_time_ms() - start_time < timeout_ms)
	{
		if (std::filesystem::exists(response_file, ec))
		{
			std::ifstream file(response_file);
			if (file.is_open())
			{
				std::string content((std::istreambuf_iterator<char>(file)),
					std::istreambuf_iterator<char>());
				file.close();

				// Delete response file after reading
				std::filesystem::remove(response_file, ec);

				try
				{
					return { true, json::parse(content) };
				}
				catch (const json::exception& e)
				{
					return { false, { { "error", std::format("response parse error: {}", e.what()) } } };
				}
			}
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	return { false, { { "error", "timeout waiting for response" } } };
}

auto print_usage() -> void
{
	Logger::handle().write(LogTypes::Information, R"(
Yi-Rang MQ Consumer Client (yirangmq-cli-consumer)

Usage: yirangmq-cli-consumer [command] [options]

Commands:
  (default)    Consume next message (when no command specified)
  consume      Consume next message from a queue
  ack          Acknowledge a message
  nack         Negative acknowledge a message
  extend-lease Extend visibility timeout for a leased message
  list-dlq     List messages in dead letter queue
  reprocess    Reprocess a DLQ message
  help         Show this help message

Global Options:
  --ipc-root <path>     IPC root directory (default: ./ipc)
  --consumer-id <id>    Consumer ID (default: from config or auto-generated)
  --timeout <ms>        Request timeout in milliseconds (default: 30000)

Consume Options:
  --queue <name>        Queue name (required, or set in config)
  --visibility <sec>    Visibility timeout in seconds (default: from config or 30)

Ack/Nack Options:
  --message-key <key>   Message key (required)
  --lease-id <id>       Lease ID
  --reason <text>       Reason for nack
  --requeue             Requeue message on nack (default: false)

Extend Lease Options:
  --message-key <key>   Message key (required)
  --lease-id <id>       Lease ID
  --visibility <sec>    New visibility timeout in seconds (default: 30)

DLQ Options:
  --queue <name>        Queue name (required for list-dlq, or set in config)
  --limit <n>           Maximum messages to list (default: 100)
  --message-key <key>   Message key (required for reprocess)

Configuration File (consumer_configuration.json):
  {
    "ipc": {
      "root": "./ipc",
      "requestsDir": "requests",
      "responsesDir": "responses",
      "timeoutMs": 30000
    },
    "consumer": {
      "queue": "telemetry",
      "consumerId": "worker-01",
      "visibilityTimeoutSec": 30
    },
    "logging": {
      "writeConsole": 3,
      "writeFile": 0,
      "logRoot": "./logs"
    }
  }

Examples:
  yirangmq-cli-consumer                # consume using config defaults
  yirangmq-cli-consumer consume --queue telemetry
  yirangmq-cli-consumer ack --message-key msg:telemetry:abc123
  yirangmq-cli-consumer nack --message-key msg:telemetry:abc123 --reason "error" --requeue
  yirangmq-cli-consumer list-dlq --queue telemetry --limit 50
  yirangmq-cli-consumer extend-lease --message-key msg:telemetry:abc123 --lease-id <id> --visibility 60
  yirangmq-cli-consumer reprocess --message-key msg:telemetry:abc123
)");
}

auto cmd_consume(ArgumentParser& args, Configurations& config) -> int
{
	std::string queue = config.default_queue();

	if (queue.empty())
	{
		Logger::handle().write(LogTypes::Error, "--queue is required (or set in config)");
		return 1;
	}

	json payload;
	payload["queue"] = queue;
	payload["consumerId"] = config.consumer_id();
	payload["visibilityTimeoutSec"] = config.visibility_timeout_sec();

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"consume_next",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		if (response.contains("data"))
		{
			auto& data = response["data"];
			if (data.contains("message") && !data["message"].is_null())
			{
				Logger::handle().write(LogTypes::Information, "Message received:");
				Logger::handle().write(LogTypes::Information, data.dump(2));
			}
			else
			{
				Logger::handle().write(LogTypes::Information, "No messages available");
			}
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Consume failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_ack(ArgumentParser& args, Configurations& config) -> int
{
	auto message_key = args.to_string("--message-key");
	auto lease_id = args.to_string("--lease-id");

	if (!message_key.has_value())
	{
		Logger::handle().write(LogTypes::Error, "--message-key is required");
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();
	if (lease_id.has_value())
	{
		payload["leaseId"] = lease_id.value();
	}
	payload["consumerId"] = config.consumer_id();

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"ack",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		Logger::handle().write(LogTypes::Information, "Message acknowledged");
		return 0;
	}
	else
	{
		std::string error_msg = "Ack failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_nack(ArgumentParser& args, Configurations& config) -> int
{
	auto message_key = args.to_string("--message-key");
	auto lease_id = args.to_string("--lease-id");
	auto reason = args.to_string("--reason");
	auto requeue = args.to_string("--requeue").has_value();

	if (!message_key.has_value())
	{
		Logger::handle().write(LogTypes::Error, "--message-key is required");
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();
	if (lease_id.has_value())
	{
		payload["leaseId"] = lease_id.value();
	}
	payload["consumerId"] = config.consumer_id();
	payload["reason"] = reason.value_or("");
	payload["requeue"] = requeue;

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"nack",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		Logger::handle().write(LogTypes::Information, "Message nacked");
		return 0;
	}
	else
	{
		std::string error_msg = "Nack failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_extend_lease(ArgumentParser& args, Configurations& config) -> int
{
	auto message_key = args.to_string("--message-key");
	auto lease_id = args.to_string("--lease-id");
	auto visibility = args.to_int("--visibility");

	if (!message_key.has_value())
	{
		Logger::handle().write(LogTypes::Error, "--message-key is required");
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();
	if (lease_id.has_value())
	{
		payload["leaseId"] = lease_id.value();
	}
	payload["consumerId"] = config.consumer_id();
	payload["visibilityTimeoutSec"] = visibility.value_or(config.visibility_timeout_sec());

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"extend_lease",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		Logger::handle().write(LogTypes::Information, "Lease extended successfully");
		return 0;
	}
	else
	{
		std::string error_msg = "Extend lease failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_list_dlq(ArgumentParser& args, Configurations& config) -> int
{
	auto limit = args.to_int("--limit");

	std::string queue = config.default_queue();

	if (queue.empty())
	{
		Logger::handle().write(LogTypes::Error, "--queue is required (or set in config)");
		return 1;
	}

	json payload;
	payload["queue"] = queue;
	payload["limit"] = limit.value_or(100);

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"list_dlq",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		Logger::handle().write(LogTypes::Information, "DLQ Messages:");
		if (response.contains("data"))
		{
			Logger::handle().write(LogTypes::Information, response["data"].dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "List DLQ failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_reprocess(ArgumentParser& args, Configurations& config) -> int
{
	auto message_key = args.to_string("--message-key");

	if (!message_key.has_value())
	{
		Logger::handle().write(LogTypes::Error, "--message-key is required");
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.consumer_id(),
		"reprocess_dlq",
		payload,
		config.timeout_ms()
	);

	if (!ok)
	{
		Logger::handle().write(LogTypes::Error, std::format("Request failed: {}", response.dump(2)));
		return 1;
	}

	if (response.value("ok", false))
	{
		Logger::handle().write(LogTypes::Information, "Message reprocessed successfully");
		if (response.contains("data"))
		{
			Logger::handle().write(LogTypes::Information, response["data"].dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Reprocess failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto main(int argc, char* argv[]) -> int
{
	ArgumentParser args(argc, argv);

	// Load configuration first
	Configurations config(std::move(args));

	// Initialize logger
	Logger::handle().file_mode(config.write_file());
	Logger::handle().console_mode(config.write_console());
	Logger::handle().log_root(config.log_root());
	Logger::handle().start("yirangmq-consumer");

	// Re-parse args for command
	ArgumentParser cmd_args(argc, argv);

	// Get command (default to "consume" if not specified)
	std::string command = (argc >= 2) ? argv[1] : "consume";
	int result = 0;

	if (command == "help" || command == "--help" || command == "-h")
	{
		print_usage();
	}
	else if (command == "consume")
	{
		result = cmd_consume(cmd_args, config);
	}
	else if (command == "ack")
	{
		result = cmd_ack(cmd_args, config);
	}
	else if (command == "nack")
	{
		result = cmd_nack(cmd_args, config);
	}
	else if (command == "extend-lease" || command == "extend" || command == "extendlease")
	{
		result = cmd_extend_lease(cmd_args, config);
	}
	else if (command == "list-dlq" || command == "listdlq" || command == "dlq")
	{
		result = cmd_list_dlq(cmd_args, config);
	}
	else if (command == "reprocess" || command == "reprocess-dlq")
	{
		result = cmd_reprocess(cmd_args, config);
	}
	else
	{
		Logger::handle().write(LogTypes::Error, std::format("Unknown command: {}", command));
		print_usage();
		result = 1;
	}

	Logger::handle().stop();
	return result;
}
