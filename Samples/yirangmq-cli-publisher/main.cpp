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
Yi-Rang MQ Publisher Client (yirangmq-cli-publisher)

Usage: yirangmq-cli-publisher [command] [options]

Commands:
  (default)    Publish a message (when no command specified)
  publish      Publish a message to a queue
  status       Get queue status
  health       Check server health
  metrics      Get server metrics
  help         Show this help message

Global Options:
  --ipc-root <path>     IPC root directory (default: ./ipc)
  --client-id <id>      Client ID (default: auto-generated)
  --timeout <ms>        Request timeout in milliseconds (default: 30000)

Publish Options:
  --queue <name>        Queue name (required, or set in config)
  --message <json>      Message payload as JSON string (required)
  --priority <n>        Message priority (default: from config or 0)
  --delay <ms>          Delay before message becomes available (default: 0)
  --target <id>         Target consumer ID (default: from config or any)

Status Options:
  --queue <name>        Queue name (required, or set in config)

Configuration File (publisher_configuration.json):
  {
    "ipc": {
      "root": "./ipc",
      "requestsDir": "requests",
      "responsesDir": "responses",
      "timeoutMs": 30000
    },
    "publisher": {
      "queue": "telemetry",
      "target": "",
      "priority": 0
    },
    "logging": {
      "writeConsole": 3,
      "writeFile": 0,
      "logRoot": "./logs"
    },
    "clientId": "publisher-01"
  }

Examples:
  yirangmq-cli-publisher --message '{"temp":36.5}'
  yirangmq-cli-publisher --message '{"temp":36.5}' --target worker-01
  yirangmq-cli-publisher status --queue telemetry
  yirangmq-cli-publisher health
  yirangmq-cli-publisher metrics
)");
}

auto cmd_publish(ArgumentParser& args, Configurations& config) -> int
{
	auto message = args.to_string("--message");
	auto delay = args.to_int("--delay");

	std::string queue = config.default_queue();
	std::string target = config.default_target();

	if (queue.empty())
	{
		Logger::handle().write(LogTypes::Error, "--queue is required (or set in config)");
		return 1;
	}

	if (!message.has_value())
	{
		Logger::handle().write(LogTypes::Error, "--message is required");
		return 1;
	}

	json payload;
	payload["queue"] = queue;
	payload["message"] = message.value();
	payload["priority"] = config.default_priority();
	payload["delayMs"] = delay.value_or(0);
	if (!target.empty())
	{
		payload["targetConsumerId"] = target;
	}

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.client_id(),
		"publish",
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
		Logger::handle().write(LogTypes::Information, "Message published successfully");
		if (response.contains("data"))
		{
			Logger::handle().write(LogTypes::Information, response["data"].dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Publish failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_status(ArgumentParser& args, Configurations& config) -> int
{
	std::string queue = config.default_queue();

	if (queue.empty())
	{
		Logger::handle().write(LogTypes::Error, "--queue is required (or set in config)");
		return 1;
	}

	json payload;
	payload["queue"] = queue;

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.client_id(),
		"status",
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
		Logger::handle().write(LogTypes::Information, "Queue Status:");
		if (response.contains("data"))
		{
			Logger::handle().write(LogTypes::Information, response["data"].dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Status failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_health(Configurations& config) -> int
{
	json payload;

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.client_id(),
		"health",
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
		Logger::handle().write(LogTypes::Information, "Server is healthy");
		if (response.contains("data"))
		{
			Logger::handle().write(LogTypes::Information, response["data"].dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Health check failed";
		if (response.contains("error"))
		{
			error_msg += ": " + response["error"].dump();
		}
		Logger::handle().write(LogTypes::Error, error_msg);
		return 1;
	}
}

auto cmd_metrics(Configurations& config) -> int
{
	json payload;

	auto [ok, response] = send_request(
		config.mailbox_config(),
		config.client_id(),
		"metrics",
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
		Logger::handle().write(LogTypes::Information, "Mailbox Metrics:");
		if (response.contains("data"))
		{
			auto data = response["data"];
			if (data.is_string())
			{
				data = json::parse(data.get<std::string>());
			}
			Logger::handle().write(LogTypes::Information, data.dump(2));
		}
		return 0;
	}
	else
	{
		std::string error_msg = "Failed to get metrics";
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
	Logger::handle().start("yirangmq-publisher");

	// Re-parse args for command
	ArgumentParser cmd_args(argc, argv);

	// Determine command: check if first arg is a known command or default to "publish"
	std::string command = "publish";
	if (argc >= 2)
	{
		std::string first_arg = argv[1];
		if (first_arg == "publish" || first_arg == "status" || first_arg == "health" ||
		    first_arg == "metrics" || first_arg == "help" || first_arg == "--help" || first_arg == "-h")
		{
			command = first_arg;
		}
		// Otherwise, assume it's an option like --message and default to publish
	}

	int result = 0;

	if (command == "help" || command == "--help" || command == "-h")
	{
		print_usage();
	}
	else if (command == "publish")
	{
		result = cmd_publish(cmd_args, config);
	}
	else if (command == "status")
	{
		result = cmd_status(cmd_args, config);
	}
	else if (command == "health")
	{
		result = cmd_health(config);
	}
	else if (command == "metrics")
	{
		result = cmd_metrics(config);
	}

	Logger::handle().stop();
	return result;
}
