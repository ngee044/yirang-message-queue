#include "ArgumentParser.h"
#include "Generator.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <format>
#include <iostream>
#include <string>
#include <thread>

using json = nlohmann::json;

struct MailboxConfig
{
	std::string root = "./ipc";
	std::string requests_dir = "requests";
	std::string responses_dir = "responses";
	int32_t timeout_ms = 30000;
};

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
		std::cerr << "Error: cannot create temp file: " << temp_path << std::endl;
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
		std::cerr << "Error: rename failed: " << ec.message() << std::endl;
		return false;
	}

	return true;
}

auto send_request(const MailboxConfig& config, const std::string& client_id, const std::string& command, const json& payload, int32_t timeout_ms)
	-> std::tuple<bool, json>
{
	auto request_id = Utilities::Generator::guid();
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
	std::cout << R"(
Yi-Rang MQ Mailbox Client (yirangmq)

Usage: yirangmq <command> [options]

Commands:
  publish      Publish a message to a queue
  consume      Consume next message from a queue
  ack          Acknowledge a message
  nack         Negative acknowledge a message
  status       Get queue status
  health       Check server health
  list-dlq     List messages in dead letter queue
  reprocess    Reprocess a DLQ message

Global Options:
  --ipc-root <path>     IPC root directory (default: ./ipc)
  --client-id <id>      Client ID (default: auto-generated)
  --timeout <ms>        Request timeout in milliseconds (default: 30000)

Publish Options:
  --queue <name>        Queue name (required)
  --message <json>      Message payload as JSON string (required)
  --priority <n>        Message priority (default: 0)
  --delay <ms>          Delay before message becomes available (default: 0)

Consume Options:
  --queue <name>        Queue name (required)
  --consumer-id <id>    Consumer ID (default: client-id)
  --visibility <sec>    Visibility timeout in seconds (default: 30)

Ack/Nack Options:
  --message-key <key>   Message key (required)
  --lease-id <id>       Lease ID
  --consumer-id <id>    Consumer ID
  --reason <text>       Reason for nack
  --requeue             Requeue message on nack (default: false)

Status Options:
  --queue <name>        Queue name (required)

DLQ Options:
  --queue <name>        Queue name (required for list-dlq)
  --limit <n>           Maximum messages to list (default: 100)
  --message-key <key>   Message key (required for reprocess)

Examples:
  yirangmq publish --queue telemetry --message '{"temp":36.5}'
  yirangmq consume --queue telemetry --consumer-id worker-01
  yirangmq ack --message-key msg:telemetry:abc123
  yirangmq nack --message-key msg:telemetry:abc123 --reason "error" --requeue
  yirangmq status --queue telemetry
  yirangmq health
  yirangmq list-dlq --queue telemetry --limit 50
  yirangmq reprocess --message-key msg:telemetry:abc123
)" << std::endl;
}

auto cmd_publish(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto queue = args.to_string("--queue");
	auto message = args.to_string("--message");
	auto priority = args.to_int("--priority");
	auto delay = args.to_int("--delay");
	auto timeout = args.to_int("--timeout");

	if (!queue.has_value())
	{
		std::cerr << "Error: --queue is required" << std::endl;
		return 1;
	}

	if (!message.has_value())
	{
		std::cerr << "Error: --message is required" << std::endl;
		return 1;
	}

	json payload;
	payload["queue"] = queue.value();
	payload["message"] = message.value();
	payload["priority"] = priority.value_or(0);
	payload["delayMs"] = delay.value_or(0);

	auto [ok, response] = send_request(
		config,
		client_id,
		"publish",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Message published successfully" << std::endl;
		if (response.contains("data"))
		{
			std::cout << response["data"].dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "Publish failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_consume(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto queue = args.to_string("--queue");
	auto consumer_id = args.to_string("--consumer-id");
	auto visibility = args.to_int("--visibility");
	auto timeout = args.to_int("--timeout");

	if (!queue.has_value())
	{
		std::cerr << "Error: --queue is required" << std::endl;
		return 1;
	}

	json payload;
	payload["queue"] = queue.value();
	payload["consumerId"] = consumer_id.value_or(client_id);
	payload["visibilityTimeoutSec"] = visibility.value_or(30);

	auto [ok, response] = send_request(
		config,
		client_id,
		"consume_next",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		if (response.contains("data"))
		{
			auto& data = response["data"];
			if (data.contains("message") && !data["message"].is_null())
			{
				std::cout << "Message received:" << std::endl;
				std::cout << data.dump(2) << std::endl;
			}
			else
			{
				std::cout << "No messages available" << std::endl;
			}
		}
		return 0;
	}
	else
	{
		std::cerr << "Consume failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_ack(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto message_key = args.to_string("--message-key");
	auto lease_id = args.to_string("--lease-id");
	auto consumer_id = args.to_string("--consumer-id");
	auto timeout = args.to_int("--timeout");

	if (!message_key.has_value())
	{
		std::cerr << "Error: --message-key is required" << std::endl;
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();
	if (lease_id.has_value())
	{
		payload["leaseId"] = lease_id.value();
	}
	payload["consumerId"] = consumer_id.value_or(client_id);

	auto [ok, response] = send_request(
		config,
		client_id,
		"ack",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Message acknowledged" << std::endl;
		return 0;
	}
	else
	{
		std::cerr << "Ack failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_nack(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto message_key = args.to_string("--message-key");
	auto lease_id = args.to_string("--lease-id");
	auto consumer_id = args.to_string("--consumer-id");
	auto reason = args.to_string("--reason");
	auto requeue = args.to_string("--requeue").has_value();
	auto timeout = args.to_int("--timeout");

	if (!message_key.has_value())
	{
		std::cerr << "Error: --message-key is required" << std::endl;
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();
	if (lease_id.has_value())
	{
		payload["leaseId"] = lease_id.value();
	}
	payload["consumerId"] = consumer_id.value_or(client_id);
	payload["reason"] = reason.value_or("");
	payload["requeue"] = requeue;

	auto [ok, response] = send_request(
		config,
		client_id,
		"nack",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Message nacked" << std::endl;
		return 0;
	}
	else
	{
		std::cerr << "Nack failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_status(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto queue = args.to_string("--queue");
	auto timeout = args.to_int("--timeout");

	if (!queue.has_value())
	{
		std::cerr << "Error: --queue is required" << std::endl;
		return 1;
	}

	json payload;
	payload["queue"] = queue.value();

	auto [ok, response] = send_request(
		config,
		client_id,
		"status",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Queue Status:" << std::endl;
		if (response.contains("data"))
		{
			std::cout << response["data"].dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "Status failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_health(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto timeout = args.to_int("--timeout");

	json payload;

	auto [ok, response] = send_request(
		config,
		client_id,
		"health",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Server is healthy" << std::endl;
		if (response.contains("data"))
		{
			std::cout << response["data"].dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "Health check failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_metrics(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto timeout = args.to_int("--timeout");

	json payload;

	auto [ok, response] = send_request(
		config,
		client_id,
		"metrics",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Mailbox Metrics:" << std::endl;
		if (response.contains("data"))
		{
			auto data = response["data"];
			if (data.is_string())
			{
				data = json::parse(data.get<std::string>());
			}
			std::cout << data.dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "Failed to get metrics: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_list_dlq(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto queue = args.to_string("--queue");
	auto limit = args.to_int("--limit");
	auto timeout = args.to_int("--timeout");

	if (!queue.has_value())
	{
		std::cerr << "Error: --queue is required" << std::endl;
		return 1;
	}

	json payload;
	payload["queue"] = queue.value();
	payload["limit"] = limit.value_or(100);

	auto [ok, response] = send_request(
		config,
		client_id,
		"list_dlq",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "DLQ Messages:" << std::endl;
		if (response.contains("data"))
		{
			std::cout << response["data"].dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "List DLQ failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto cmd_reprocess(Utilities::ArgumentParser& args, const MailboxConfig& config, const std::string& client_id) -> int
{
	auto message_key = args.to_string("--message-key");
	auto timeout = args.to_int("--timeout");

	if (!message_key.has_value())
	{
		std::cerr << "Error: --message-key is required" << std::endl;
		return 1;
	}

	json payload;
	payload["messageKey"] = message_key.value();

	auto [ok, response] = send_request(
		config,
		client_id,
		"reprocess_dlq",
		payload,
		timeout.value_or(config.timeout_ms)
	);

	if (!ok)
	{
		std::cerr << "Error: " << response.dump(2) << std::endl;
		return 1;
	}

	if (response.value("ok", false))
	{
		std::cout << "Message reprocessed successfully" << std::endl;
		if (response.contains("data"))
		{
			std::cout << response["data"].dump(2) << std::endl;
		}
		return 0;
	}
	else
	{
		std::cerr << "Reprocess failed: ";
		if (response.contains("error"))
		{
			std::cerr << response["error"].dump() << std::endl;
		}
		return 1;
	}
}

auto main(int argc, char* argv[]) -> int
{
	Utilities::ArgumentParser args(argc, argv);

	if (argc < 2 || args.to_string("--help").has_value() || args.to_string("-h").has_value())
	{
		print_usage();
		return 0;
	}

	// Get global options
	MailboxConfig config;
	auto ipc_root = args.to_string("--ipc-root");
	if (ipc_root.has_value())
	{
		config.root = ipc_root.value();
	}

	auto timeout = args.to_int("--timeout");
	if (timeout.has_value())
	{
		config.timeout_ms = timeout.value();
	}

	auto client_id_opt = args.to_string("--client-id");
	std::string client_id = client_id_opt.value_or(std::format("client-{}", Utilities::Generator::guid().substr(0, 8)));

	// Get command
	std::string command = argv[1];

	if (command == "publish")
	{
		return cmd_publish(args, config, client_id);
	}
	else if (command == "consume")
	{
		return cmd_consume(args, config, client_id);
	}
	else if (command == "ack")
	{
		return cmd_ack(args, config, client_id);
	}
	else if (command == "nack")
	{
		return cmd_nack(args, config, client_id);
	}
	else if (command == "status")
	{
		return cmd_status(args, config, client_id);
	}
	else if (command == "health")
	{
		return cmd_health(args, config, client_id);
	}
	else if (command == "metrics")
	{
		return cmd_metrics(args, config, client_id);
	}
	else if (command == "list-dlq" || command == "listdlq" || command == "dlq")
	{
		return cmd_list_dlq(args, config, client_id);
	}
	else if (command == "reprocess" || command == "reprocess-dlq")
	{
		return cmd_reprocess(args, config, client_id);
	}
	else
	{
		std::cerr << "Unknown command: " << command << std::endl;
		print_usage();
		return 1;
	}
}
