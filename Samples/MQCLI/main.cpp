#include "ArgumentParser.h"
#include "BackendAdapter.h"
#include "Generator.h"
#include "Logger.h"
#include "SQLiteAdapter.h"

#include <format>
#include <iostream>
#include <nlohmann/json.hpp>

using namespace Utilities;
using json = nlohmann::json;

auto print_usage() -> void
{
	std::cout << "Yi-Rang MQ CLI Tool\n";
	std::cout << "\nUsage:\n";
	std::cout << "  mq_cli publish --queue <queue> --message <json> [--priority <n>]\n";
	std::cout << "  mq_cli consume --queue <queue> --consumer-id <id> [--timeout <sec>]\n";
	std::cout << "  mq_cli status --queue <queue>\n";
	std::cout << "  mq_cli ack --message-key <key>\n";
	std::cout << "  mq_cli nack --message-key <key> --reason <reason> [--requeue]\n";
	std::cout << "\nOptions:\n";
	std::cout << "  --db <path>           SQLite database path (default: ./data/yirangmq.db)\n";
	std::cout << "  --schema <path>       SQLite schema path (default: ./sqlite_schema.sql)\n";
	std::cout << "\nExamples:\n";
	std::cout << "  mq_cli publish --queue telemetry --message '{\"temp\":36.5,\"rpm\":1200}'\n";
	std::cout << "  mq_cli consume --queue telemetry --consumer-id consumer-01\n";
	std::cout << "  mq_cli status --queue telemetry\n";
}

auto cmd_publish(std::shared_ptr<SQLiteAdapter> adapter, ArgumentParser& args) -> int
{
	auto queue = args.to_string("--queue");
	auto message_json = args.to_string("--message");
	auto priority = args.to_int("--priority").value_or(0);

	if (!queue.has_value() || !message_json.has_value())
	{
		std::cerr << "Error: --queue and --message are required\n";
		return 1;
	}

	auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();

	MessageEnvelope envelope;
	envelope.key = "msg:" + queue.value() + ":" + Generator::guid();
	envelope.message_id = Generator::guid();
	envelope.queue = queue.value();
	envelope.payload_json = message_json.value();
	envelope.attributes_json = "{}";
	envelope.priority = priority;
	envelope.attempt = 0;
	envelope.created_at_ms = now;
	envelope.available_at_ms = now;

	auto [success, error] = adapter->enqueue(envelope);
	if (!success)
	{
		std::cerr << "Failed to publish message: " << error.value_or("unknown") << "\n";
		return 1;
	}

	std::cout << "Message published successfully\n";
	std::cout << "  Message ID: " << envelope.message_id << "\n";
	std::cout << "  Message Key: " << envelope.key << "\n";
	std::cout << "  Queue: " << envelope.queue << "\n";

	return 0;
}

auto cmd_consume(std::shared_ptr<SQLiteAdapter> adapter, ArgumentParser& args) -> int
{
	auto queue = args.to_string("--queue");
	auto consumer_id = args.to_string("--consumer-id");
	auto timeout_sec = args.to_int("--timeout").value_or(30);

	if (!queue.has_value() || !consumer_id.has_value())
	{
		std::cerr << "Error: --queue and --consumer-id are required\n";
		return 1;
	}

	auto result = adapter->lease_next(queue.value(), consumer_id.value(), timeout_sec);
	if (result.error.has_value())
	{
		std::cerr << "Error: " << result.error.value() << "\n";
		return 1;
	}

	if (!result.leased)
	{
		std::cout << "No messages available in queue: " << queue.value() << "\n";
		return 0;
	}

	auto& msg = result.message.value();
	auto& lease = result.lease.value();

	std::cout << "Message consumed successfully\n";
	std::cout << "  Message ID: " << msg.message_id << "\n";
	std::cout << "  Message Key: " << msg.key << "\n";
	std::cout << "  Queue: " << msg.queue << "\n";
	std::cout << "  Priority: " << msg.priority << "\n";
	std::cout << "  Attempt: " << msg.attempt << "\n";
	std::cout << "  Payload: " << msg.payload_json << "\n";
	std::cout << "  Lease ID: " << lease.lease_id << "\n";
	std::cout << "  Lease Until: " << lease.lease_until_ms << "\n";
	std::cout << "\nTo acknowledge, use:\n";
	std::cout << "  mq_cli ack --message-key " << msg.key << "\n";

	return 0;
}

auto cmd_status(std::shared_ptr<SQLiteAdapter> adapter, ArgumentParser& args) -> int
{
	auto queue = args.to_string("--queue");

	if (!queue.has_value())
	{
		std::cerr << "Error: --queue is required\n";
		return 1;
	}

	auto [metrics, error] = adapter->metrics(queue.value());
	if (error.has_value())
	{
		std::cerr << "Error: " << error.value() << "\n";
		return 1;
	}

	std::cout << "Queue Status: " << queue.value() << "\n";
	std::cout << "  Ready: " << metrics.ready << "\n";
	std::cout << "  Inflight: " << metrics.inflight << "\n";
	std::cout << "  Delayed: " << metrics.delayed << "\n";
	std::cout << "  DLQ: " << metrics.dlq << "\n";
	std::cout << "  Total: " << (metrics.ready + metrics.inflight + metrics.delayed + metrics.dlq) << "\n";

	return 0;
}

auto cmd_ack(std::shared_ptr<SQLiteAdapter> adapter, ArgumentParser& args) -> int
{
	auto message_key = args.to_string("--message-key");

	if (!message_key.has_value())
	{
		std::cerr << "Error: --message-key is required\n";
		return 1;
	}

	LeaseToken lease;
	lease.message_key = message_key.value();
	lease.lease_id = "cli-ack";
	lease.consumer_id = "cli";

	auto [success, error] = adapter->ack(lease);
	if (!success)
	{
		std::cerr << "Failed to acknowledge message: " << error.value_or("unknown") << "\n";
		return 1;
	}

	std::cout << "Message acknowledged successfully: " << message_key.value() << "\n";
	return 0;
}

auto cmd_nack(std::shared_ptr<SQLiteAdapter> adapter, ArgumentParser& args) -> int
{
	auto message_key = args.to_string("--message-key");
	auto reason = args.to_string("--reason");
	bool requeue = args.to_bool("--requeue").value_or(false);

	if (!message_key.has_value() || !reason.has_value())
	{
		std::cerr << "Error: --message-key and --reason are required\n";
		return 1;
	}

	LeaseToken lease;
	lease.message_key = message_key.value();
	lease.lease_id = "cli-nack";
	lease.consumer_id = "cli";

	auto [success, error] = adapter->nack(lease, reason.value(), requeue);
	if (!success)
	{
		std::cerr << "Failed to nack message: " << error.value_or("unknown") << "\n";
		return 1;
	}

	if (requeue)
	{
		std::cout << "Message requeued: " << message_key.value() << "\n";
	}
	else
	{
		std::cout << "Message moved to DLQ: " << message_key.value() << "\n";
	}

	return 0;
}

int main(int argc, char* argv[])
{
	ArgumentParser args(argc, argv);

	if (argc < 2)
	{
		print_usage();
		return 1;
	}

	std::string command = argv[1];

	// Parse database path
	std::string db_path = args.to_string("--db").value_or(args.program_folder() + "../data/yirangmq.db");
	std::string schema_path = args.to_string("--schema").value_or(args.program_folder() + "sqlite_schema.sql");

	// Initialize backend
	auto adapter = std::make_shared<SQLiteAdapter>(schema_path);

	BackendConfig config;
	config.type = BackendType::SQLite;
	config.sqlite.db_path = db_path;
	config.sqlite.kv_table = "kv";
	config.sqlite.message_index_table = "msg_index";
	config.sqlite.busy_timeout_ms = 5000;
	config.sqlite.journal_mode = "WAL";
	config.sqlite.synchronous = "NORMAL";

	auto [opened, open_error] = adapter->open(config);
	if (!opened)
	{
		std::cerr << "Failed to open database: " << open_error.value_or("unknown") << "\n";
		return 1;
	}

	int result = 0;

	if (command == "publish")
	{
		result = cmd_publish(adapter, args);
	}
	else if (command == "consume")
	{
		result = cmd_consume(adapter, args);
	}
	else if (command == "status")
	{
		result = cmd_status(adapter, args);
	}
	else if (command == "ack")
	{
		result = cmd_ack(adapter, args);
	}
	else if (command == "nack")
	{
		result = cmd_nack(adapter, args);
	}
	else
	{
		std::cerr << "Unknown command: " << command << "\n\n";
		print_usage();
		result = 1;
	}

	adapter->close();
	return result;
}
