#include "Configurations.h"

#include "Converter.h"
#include "File.h"
#include "Generator.h"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <format>

using namespace Utilities;
using json = nlohmann::json;

Configurations::Configurations(ArgumentParser&& arguments)
	: write_file_(LogTypes::None)
	, write_console_(LogTypes::Information)
	, log_root_("./logs")
	, root_path_("")
	, default_queue_("")
	, default_target_("")
	, default_priority_(0)
	, client_id_("")
{
	// IPC (Mailbox) defaults
	mailbox_config_.root = "./ipc";
	mailbox_config_.requests_dir = "requests";
	mailbox_config_.responses_dir = "responses";
	mailbox_config_.timeout_ms = 30000;

	root_path_ = arguments.program_folder();

	load();
	parse(arguments);

	// Generate client ID if not specified
	if (client_id_.empty())
	{
		client_id_ = std::format("publisher-{}", Generator::guid().substr(0, 8));
	}
}

Configurations::~Configurations(void) {}

auto Configurations::write_file() -> LogTypes { return write_file_; }
auto Configurations::write_console() -> LogTypes { return write_console_; }
auto Configurations::log_root() -> std::string { return log_root_; }

auto Configurations::root_path() -> std::string { return root_path_; }

auto Configurations::mailbox_config() -> MailboxConfig { return mailbox_config_; }
auto Configurations::ipc_root() -> std::string { return mailbox_config_.root; }
auto Configurations::timeout_ms() -> int32_t { return mailbox_config_.timeout_ms; }

auto Configurations::default_queue() -> std::string { return default_queue_; }
auto Configurations::default_target() -> std::string { return default_target_; }
auto Configurations::default_priority() -> int32_t { return default_priority_; }

auto Configurations::client_id() -> std::string { return client_id_; }

auto Configurations::load() -> void
{
	std::filesystem::path path = root_path_ + "publisher_configuration.json";
	if (!std::filesystem::exists(path))
	{
		// Try alternate name
		path = root_path_ + "publisher.json";
		if (!std::filesystem::exists(path))
		{
			return;
		}
	}

	File source;
	auto [opened, open_error] = source.open(path.string(), std::ios::in | std::ios::binary);
	if (!opened)
	{
		return;
	}

	auto [source_data, read_error] = source.read_bytes();
	source.close();

	if (source_data == std::nullopt)
	{
		return;
	}

	try
	{
		json config = json::parse(Converter::to_string(source_data.value()));

		// IPC (Mailbox) config
		if (config.contains("ipc") && config["ipc"].is_object())
		{
			auto& ipc = config["ipc"];
			if (ipc.contains("root") && ipc["root"].is_string())
			{
				mailbox_config_.root = ipc["root"].get<std::string>();
			}
			if (ipc.contains("requestsDir") && ipc["requestsDir"].is_string())
			{
				mailbox_config_.requests_dir = ipc["requestsDir"].get<std::string>();
			}
			if (ipc.contains("responsesDir") && ipc["responsesDir"].is_string())
			{
				mailbox_config_.responses_dir = ipc["responsesDir"].get<std::string>();
			}
			if (ipc.contains("timeoutMs") && ipc["timeoutMs"].is_number())
			{
				mailbox_config_.timeout_ms = ipc["timeoutMs"].get<int32_t>();
			}
		}

		// Legacy flat format support
		if (config.contains("ipcRoot") && config["ipcRoot"].is_string())
		{
			mailbox_config_.root = config["ipcRoot"].get<std::string>();
		}
		if (config.contains("timeout") && config["timeout"].is_number())
		{
			mailbox_config_.timeout_ms = config["timeout"].get<int32_t>();
		}

		// Publisher settings
		if (config.contains("publisher") && config["publisher"].is_object())
		{
			auto& pub = config["publisher"];
			if (pub.contains("queue") && pub["queue"].is_string())
			{
				default_queue_ = pub["queue"].get<std::string>();
			}
			if (pub.contains("target") && pub["target"].is_string())
			{
				default_target_ = pub["target"].get<std::string>();
			}
			if (pub.contains("priority") && pub["priority"].is_number())
			{
				default_priority_ = pub["priority"].get<int32_t>();
			}
		}

		// Legacy flat format support
		if (config.contains("queue") && config["queue"].is_string())
		{
			default_queue_ = config["queue"].get<std::string>();
		}
		if (config.contains("target") && config["target"].is_string())
		{
			default_target_ = config["target"].get<std::string>();
		}
		if (config.contains("priority") && config["priority"].is_number())
		{
			default_priority_ = config["priority"].get<int32_t>();
		}

		// Client ID
		if (config.contains("clientId") && config["clientId"].is_string())
		{
			client_id_ = config["clientId"].get<std::string>();
		}

		// Logging config
		if (config.contains("logging") && config["logging"].is_object())
		{
			auto& logging = config["logging"];
			if (logging.contains("writeConsole") && logging["writeConsole"].is_number())
			{
				write_console_ = static_cast<LogTypes>(logging["writeConsole"].get<int32_t>());
			}
			if (logging.contains("writeFile") && logging["writeFile"].is_number())
			{
				write_file_ = static_cast<LogTypes>(logging["writeFile"].get<int32_t>());
			}
			if (logging.contains("logRoot") && logging["logRoot"].is_string())
			{
				log_root_ = logging["logRoot"].get<std::string>();
			}
		}
	}
	catch (const json::exception&)
	{
		// Configuration parse error - will use defaults
	}
}

auto Configurations::parse(ArgumentParser& arguments) -> void
{
	auto string_target = arguments.to_string("--ipc-root");
	if (string_target != std::nullopt)
	{
		mailbox_config_.root = string_target.value();
	}

	auto int_target = arguments.to_int("--timeout");
	if (int_target != std::nullopt)
	{
		mailbox_config_.timeout_ms = int_target.value();
	}

	string_target = arguments.to_string("--queue");
	if (string_target != std::nullopt)
	{
		default_queue_ = string_target.value();
	}

	string_target = arguments.to_string("--target");
	if (string_target != std::nullopt)
	{
		default_target_ = string_target.value();
	}

	int_target = arguments.to_int("--priority");
	if (int_target != std::nullopt)
	{
		default_priority_ = int_target.value();
	}

	string_target = arguments.to_string("--client-id");
	if (string_target != std::nullopt)
	{
		client_id_ = string_target.value();
	}
}
