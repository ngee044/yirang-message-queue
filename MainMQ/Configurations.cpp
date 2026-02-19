#include "Configurations.h"

#include "Converter.h"
#include "File.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <filesystem>
#include <format>
#include <regex>
#include <set>

using json = nlohmann::json;

namespace
{
	auto is_valid_queue_name(const std::string& name) -> bool
	{
		if (name.empty() || name.length() > 256)
		{
			return false;
		}

		// Allow alphanumeric, hyphen, underscore
		return std::all_of(name.begin(), name.end(), [](char c) {
			return std::isalnum(c) || c == '-' || c == '_';
		});
	}

	auto is_valid_backoff_type(const std::string& backoff) -> bool
	{
		static const std::set<std::string> valid_types = { "exponential", "linear", "fixed" };
		return valid_types.find(backoff) != valid_types.end();
	}
}

		Configurations::Configurations(ArgumentParser&& arguments)
			: write_file_(LogTypes::None)
			, write_console_(LogTypes::Information)
			, root_path_("")
			, data_root_("./data")
			, log_root_("./logs")
			, schema_version_("0.1")
			, node_id_("local-01")
			, backend_type_(BackendType::SQLite)
			, operation_mode_(OperationMode::MailboxSqlite)
			, lease_visibility_timeout_sec_(30)
			, lease_sweep_interval_ms_(1000)
		{
			// SQLite defaults
			sqlite_config_.db_path = "./data/yirangmq.db";
			sqlite_config_.kv_table = "kv";
			sqlite_config_.message_index_table = "msg_index";
			sqlite_config_.busy_timeout_ms = 5000;
			sqlite_config_.journal_mode = "WAL";
			sqlite_config_.synchronous = "NORMAL";

			// FileSystem defaults
			filesystem_config_.root = "./data/fs";
			filesystem_config_.inbox_dir = "inbox";
			filesystem_config_.processing_dir = "processing";
			filesystem_config_.archive_dir = "archive";
			filesystem_config_.dlq_dir = "dlq";
			filesystem_config_.meta_dir = "meta";

			// IPC (Mailbox) defaults
			mailbox_config_.root = "./ipc";
			mailbox_config_.requests_dir = "requests";
			mailbox_config_.processing_dir = "processing";
			mailbox_config_.responses_dir = "responses";
			mailbox_config_.dead_dir = "dead";
			mailbox_config_.stale_timeout_ms = 30000;
			mailbox_config_.poll_interval_ms = 100;

			// Policy defaults
			policy_defaults_.visibility_timeout_sec = 30;
			policy_defaults_.ttl_sec = 0;
			policy_defaults_.retry.limit = 5;
			policy_defaults_.retry.backoff = "exponential";
			policy_defaults_.retry.initial_delay_sec = 1;
			policy_defaults_.retry.max_delay_sec = 60;
			policy_defaults_.dlq.enabled = true;
			policy_defaults_.dlq.queue = "";
			policy_defaults_.dlq.retention_days = 14;

			root_path_ = arguments.program_folder();

			load();
			parse(arguments);
			validate();
		}

		Configurations::~Configurations(void) {}

		auto Configurations::write_file() -> LogTypes { return write_file_; }
		auto Configurations::write_console() -> LogTypes { return write_console_; }

		auto Configurations::root_path() -> std::string { return root_path_; }
		auto Configurations::data_root() -> std::string { return data_root_; }
		auto Configurations::log_root() -> std::string { return log_root_; }

		auto Configurations::schema_version() -> std::string { return schema_version_; }
		auto Configurations::node_id() -> std::string { return node_id_; }
		auto Configurations::backend_type() -> BackendType { return backend_type_; }
		auto Configurations::operation_mode() -> OperationMode { return operation_mode_; }

		auto Configurations::mailbox_config() -> MailboxConfig { return mailbox_config_; }

		auto Configurations::sqlite_config() -> SQLiteConfig { return sqlite_config_; }
		auto Configurations::sqlite_db_path() -> std::string { return sqlite_config_.db_path; }
		auto Configurations::sqlite_kv_table() -> std::string { return sqlite_config_.kv_table; }
		auto Configurations::sqlite_message_index_table() -> std::string { return sqlite_config_.message_index_table; }
		auto Configurations::sqlite_schema_path() -> std::string { return root_path_ + "sqlite_schema.sql"; }
		auto Configurations::sqlite_busy_timeout_ms() -> int32_t { return sqlite_config_.busy_timeout_ms; }
		auto Configurations::sqlite_journal_mode() -> std::string { return sqlite_config_.journal_mode; }
		auto Configurations::sqlite_synchronous() -> std::string { return sqlite_config_.synchronous; }

		auto Configurations::filesystem_config() -> FileSystemConfig { return filesystem_config_; }

		auto Configurations::lease_visibility_timeout_sec() -> int32_t { return lease_visibility_timeout_sec_; }
		auto Configurations::lease_sweep_interval_ms() -> int32_t { return lease_sweep_interval_ms_; }

		auto Configurations::policy_defaults() -> QueuePolicy { return policy_defaults_; }

		auto Configurations::queues() -> std::vector<QueueConfig>& { return queues_; }

		auto Configurations::backend_config() -> BackendConfig
		{
			BackendConfig config;
			config.type = backend_type_;
			config.sqlite = sqlite_config_;
			config.filesystem = filesystem_config_;
			return config;
		}

		auto Configurations::load() -> void
		{
			std::filesystem::path path = root_path_ + "main_mq_configuration.json";
			if (!std::filesystem::exists(path))
			{
				Logger::handle().write(LogTypes::Error, std::format("Configuration file does not exist: {}", path.string()));
				return;
			}

			Utilities::File source;
			auto [opened, open_error] = source.open(path.string(), std::ios::in | std::ios::binary);
			if (!opened)
			{
				Logger::handle().write(LogTypes::Error, open_error.value_or("Failed to open configuration file"));
				return;
			}

			auto [source_data, read_error] = source.read_bytes();
			source.close();

			if (source_data == std::nullopt)
			{
				Logger::handle().write(LogTypes::Error, read_error.value_or("Failed to read configuration file"));
				return;
			}

			try
			{
				json config = json::parse(Utilities::Converter::to_string(source_data.value()));

				// Schema version
				if (config.contains("schemaVersion") && config["schemaVersion"].is_string())
				{
					schema_version_ = config["schemaVersion"].get<std::string>();
				}

				// Node ID
				if (config.contains("nodeId") && config["nodeId"].is_string())
				{
					node_id_ = config["nodeId"].get<std::string>();
				}

				// Backend type
				if (config.contains("backend") && config["backend"].is_string())
				{
					std::string backend = config["backend"].get<std::string>();
					if (backend == "sqlite")
					{
						backend_type_ = BackendType::SQLite;
					}
					else if (backend == "filesystem" || backend == "fs")
					{
						backend_type_ = BackendType::FileSystem;
					}
					else if (backend == "hybrid")
					{
						backend_type_ = BackendType::Hybrid;
					}
				}

				// Operation mode
				if (config.contains("mode") && config["mode"].is_string())
				{
					std::string mode = config["mode"].get<std::string>();
					if (mode == "mailbox_sqlite")
					{
						operation_mode_ = OperationMode::MailboxSqlite;
					}
					else if (mode == "mailbox_fs" || mode == "mailbox_filesystem")
					{
						operation_mode_ = OperationMode::MailboxFileSystem;
					}
					else if (mode == "hybrid")
					{
						operation_mode_ = OperationMode::Hybrid;
					}
				}

				// Paths
				if (config.contains("paths") && config["paths"].is_object())
				{
					auto& paths = config["paths"];
					if (paths.contains("dataRoot") && paths["dataRoot"].is_string())
					{
						data_root_ = paths["dataRoot"].get<std::string>();
					}
					if (paths.contains("logRoot") && paths["logRoot"].is_string())
					{
						log_root_ = paths["logRoot"].get<std::string>();
					}
				}

				// SQLite config
				if (config.contains("sqlite") && config["sqlite"].is_object())
				{
					auto& sqlite = config["sqlite"];
					if (sqlite.contains("dbPath") && sqlite["dbPath"].is_string())
					{
						sqlite_config_.db_path = sqlite["dbPath"].get<std::string>();
					}
					if (sqlite.contains("kvTable") && sqlite["kvTable"].is_string())
					{
						sqlite_config_.kv_table = sqlite["kvTable"].get<std::string>();
					}
					if (sqlite.contains("messageIndexTable") && sqlite["messageIndexTable"].is_string())
					{
						sqlite_config_.message_index_table = sqlite["messageIndexTable"].get<std::string>();
					}
					if (sqlite.contains("busyTimeoutMs") && sqlite["busyTimeoutMs"].is_number())
					{
						sqlite_config_.busy_timeout_ms = sqlite["busyTimeoutMs"].get<int32_t>();
					}
					if (sqlite.contains("journalMode") && sqlite["journalMode"].is_string())
					{
						sqlite_config_.journal_mode = sqlite["journalMode"].get<std::string>();
					}
					if (sqlite.contains("synchronous") && sqlite["synchronous"].is_string())
					{
						sqlite_config_.synchronous = sqlite["synchronous"].get<std::string>();
					}
				}

				// FileSystem config
				if (config.contains("filesystem") && config["filesystem"].is_object())
				{
					auto& fs = config["filesystem"];
					if (fs.contains("root") && fs["root"].is_string())
					{
						filesystem_config_.root = fs["root"].get<std::string>();
					}
					if (fs.contains("inboxDir") && fs["inboxDir"].is_string())
					{
						filesystem_config_.inbox_dir = fs["inboxDir"].get<std::string>();
					}
					if (fs.contains("processingDir") && fs["processingDir"].is_string())
					{
						filesystem_config_.processing_dir = fs["processingDir"].get<std::string>();
					}
					if (fs.contains("archiveDir") && fs["archiveDir"].is_string())
					{
						filesystem_config_.archive_dir = fs["archiveDir"].get<std::string>();
					}
					if (fs.contains("dlqDir") && fs["dlqDir"].is_string())
					{
						filesystem_config_.dlq_dir = fs["dlqDir"].get<std::string>();
					}
					if (fs.contains("metaDir") && fs["metaDir"].is_string())
					{
						filesystem_config_.meta_dir = fs["metaDir"].get<std::string>();
					}
				}

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
					if (ipc.contains("processingDir") && ipc["processingDir"].is_string())
					{
						mailbox_config_.processing_dir = ipc["processingDir"].get<std::string>();
					}
					if (ipc.contains("responsesDir") && ipc["responsesDir"].is_string())
					{
						mailbox_config_.responses_dir = ipc["responsesDir"].get<std::string>();
					}
					if (ipc.contains("deadDir") && ipc["deadDir"].is_string())
					{
						mailbox_config_.dead_dir = ipc["deadDir"].get<std::string>();
					}
					if (ipc.contains("staleTimeoutMs") && ipc["staleTimeoutMs"].is_number())
					{
						mailbox_config_.stale_timeout_ms = ipc["staleTimeoutMs"].get<int32_t>();
					}
					if (ipc.contains("pollIntervalMs") && ipc["pollIntervalMs"].is_number())
					{
						mailbox_config_.poll_interval_ms = ipc["pollIntervalMs"].get<int32_t>();
					}
					if (ipc.contains("useFolderWatcher") && ipc["useFolderWatcher"].is_boolean())
					{
						mailbox_config_.use_folder_watcher = ipc["useFolderWatcher"].get<bool>();
					}
				}

				// Lease config
				if (config.contains("lease") && config["lease"].is_object())
				{
					auto& lease = config["lease"];
					if (lease.contains("visibilityTimeoutSec") && lease["visibilityTimeoutSec"].is_number())
					{
						lease_visibility_timeout_sec_ = lease["visibilityTimeoutSec"].get<int32_t>();
					}
					if (lease.contains("sweepIntervalMs") && lease["sweepIntervalMs"].is_number())
					{
						lease_sweep_interval_ms_ = lease["sweepIntervalMs"].get<int32_t>();
					}
				}

				// Policy defaults
				if (config.contains("policyDefaults") && config["policyDefaults"].is_object())
				{
					policy_defaults_ = load_queue_policy(&config["policyDefaults"]);
				}

				// Queues
				if (config.contains("queues") && config["queues"].is_array())
				{
					queues_.clear();
					for (auto& queue_json : config["queues"])
					{
						QueueConfig queue_config;
						if (queue_json.contains("name") && queue_json["name"].is_string())
						{
							queue_config.name = queue_json["name"].get<std::string>();
						}
						if (queue_json.contains("policy") && queue_json["policy"].is_object())
						{
							queue_config.policy = load_queue_policy(&queue_json["policy"]);
						}
						else
						{
							queue_config.policy = policy_defaults_;
						}
						if (queue_json.contains("messageSchema") && queue_json["messageSchema"].is_object())
						{
							queue_config.message_schema = load_message_schema(&queue_json["messageSchema"]);
						}
						queues_.push_back(queue_config);
					}
				}
			}
			catch (const json::exception& e)
			{
				Logger::handle().write(LogTypes::Error, std::format("JSON parse error: {}", e.what()));
			}
		}

		auto Configurations::parse(ArgumentParser& arguments) -> void
		{
			auto string_target = arguments.to_string("--backend");
			if (string_target != std::nullopt)
			{
				if (string_target.value() == "sqlite")
				{
					backend_type_ = BackendType::SQLite;
				}
				else if (string_target.value() == "filesystem" || string_target.value() == "fs")
				{
					backend_type_ = BackendType::FileSystem;
				}
			}

			string_target = arguments.to_string("--db-path");
			if (string_target != std::nullopt)
			{
				sqlite_config_.db_path = string_target.value();
			}

			string_target = arguments.to_string("--data-root");
			if (string_target != std::nullopt)
			{
				data_root_ = string_target.value();
			}

			string_target = arguments.to_string("--log-root");
			if (string_target != std::nullopt)
			{
				log_root_ = string_target.value();
			}

			string_target = arguments.to_string("--node-id");
			if (string_target != std::nullopt)
			{
				node_id_ = string_target.value();
			}

			auto int_target = arguments.to_int("--visibility-timeout");
			if (int_target != std::nullopt)
			{
				lease_visibility_timeout_sec_ = int_target.value();
			}

			int_target = arguments.to_int("--write-console-log");
			if (int_target != std::nullopt)
			{
				write_console_ = static_cast<LogTypes>(int_target.value());
			}

			int_target = arguments.to_int("--write-file-log");
			if (int_target != std::nullopt)
			{
				write_file_ = static_cast<LogTypes>(int_target.value());
			}
		}

		auto Configurations::load_retry_policy(const void* json_obj) -> RetryPolicy
		{
			RetryPolicy policy;
			policy.limit = policy_defaults_.retry.limit;
			policy.backoff = policy_defaults_.retry.backoff;
			policy.initial_delay_sec = policy_defaults_.retry.initial_delay_sec;
			policy.max_delay_sec = policy_defaults_.retry.max_delay_sec;

			const json& obj = *static_cast<const json*>(json_obj);

			if (obj.contains("limit") && obj["limit"].is_number())
			{
				policy.limit = obj["limit"].get<int32_t>();
			}
			if (obj.contains("backoff") && obj["backoff"].is_string())
			{
				policy.backoff = obj["backoff"].get<std::string>();
			}
			if (obj.contains("initialDelaySec") && obj["initialDelaySec"].is_number())
			{
				policy.initial_delay_sec = obj["initialDelaySec"].get<int32_t>();
			}
			if (obj.contains("maxDelaySec") && obj["maxDelaySec"].is_number())
			{
				policy.max_delay_sec = obj["maxDelaySec"].get<int32_t>();
			}

			return policy;
		}

		auto Configurations::load_dlq_policy(const void* json_obj) -> DlqPolicy
		{
			DlqPolicy policy;
			policy.enabled = policy_defaults_.dlq.enabled;
			policy.queue = policy_defaults_.dlq.queue;
			policy.retention_days = policy_defaults_.dlq.retention_days;

			const json& obj = *static_cast<const json*>(json_obj);

			if (obj.contains("enabled") && obj["enabled"].is_boolean())
			{
				policy.enabled = obj["enabled"].get<bool>();
			}
			if (obj.contains("queue") && obj["queue"].is_string())
			{
				policy.queue = obj["queue"].get<std::string>();
			}
			if (obj.contains("retentionDays") && obj["retentionDays"].is_number())
			{
				policy.retention_days = obj["retentionDays"].get<int32_t>();
			}

			return policy;
		}

		auto Configurations::load_queue_policy(const void* json_obj) -> QueuePolicy
		{
			QueuePolicy policy;
			policy.visibility_timeout_sec = policy_defaults_.visibility_timeout_sec;

			const json& obj = *static_cast<const json*>(json_obj);

			if (obj.contains("visibilityTimeoutSec") && obj["visibilityTimeoutSec"].is_number())
			{
				policy.visibility_timeout_sec = obj["visibilityTimeoutSec"].get<int32_t>();
			}
			if (obj.contains("ttlSec") && obj["ttlSec"].is_number())
			{
				policy.ttl_sec = obj["ttlSec"].get<int32_t>();
			}
			if (obj.contains("retry") && obj["retry"].is_object())
			{
				policy.retry = load_retry_policy(&obj["retry"]);
			}
			else
			{
				policy.retry = policy_defaults_.retry;
			}
			if (obj.contains("dlq") && obj["dlq"].is_object())
			{
				policy.dlq = load_dlq_policy(&obj["dlq"]);
			}
			else
			{
				policy.dlq = policy_defaults_.dlq;
			}

			return policy;
		}

		auto Configurations::validate() -> void
		{
			// Validate lease timeouts
			if (lease_visibility_timeout_sec_ <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("Invalid lease.visibilityTimeoutSec ({}), using default 30", lease_visibility_timeout_sec_));
				lease_visibility_timeout_sec_ = 30;
			}

			if (lease_sweep_interval_ms_ <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("Invalid lease.sweepIntervalMs ({}), using default 1000", lease_sweep_interval_ms_));
				lease_sweep_interval_ms_ = 1000;
			}

			// Validate policy defaults
			validate_queue_policy(policy_defaults_, "policyDefaults");

			// Validate queues
			std::set<std::string> seen_names;
			for (auto& queue : queues_)
			{
				if (!is_valid_queue_name(queue.name))
				{
					Logger::handle().write(LogTypes::Error,
						std::format("Invalid queue name '{}': must be 1-256 alphanumeric/hyphen/underscore characters", queue.name));
				}

				if (seen_names.find(queue.name) != seen_names.end())
				{
					Logger::handle().write(LogTypes::Information,
						std::format("Duplicate queue name: {}", queue.name));
				}
				seen_names.insert(queue.name);

				validate_queue_policy(queue.policy, std::format("queue '{}'", queue.name));
			}
		}

		auto Configurations::validate_retry_policy(RetryPolicy& policy, const std::string& context) -> void
		{
			if (policy.limit < 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: retry.limit ({}) must be >= 0, using 0", context, policy.limit));
				policy.limit = 0;
			}

			if (!is_valid_backoff_type(policy.backoff))
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: invalid retry.backoff '{}', using 'exponential'", context, policy.backoff));
				policy.backoff = "exponential";
			}

			if (policy.initial_delay_sec <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: retry.initialDelaySec ({}) must be > 0, using 1", context, policy.initial_delay_sec));
				policy.initial_delay_sec = 1;
			}

			if (policy.max_delay_sec <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: retry.maxDelaySec ({}) must be > 0, using 60", context, policy.max_delay_sec));
				policy.max_delay_sec = 60;
			}

			if (policy.max_delay_sec < policy.initial_delay_sec)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: retry.maxDelaySec ({}) < initialDelaySec ({}), swapping values",
						context, policy.max_delay_sec, policy.initial_delay_sec));
				std::swap(policy.max_delay_sec, policy.initial_delay_sec);
			}
		}

		auto Configurations::validate_queue_policy(QueuePolicy& policy, const std::string& context) -> void
		{
			if (policy.visibility_timeout_sec <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: visibilityTimeoutSec ({}) must be > 0, using 30", context, policy.visibility_timeout_sec));
				policy.visibility_timeout_sec = 30;
			}

			if (policy.ttl_sec < 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: ttlSec ({}) must be >= 0, using 0 (disabled)", context, policy.ttl_sec));
				policy.ttl_sec = 0;
			}

			validate_retry_policy(policy.retry, context);

			if (policy.dlq.retention_days <= 0)
			{
				Logger::handle().write(LogTypes::Information,
					std::format("{}: dlq.retentionDays ({}) must be > 0, using 14", context, policy.dlq.retention_days));
				policy.dlq.retention_days = 14;
			}
		}

		auto Configurations::load_message_schema(const void* json_obj) -> std::optional<MessageSchema>
		{
			const json& obj = *static_cast<const json*>(json_obj);

			MessageSchema schema;

			if (obj.contains("name") && obj["name"].is_string())
			{
				schema.name = obj["name"].get<std::string>();
			}

			if (obj.contains("description") && obj["description"].is_string())
			{
				schema.description = obj["description"].get<std::string>();
			}

			if (obj.contains("rules") && obj["rules"].is_array())
			{
				for (const auto& rule_json : obj["rules"])
				{
					auto rule_opt = load_validation_rule(&rule_json);
					if (rule_opt.has_value())
					{
						schema.rules.push_back(rule_opt.value());
					}
				}
			}

			if (schema.rules.empty())
			{
				return std::nullopt;
			}

			return schema;
		}

		auto Configurations::load_validation_rule(const void* json_obj) -> std::optional<ValidationRule>
		{
			const json& obj = *static_cast<const json*>(json_obj);

			if (!obj.contains("field") || !obj["field"].is_string())
			{
				return std::nullopt;
			}

			if (!obj.contains("type") || !obj["type"].is_string())
			{
				return std::nullopt;
			}

			std::string field = obj["field"].get<std::string>();
			std::string type_str = obj["type"].get<std::string>();
			std::string error_msg = obj.value("errorMessage", "");

			ValidationRule rule;
			rule.field = field;
			rule.error_message = error_msg;

			if (type_str == "required")
			{
				rule.type = ValidationRuleType::Required;
			}
			else if (type_str == "type")
			{
				rule.type = ValidationRuleType::Type;
				if (obj.contains("expectedType") && obj["expectedType"].is_string())
				{
					rule.expected_type = obj["expectedType"].get<std::string>();
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "minLength")
			{
				rule.type = ValidationRuleType::MinLength;
				if (obj.contains("value") && obj["value"].is_number())
				{
					rule.min_length = obj["value"].get<int64_t>();
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "maxLength")
			{
				rule.type = ValidationRuleType::MaxLength;
				if (obj.contains("value") && obj["value"].is_number())
				{
					rule.max_length = obj["value"].get<int64_t>();
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "minValue")
			{
				rule.type = ValidationRuleType::MinValue;
				if (obj.contains("value") && obj["value"].is_number())
				{
					rule.min_value = obj["value"].get<double>();
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "maxValue")
			{
				rule.type = ValidationRuleType::MaxValue;
				if (obj.contains("value") && obj["value"].is_number())
				{
					rule.max_value = obj["value"].get<double>();
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "pattern")
			{
				rule.type = ValidationRuleType::Pattern;
				if (obj.contains("value") && obj["value"].is_string())
				{
					rule.pattern = obj["value"].get<std::string>();
					try
					{
						std::regex test_compile(rule.pattern);
					}
					catch (const std::regex_error& e)
					{
						Logger::handle().write(LogTypes::Information,
							std::format("invalid regex pattern '{}' for field '{}': {}",
								rule.pattern, field, e.what()));
						return std::nullopt;
					}
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "enum")
			{
				rule.type = ValidationRuleType::Enum;
				if (obj.contains("values") && obj["values"].is_array())
				{
					for (const auto& val : obj["values"])
					{
						if (val.is_string())
						{
							rule.enum_values.push_back(val.get<std::string>());
						}
					}
					if (rule.enum_values.empty())
					{
						return std::nullopt;
					}
				}
				else
				{
					return std::nullopt;
				}
			}
			else if (type_str == "custom")
			{
				rule.type = ValidationRuleType::Custom;
				if (obj.contains("validator") && obj["validator"].is_string())
				{
					rule.custom_validator_name = obj["validator"].get<std::string>();
				}
				else
				{
					Logger::handle().write(LogTypes::Information,
						std::format("custom rule for field '{}' missing 'validator' name", field));
					return std::nullopt;
				}
			}
			else
			{
				Logger::handle().write(LogTypes::Information,
					std::format("unknown validation rule type '{}' for field '{}'", type_str, field));
				return std::nullopt;
			}

			return rule;
		}