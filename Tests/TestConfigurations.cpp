#include "TestHelpers.h"
#include "Configurations.h"
#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

using json = nlohmann::json;

namespace fs = std::filesystem;

// Helper: get the directory containing the test executable
// (ArgumentParser::program_folder uses _NSGetExecutablePath / /proc/self/exe,
//  so argv[0] is ignored — the config file must be placed next to the real executable)
static auto get_exe_dir() -> std::string
{
#if defined(__APPLE__)
	char buf[4096];
	uint32_t size = sizeof(buf);
	if (_NSGetExecutablePath(buf, &size) == 0)
	{
		return fs::path(buf).parent_path().string();
	}
#elif defined(__linux__)
	auto exe = fs::read_symlink("/proc/self/exe");
	return exe.parent_path().string();
#endif
	return fs::current_path().string();
}

static const std::string CONFIG_FILENAME = "main_mq_configuration.json";

// RAII guard: writes a custom config JSON to the executable directory,
// restores the original on destruction.
class ConfigFileGuard
{
public:
	explicit ConfigFileGuard(const json& config_json)
	{
		exe_dir_ = get_exe_dir();
		config_path_ = fs::path(exe_dir_) / CONFIG_FILENAME;

		// Save original if it exists
		if (fs::exists(config_path_))
		{
			std::ifstream ifs(config_path_);
			std::ostringstream ss;
			ss << ifs.rdbuf();
			original_content_ = ss.str();
			has_original_ = true;
		}

		// Write custom config
		std::ofstream ofs(config_path_);
		ofs << config_json.dump(2);
		ofs.close();
	}

	~ConfigFileGuard()
	{
		if (has_original_)
		{
			std::ofstream ofs(config_path_);
			ofs << original_content_;
		}
		else
		{
			std::error_code ec;
			fs::remove(config_path_, ec);
		}
	}

	auto make_configurations() -> std::unique_ptr<Configurations>
	{
		std::string fake_exe = exe_dir_ + "/fake_exe";
		std::vector<char> exe_buf(fake_exe.begin(), fake_exe.end());
		exe_buf.push_back('\0');
		char* argv[] = { exe_buf.data() };
		int argc = 1;

		Utilities::ArgumentParser args(argc, argv);
		return std::make_unique<Configurations>(std::move(args));
	}

private:
	std::string exe_dir_;
	fs::path config_path_;
	std::string original_content_;
	bool has_original_ = false;
};

class ConfigurationsTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		init_test_logger();
	}
};

// =============================================================================
// DefaultValues
// =============================================================================

TEST_F(ConfigurationsTest, DefaultValues)
{
	// Empty JSON — Configurations uses built-in defaults
	ConfigFileGuard guard(json::object());
	auto cfg = guard.make_configurations();

	// General defaults
	EXPECT_EQ(cfg->schema_version(), "0.1");
	EXPECT_EQ(cfg->node_id(), "local-01");
	EXPECT_EQ(cfg->backend_type(), BackendType::SQLite);
	EXPECT_EQ(cfg->operation_mode(), OperationMode::MailboxSqlite);

	// Lease defaults
	EXPECT_EQ(cfg->lease_visibility_timeout_sec(), 30);
	EXPECT_EQ(cfg->lease_sweep_interval_ms(), 1000);

	// Policy defaults
	auto policy = cfg->policy_defaults();
	EXPECT_EQ(policy.visibility_timeout_sec, 30);
	EXPECT_EQ(policy.retry.limit, 5);
	EXPECT_EQ(policy.retry.backoff, "exponential");
	EXPECT_EQ(policy.retry.initial_delay_sec, 1);
	EXPECT_EQ(policy.retry.max_delay_sec, 60);
	EXPECT_TRUE(policy.dlq.enabled);
	EXPECT_EQ(policy.dlq.retention_days, 14);

	// Mailbox defaults
	auto mailbox = cfg->mailbox_config();
	EXPECT_EQ(mailbox.root, "./ipc");
	EXPECT_EQ(mailbox.requests_dir, "requests");
	EXPECT_EQ(mailbox.processing_dir, "processing");
	EXPECT_EQ(mailbox.responses_dir, "responses");
	EXPECT_EQ(mailbox.dead_dir, "dead");
	EXPECT_EQ(mailbox.stale_timeout_ms, 30000);
	EXPECT_EQ(mailbox.poll_interval_ms, 100);

	// SQLite defaults
	auto sqlite = cfg->sqlite_config();
	EXPECT_EQ(sqlite.db_path, "./data/yirangmq.db");
	EXPECT_EQ(sqlite.kv_table, "kv");
	EXPECT_EQ(sqlite.message_index_table, "msg_index");
	EXPECT_EQ(sqlite.busy_timeout_ms, 5000);
	EXPECT_EQ(sqlite.journal_mode, "WAL");
	EXPECT_EQ(sqlite.synchronous, "NORMAL");

	// FileSystem defaults
	auto fsc = cfg->filesystem_config();
	EXPECT_EQ(fsc.root, "./data/fs");
	EXPECT_EQ(fsc.inbox_dir, "inbox");
	EXPECT_EQ(fsc.processing_dir, "processing");
	EXPECT_EQ(fsc.archive_dir, "archive");
	EXPECT_EQ(fsc.dlq_dir, "dlq");
	EXPECT_EQ(fsc.meta_dir, "meta");

	// Queues should be empty (empty JSON has no "queues" array)
	EXPECT_TRUE(cfg->queues().empty());
}

// =============================================================================
// SchemaVersionParsing
// =============================================================================

TEST_F(ConfigurationsTest, SchemaVersionParsing)
{
	ConfigFileGuard guard(json({{"schemaVersion", "2.0"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->schema_version(), "2.0");
}

// =============================================================================
// NodeIdParsing
// =============================================================================

TEST_F(ConfigurationsTest, NodeIdParsing)
{
	ConfigFileGuard guard(json({{"nodeId", "edge-node-42"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->node_id(), "edge-node-42");
}

// =============================================================================
// BackendType parsing
// =============================================================================

TEST_F(ConfigurationsTest, BackendTypeSqlite)
{
	ConfigFileGuard guard(json({{"backend", "sqlite"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->backend_type(), BackendType::SQLite);
}

TEST_F(ConfigurationsTest, BackendTypeFileSystem)
{
	ConfigFileGuard guard(json({{"backend", "filesystem"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->backend_type(), BackendType::FileSystem);
}

TEST_F(ConfigurationsTest, BackendTypeFileSystemShortForm)
{
	ConfigFileGuard guard(json({{"backend", "fs"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->backend_type(), BackendType::FileSystem);
}

TEST_F(ConfigurationsTest, BackendTypeHybrid)
{
	ConfigFileGuard guard(json({{"backend", "hybrid"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->backend_type(), BackendType::Hybrid);
}

// =============================================================================
// OperationModeParsing
// =============================================================================

TEST_F(ConfigurationsTest, OperationModeMailboxSqlite)
{
	ConfigFileGuard guard(json({{"mode", "mailbox_sqlite"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->operation_mode(), OperationMode::MailboxSqlite);
}

TEST_F(ConfigurationsTest, OperationModeMailboxFileSystem)
{
	ConfigFileGuard guard(json({{"mode", "mailbox_fs"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->operation_mode(), OperationMode::MailboxFileSystem);
}

TEST_F(ConfigurationsTest, OperationModeMailboxFileSystemLong)
{
	ConfigFileGuard guard(json({{"mode", "mailbox_filesystem"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->operation_mode(), OperationMode::MailboxFileSystem);
}

TEST_F(ConfigurationsTest, OperationModeHybrid)
{
	ConfigFileGuard guard(json({{"mode", "hybrid"}}));
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->operation_mode(), OperationMode::Hybrid);
}

// =============================================================================
// PathsParsing
// =============================================================================

TEST_F(ConfigurationsTest, PathsParsing)
{
	json config = {
		{"paths", {
			{"dataRoot", "/opt/yirangmq/data"},
			{"logRoot", "/var/log/yirangmq"}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->data_root(), "/opt/yirangmq/data");
	EXPECT_EQ(cfg->log_root(), "/var/log/yirangmq");
}

// =============================================================================
// SqliteConfigParsing
// =============================================================================

TEST_F(ConfigurationsTest, SqliteConfigParsing)
{
	json config = {
		{"sqlite", {
			{"dbPath", "/tmp/custom.db"},
			{"kvTable", "custom_kv"},
			{"messageIndexTable", "custom_idx"},
			{"busyTimeoutMs", 10000},
			{"journalMode", "DELETE"},
			{"synchronous", "FULL"}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto sqlite = cfg->sqlite_config();

	EXPECT_EQ(sqlite.db_path, "/tmp/custom.db");
	EXPECT_EQ(sqlite.kv_table, "custom_kv");
	EXPECT_EQ(sqlite.message_index_table, "custom_idx");
	EXPECT_EQ(sqlite.busy_timeout_ms, 10000);
	EXPECT_EQ(sqlite.journal_mode, "DELETE");
	EXPECT_EQ(sqlite.synchronous, "FULL");
}

// =============================================================================
// FileSystemConfigParsing
// =============================================================================

TEST_F(ConfigurationsTest, FileSystemConfigParsing)
{
	json config = {
		{"filesystem", {
			{"root", "/mnt/storage/mq"},
			{"inboxDir", "incoming"},
			{"processingDir", "active"},
			{"archiveDir", "done"},
			{"dlqDir", "failed"},
			{"metaDir", "metadata"}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto fsc = cfg->filesystem_config();

	EXPECT_EQ(fsc.root, "/mnt/storage/mq");
	EXPECT_EQ(fsc.inbox_dir, "incoming");
	EXPECT_EQ(fsc.processing_dir, "active");
	EXPECT_EQ(fsc.archive_dir, "done");
	EXPECT_EQ(fsc.dlq_dir, "failed");
	EXPECT_EQ(fsc.meta_dir, "metadata");
}

// =============================================================================
// MailboxConfigParsing
// =============================================================================

TEST_F(ConfigurationsTest, MailboxConfigParsing)
{
	json config = {
		{"ipc", {
			{"root", "/var/run/mq/ipc"},
			{"requestsDir", "req"},
			{"processingDir", "proc"},
			{"responsesDir", "resp"},
			{"deadDir", "dead_letters"},
			{"staleTimeoutMs", 60000},
			{"pollIntervalMs", 250}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto mailbox = cfg->mailbox_config();

	EXPECT_EQ(mailbox.root, "/var/run/mq/ipc");
	EXPECT_EQ(mailbox.requests_dir, "req");
	EXPECT_EQ(mailbox.processing_dir, "proc");
	EXPECT_EQ(mailbox.responses_dir, "resp");
	EXPECT_EQ(mailbox.dead_dir, "dead_letters");
	EXPECT_EQ(mailbox.stale_timeout_ms, 60000);
	EXPECT_EQ(mailbox.poll_interval_ms, 250);
}

// =============================================================================
// LeaseConfigParsing
// =============================================================================

TEST_F(ConfigurationsTest, LeaseConfigParsing)
{
	json config = {
		{"lease", {
			{"visibilityTimeoutSec", 120},
			{"sweepIntervalMs", 5000}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	EXPECT_EQ(cfg->lease_visibility_timeout_sec(), 120);
	EXPECT_EQ(cfg->lease_sweep_interval_ms(), 5000);
}

// =============================================================================
// PolicyDefaultsParsing
// =============================================================================

TEST_F(ConfigurationsTest, PolicyDefaultsParsing)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 60},
			{"retry", {
				{"limit", 10},
				{"backoff", "linear"},
				{"initialDelaySec", 2},
				{"maxDelaySec", 120}
			}},
			{"dlq", {
				{"enabled", false},
				{"queue", "system-dlq"},
				{"retentionDays", 30}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	EXPECT_EQ(policy.visibility_timeout_sec, 60);
	EXPECT_EQ(policy.retry.limit, 10);
	EXPECT_EQ(policy.retry.backoff, "linear");
	EXPECT_EQ(policy.retry.initial_delay_sec, 2);
	EXPECT_EQ(policy.retry.max_delay_sec, 120);
	EXPECT_FALSE(policy.dlq.enabled);
	EXPECT_EQ(policy.dlq.queue, "system-dlq");
	EXPECT_EQ(policy.dlq.retention_days, 30);
}

// =============================================================================
// QueuesParsing
// =============================================================================

TEST_F(ConfigurationsTest, QueuesParsing)
{
	json config = {
		{"queues", json::array({
			{
				{"name", "telemetry"},
				{"policy", {
					{"visibilityTimeoutSec", 45},
					{"retry", {
						{"limit", 3},
						{"backoff", "fixed"},
						{"initialDelaySec", 5},
						{"maxDelaySec", 5}
					}},
					{"dlq", {
						{"enabled", true},
						{"queue", "telemetry-dlq"},
						{"retentionDays", 7}
					}}
				}},
				{"messageSchema", {
					{"name", "telemetry-schema"},
					{"description", "Telemetry data schema"},
					{"rules", json::array({
						{{"field", "temp"}, {"type", "required"}},
						{{"field", "temp"}, {"type", "type"}, {"expectedType", "number"}}
					})}
				}}
			},
			{
				{"name", "commands"}
			}
		})}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto& queues = cfg->queues();

	ASSERT_EQ(queues.size(), 2u);

	// First queue: telemetry with custom policy and schema
	EXPECT_EQ(queues[0].name, "telemetry");
	EXPECT_EQ(queues[0].policy.visibility_timeout_sec, 45);
	EXPECT_EQ(queues[0].policy.retry.limit, 3);
	EXPECT_EQ(queues[0].policy.retry.backoff, "fixed");
	EXPECT_EQ(queues[0].policy.retry.initial_delay_sec, 5);
	EXPECT_EQ(queues[0].policy.retry.max_delay_sec, 5);
	EXPECT_TRUE(queues[0].policy.dlq.enabled);
	EXPECT_EQ(queues[0].policy.dlq.queue, "telemetry-dlq");
	EXPECT_EQ(queues[0].policy.dlq.retention_days, 7);

	// Schema should be present with 2 rules
	ASSERT_TRUE(queues[0].message_schema.has_value());
	EXPECT_EQ(queues[0].message_schema->name, "telemetry-schema");
	EXPECT_EQ(queues[0].message_schema->description, "Telemetry data schema");
	EXPECT_EQ(queues[0].message_schema->rules.size(), 2u);

	// Second queue: commands with defaults, no schema
	EXPECT_EQ(queues[1].name, "commands");
	EXPECT_FALSE(queues[1].message_schema.has_value());
}

// =============================================================================
// ValidationRuleLoading - all 8 rule types
// =============================================================================

TEST_F(ConfigurationsTest, ValidationRuleLoading)
{
	json config = {
		{"queues", json::array({
			{
				{"name", "sensor-data"},
				{"messageSchema", {
					{"name", "sensor-schema"},
					{"description", "Full sensor data schema"},
					{"rules", json::array({
						{{"field", "sensor_id"}, {"type", "required"}},
						{{"field", "temp"}, {"type", "type"}, {"expectedType", "number"}},
						{{"field", "sensor_id"}, {"type", "minLength"}, {"value", 3}},
						{{"field", "sensor_id"}, {"type", "maxLength"}, {"value", 64}},
						{{"field", "temp"}, {"type", "minValue"}, {"value", -40.0}},
						{{"field", "temp"}, {"type", "maxValue"}, {"value", 125.0}},
						{{"field", "sensor_id"}, {"type", "pattern"}, {"value", "^[A-Z]{2}-[0-9]+$"}},
						{{"field", "status"}, {"type", "enum"}, {"values", json::array({"active", "inactive", "maintenance"})}}
					})}
				}}
			}
		})}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto& queues = cfg->queues();

	ASSERT_EQ(queues.size(), 1u);
	ASSERT_TRUE(queues[0].message_schema.has_value());

	auto& rules = queues[0].message_schema->rules;
	ASSERT_EQ(rules.size(), 8u);

	// required
	EXPECT_EQ(rules[0].field, "sensor_id");
	EXPECT_EQ(rules[0].type, ValidationRuleType::Required);

	// type
	EXPECT_EQ(rules[1].field, "temp");
	EXPECT_EQ(rules[1].type, ValidationRuleType::Type);
	EXPECT_EQ(rules[1].expected_type, "number");

	// minLength
	EXPECT_EQ(rules[2].field, "sensor_id");
	EXPECT_EQ(rules[2].type, ValidationRuleType::MinLength);
	EXPECT_EQ(rules[2].min_length, 3);

	// maxLength
	EXPECT_EQ(rules[3].field, "sensor_id");
	EXPECT_EQ(rules[3].type, ValidationRuleType::MaxLength);
	EXPECT_EQ(rules[3].max_length, 64);

	// minValue
	EXPECT_EQ(rules[4].field, "temp");
	EXPECT_EQ(rules[4].type, ValidationRuleType::MinValue);
	EXPECT_DOUBLE_EQ(rules[4].min_value, -40.0);

	// maxValue
	EXPECT_EQ(rules[5].field, "temp");
	EXPECT_EQ(rules[5].type, ValidationRuleType::MaxValue);
	EXPECT_DOUBLE_EQ(rules[5].max_value, 125.0);

	// pattern
	EXPECT_EQ(rules[6].field, "sensor_id");
	EXPECT_EQ(rules[6].type, ValidationRuleType::Pattern);
	EXPECT_EQ(rules[6].pattern, "^[A-Z]{2}-[0-9]+$");

	// enum
	EXPECT_EQ(rules[7].field, "status");
	EXPECT_EQ(rules[7].type, ValidationRuleType::Enum);
	ASSERT_EQ(rules[7].enum_values.size(), 3u);
	EXPECT_EQ(rules[7].enum_values[0], "active");
	EXPECT_EQ(rules[7].enum_values[1], "inactive");
	EXPECT_EQ(rules[7].enum_values[2], "maintenance");
}

// =============================================================================
// ValidationRuleUnknownType
// =============================================================================

TEST_F(ConfigurationsTest, ValidationRuleUnknownType)
{
	json config = {
		{"queues", json::array({
			{
				{"name", "test-queue"},
				{"messageSchema", {
					{"name", "test-schema"},
					{"description", "Schema with unknown rule type"},
					{"rules", json::array({
						{{"field", "data"}, {"type", "unknownValidator"}}
					})}
				}}
			}
		})}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto& queues = cfg->queues();

	ASSERT_EQ(queues.size(), 1u);

	// load_message_schema returns nullopt if no valid rules were loaded
	EXPECT_FALSE(queues[0].message_schema.has_value());
}

// =============================================================================
// PolicyValidation - negative and invalid values get corrected
// =============================================================================

TEST_F(ConfigurationsTest, PolicyValidationNegativeRetryLimit)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", -5},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative retry.limit should be corrected to 0
	EXPECT_EQ(policy.retry.limit, 0);
}

TEST_F(ConfigurationsTest, PolicyValidationInvalidBackoff)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 5},
				{"backoff", "random_invalid"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Invalid backoff should be corrected to "exponential"
	EXPECT_EQ(policy.retry.backoff, "exponential");
}

TEST_F(ConfigurationsTest, PolicyValidationMaxLessThanInitialDelay)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 5},
				{"backoff", "exponential"},
				{"initialDelaySec", 100},
				{"maxDelaySec", 10}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// max < initial should be swapped
	EXPECT_LE(policy.retry.initial_delay_sec, policy.retry.max_delay_sec);
	EXPECT_EQ(policy.retry.initial_delay_sec, 10);
	EXPECT_EQ(policy.retry.max_delay_sec, 100);
}

TEST_F(ConfigurationsTest, PolicyValidationNegativeVisibilityTimeout)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", -10},
			{"retry", {
				{"limit", 5},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative visibility timeout should be corrected to 30
	EXPECT_EQ(policy.visibility_timeout_sec, 30);
}

TEST_F(ConfigurationsTest, PolicyValidationNegativeDlqRetention)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 5},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", -7}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative dlq.retentionDays should be corrected to 14
	EXPECT_EQ(policy.dlq.retention_days, 14);
}

TEST_F(ConfigurationsTest, LeaseValidationNegativeValues)
{
	json config = {
		{"lease", {
			{"visibilityTimeoutSec", -1},
			{"sweepIntervalMs", -500}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();

	// Negative lease values should be corrected to defaults
	EXPECT_EQ(cfg->lease_visibility_timeout_sec(), 30);
	EXPECT_EQ(cfg->lease_sweep_interval_ms(), 1000);
}

// =============================================================================
// BackendConfig composition
// =============================================================================

TEST_F(ConfigurationsTest, BackendConfigComposition)
{
	json config = {
		{"backend", "hybrid"},
		{"sqlite", {
			{"dbPath", "/tmp/hybrid.db"},
			{"kvTable", "kv_store"},
			{"messageIndexTable", "idx"},
			{"busyTimeoutMs", 3000},
			{"journalMode", "WAL"},
			{"synchronous", "NORMAL"}
		}},
		{"filesystem", {
			{"root", "/tmp/payload"},
			{"inboxDir", "in"},
			{"processingDir", "proc"},
			{"archiveDir", "arch"},
			{"dlqDir", "dead"},
			{"metaDir", "meta"}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto bc = cfg->backend_config();

	EXPECT_EQ(bc.type, BackendType::Hybrid);
	EXPECT_EQ(bc.sqlite.db_path, "/tmp/hybrid.db");
	EXPECT_EQ(bc.sqlite.kv_table, "kv_store");
	EXPECT_EQ(bc.filesystem.root, "/tmp/payload");
	EXPECT_EQ(bc.filesystem.inbox_dir, "in");
}

// =============================================================================
// QueueWithDefaultPolicy - queue without explicit policy inherits defaults
// =============================================================================

TEST_F(ConfigurationsTest, QueueWithDefaultPolicy)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 90},
			{"retry", {
				{"limit", 7},
				{"backoff", "linear"},
				{"initialDelaySec", 3},
				{"maxDelaySec", 90}
			}},
			{"dlq", {
				{"enabled", true},
				{"queue", "global-dlq"},
				{"retentionDays", 21}
			}}
		}},
		{"queues", json::array({
			{
				{"name", "events"}
			}
		})}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto& queues = cfg->queues();

	ASSERT_EQ(queues.size(), 1u);

	// Queue without explicit policy should inherit policyDefaults
	EXPECT_EQ(queues[0].policy.visibility_timeout_sec, 90);
	EXPECT_EQ(queues[0].policy.retry.limit, 7);
	EXPECT_EQ(queues[0].policy.retry.backoff, "linear");
	EXPECT_EQ(queues[0].policy.retry.initial_delay_sec, 3);
	EXPECT_EQ(queues[0].policy.retry.max_delay_sec, 90);
	EXPECT_TRUE(queues[0].policy.dlq.enabled);
	EXPECT_EQ(queues[0].policy.dlq.queue, "global-dlq");
	EXPECT_EQ(queues[0].policy.dlq.retention_days, 21);
}

// =============================================================================
// PolicyValidationNegativeInitialDelay
// =============================================================================

TEST_F(ConfigurationsTest, PolicyValidationNegativeInitialDelay)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 3},
				{"backoff", "exponential"},
				{"initialDelaySec", -5},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative initialDelaySec should be corrected to 1
	EXPECT_EQ(policy.retry.initial_delay_sec, 1);
}

// =============================================================================
// PolicyValidationNegativeMaxDelay
// =============================================================================

TEST_F(ConfigurationsTest, PolicyValidationNegativeMaxDelay)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 3},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", -10}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative maxDelaySec should be corrected to 60
	EXPECT_EQ(policy.retry.max_delay_sec, 60);
}

// =============================================================================
// TTL Configuration Tests
// =============================================================================

TEST_F(ConfigurationsTest, TTLSecDefaultZero)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"retry", {
				{"limit", 3},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// ttlSec not specified, should default to 0 (disabled)
	EXPECT_EQ(policy.ttl_sec, 0);
}

TEST_F(ConfigurationsTest, TTLSecParsedFromConfig)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"ttlSec", 3600},
			{"retry", {
				{"limit", 3},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	EXPECT_EQ(policy.ttl_sec, 3600);
}

TEST_F(ConfigurationsTest, TTLSecNegativeValidatesToZero)
{
	json config = {
		{"policyDefaults", {
			{"visibilityTimeoutSec", 30},
			{"ttlSec", -100},
			{"retry", {
				{"limit", 3},
				{"backoff", "exponential"},
				{"initialDelaySec", 1},
				{"maxDelaySec", 60}
			}},
			{"dlq", {
				{"enabled", true},
				{"retentionDays", 14}
			}}
		}}
	};

	ConfigFileGuard guard(config);
	auto cfg = guard.make_configurations();
	auto policy = cfg->policy_defaults();

	// Negative ttlSec should be corrected to 0 (disabled)
	EXPECT_EQ(policy.ttl_sec, 0);
}
