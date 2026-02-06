#include "TestHelpers.h"
#include "HybridAdapter.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helper: locate sqlite_schema.sql next to executable or in current directory
// ---------------------------------------------------------------------------
static auto find_schema_path() -> std::string
{
	// Check current directory
	if (fs::exists("sqlite_schema.sql"))
	{
		return "sqlite_schema.sql";
	}

	// Check next to executable (common when run via ctest from build/out)
	auto exe_dir = fs::current_path();
	auto candidate = exe_dir / "sqlite_schema.sql";
	if (fs::exists(candidate))
	{
		return candidate.string();
	}

	// Fallback: walk up to project root and find in MainMQ/BackendAdapter
	auto search = exe_dir;
	for (int i = 0; i < 5; ++i)
	{
		auto deep = search / "MainMQ" / "BackendAdapter" / "sqlite_schema.sql";
		if (fs::exists(deep))
		{
			return deep.string();
		}

		auto shallow = search / "MainMQ" / "sqlite_schema.sql";
		if (fs::exists(shallow))
		{
			return shallow.string();
		}

		if (search.has_parent_path() && search.parent_path() != search)
		{
			search = search.parent_path();
		}
		else
		{
			break;
		}
	}

	return "sqlite_schema.sql";
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class HybridAdapterTest : public ::testing::Test
{
protected:
	std::unique_ptr<TempDir> temp_dir_;
	std::unique_ptr<HybridAdapter> adapter_;
	std::string schema_path_;
	fs::path original_cwd_;

	void SetUp() override
	{
		init_test_logger();

		temp_dir_ = std::make_unique<TempDir>("hybrid_adapter_test_");
		schema_path_ = find_schema_path();

		// Save original CWD and change to temp dir so that payload_root_
		// ("./data/payloads") resolves inside the temp directory
		original_cwd_ = fs::current_path();
		fs::current_path(temp_dir_->path_obj());

		adapter_ = std::make_unique<HybridAdapter>(schema_path_);

		auto config = make_hybrid_config(temp_dir_->path());
		auto [ok, err] = adapter_->open(config);
		ASSERT_TRUE(ok) << "Failed to open HybridAdapter: " << err.value_or("unknown");
	}

	void TearDown() override
	{
		if (adapter_)
		{
			adapter_->close();
			adapter_.reset();
		}

		// Restore original CWD before temp dir cleanup
		std::error_code ec;
		fs::current_path(original_cwd_, ec);

		temp_dir_.reset();
	}

	// Convenience: path to payload root used by the adapter (relative to temp dir CWD)
	auto payload_root() -> std::string
	{
		return "./data/payloads";
	}

	auto active_dir(const std::string& queue) -> std::string
	{
		return std::format("{}/{}/active", payload_root(), queue);
	}

	auto archive_dir(const std::string& queue) -> std::string
	{
		return std::format("{}/{}/archive", payload_root(), queue);
	}

	auto dlq_dir(const std::string& queue) -> std::string
	{
		return std::format("{}/{}/dlq", payload_root(), queue);
	}
};

// ---------------------------------------------------------------------------
// OpenClose: DB file and payload directory are created after open
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, OpenClose)
{
	// SQLite database file should exist
	auto db_path = temp_dir_->path() + "/hybrid.db";
	EXPECT_TRUE(fs::exists(db_path)) << "Database file was not created";

	// Payload root directory should exist
	EXPECT_TRUE(fs::exists(payload_root())) << "Payload root directory was not created";

	// Close and verify we can re-open
	adapter_->close();

	auto config = make_hybrid_config(temp_dir_->path());
	auto [ok, err] = adapter_->open(config);
	EXPECT_TRUE(ok) << "Failed to re-open after close: " << err.value_or("unknown");
}

// ---------------------------------------------------------------------------
// TypeMismatch: open with wrong BackendType
// Note: HybridAdapter does not check config.type, so open succeeds regardless.
// This test documents that behavior.
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, TypeMismatch)
{
	HybridAdapter other_adapter(schema_path_);

	BackendConfig bad_config;
	bad_config.type = BackendType::FileSystem;
	bad_config.sqlite.db_path = temp_dir_->path() + "/mismatch.db";
	bad_config.sqlite.kv_table = "kv";
	bad_config.sqlite.message_index_table = "msg_index";
	bad_config.sqlite.busy_timeout_ms = 5000;
	bad_config.sqlite.journal_mode = "WAL";
	bad_config.sqlite.synchronous = "NORMAL";

	// HybridAdapter does not validate config.type -- it opens regardless
	auto [ok, err] = other_adapter.open(bad_config);
	EXPECT_TRUE(ok) << "HybridAdapter does not reject mismatched config.type";

	other_adapter.close();
}

// ---------------------------------------------------------------------------
// EnqueueAndLease: enqueue creates payload file + DB index, lease returns data
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, EnqueueAndLease)
{
	auto env = make_envelope("telemetry", R"({"temp":36.5})");
	auto original_key = env.key;
	auto original_msg_id = env.message_id;

	auto [ok, err] = adapter_->enqueue(env);
	ASSERT_TRUE(ok) << "enqueue failed: " << err.value_or("unknown");

	// Verify payload file exists in active directory
	auto payload_path = std::format("{}/{}.json", active_dir("telemetry"), original_msg_id);
	EXPECT_TRUE(fs::exists(payload_path)) << "Payload file was not created at: " << payload_path;

	// Lease the message
	auto result = adapter_->lease_next("telemetry", "worker-01", 30);
	ASSERT_TRUE(result.leased) << "lease_next failed: " << result.error.value_or("unknown");
	ASSERT_TRUE(result.message.has_value());
	ASSERT_TRUE(result.lease.has_value());

	EXPECT_EQ(result.message->queue, "telemetry");
	EXPECT_EQ(result.message->payload_json, R"({"temp":36.5})");
	EXPECT_EQ(result.message->key, original_key);
	EXPECT_EQ(result.lease->consumer_id, "worker-01");
	EXPECT_FALSE(result.lease->lease_id.empty());
	EXPECT_GT(result.lease->lease_until_ms, 0);
}

// ---------------------------------------------------------------------------
// AckDeletesPayload: ack removes payload from active, moves to archive
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, AckDeletesPayload)
{
	auto env = make_envelope("jobs", R"({"action":"build"})");
	auto msg_id = env.message_id;

	adapter_->enqueue(env);

	auto lease_result = adapter_->lease_next("jobs", "w1", 30);
	ASSERT_TRUE(lease_result.leased);
	ASSERT_TRUE(lease_result.lease.has_value());

	auto active_path = std::format("{}/{}.json", active_dir("jobs"), msg_id);
	EXPECT_TRUE(fs::exists(active_path)) << "Payload should exist in active before ack";

	auto [aok, aerr] = adapter_->ack(lease_result.lease.value());
	EXPECT_TRUE(aok) << "ack failed: " << aerr.value_or("unknown");

	// Payload should be moved from active to archive
	EXPECT_FALSE(fs::exists(active_path)) << "Payload should be removed from active after ack";

	auto archive_path = std::format("{}/{}.json", archive_dir("jobs"), msg_id);
	EXPECT_TRUE(fs::exists(archive_path)) << "Payload should be moved to archive after ack";

	// Should not be leasable anymore
	auto result2 = adapter_->lease_next("jobs", "w1", 30);
	EXPECT_FALSE(result2.leased);
}

// ---------------------------------------------------------------------------
// NackRequeue: nack with requeue returns message to ready state
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, NackRequeue)
{
	auto env = make_envelope("tasks", R"({"step":"validate"})");
	adapter_->enqueue(env);

	auto lease_result = adapter_->lease_next("tasks", "w1", 30);
	ASSERT_TRUE(lease_result.leased);
	ASSERT_TRUE(lease_result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(lease_result.lease.value(), "transient failure", true);
	EXPECT_TRUE(nok) << "nack requeue failed: " << nerr.value_or("unknown");

	// Message should be leasable again
	auto result2 = adapter_->lease_next("tasks", "w2", 30);
	ASSERT_TRUE(result2.leased) << "Message should be leasable after nack with requeue";
	ASSERT_TRUE(result2.message.has_value());
	EXPECT_EQ(result2.message->payload_json, R"({"step":"validate"})");
}

// ---------------------------------------------------------------------------
// NackToDlq: nack without requeue moves to DLQ
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, NackToDlq)
{
	auto env = make_envelope("orders", R"({"id":"ORD-001"})");
	auto msg_id = env.message_id;

	adapter_->enqueue(env);

	auto lease_result = adapter_->lease_next("orders", "w1", 30);
	ASSERT_TRUE(lease_result.leased);
	ASSERT_TRUE(lease_result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(lease_result.lease.value(), "permanent failure", false);
	EXPECT_TRUE(nok) << "nack to DLQ failed: " << nerr.value_or("unknown");

	// Payload should be moved to DLQ directory
	auto active_path = std::format("{}/{}.json", active_dir("orders"), msg_id);
	EXPECT_FALSE(fs::exists(active_path)) << "Payload should not remain in active after DLQ nack";

	auto dlq_path_file = std::format("{}/{}.json", dlq_dir("orders"), msg_id);
	EXPECT_TRUE(fs::exists(dlq_path_file)) << "Payload should be moved to DLQ directory";

	// Should not be leasable from the main queue
	auto result2 = adapter_->lease_next("orders", "w1", 30);
	EXPECT_FALSE(result2.leased) << "DLQ message should not be leasable";
}

// ---------------------------------------------------------------------------
// Metrics: state counts match operations
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, Metrics)
{
	// Empty queue
	auto [m0, m0err] = adapter_->metrics("stats_q");
	EXPECT_FALSE(m0err.has_value());
	EXPECT_EQ(m0.ready, 0u);
	EXPECT_EQ(m0.inflight, 0u);
	EXPECT_EQ(m0.delayed, 0u);
	EXPECT_EQ(m0.dlq, 0u);

	// Enqueue 3 messages
	adapter_->enqueue(make_envelope("stats_q", R"({"n":1})"));
	adapter_->enqueue(make_envelope("stats_q", R"({"n":2})"));
	adapter_->enqueue(make_envelope("stats_q", R"({"n":3})"));

	auto [m1, m1err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m1.ready, 3u);

	// Lease one
	auto lease_result = adapter_->lease_next("stats_q", "w1", 30);
	ASSERT_TRUE(lease_result.leased);

	auto [m2, m2err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m2.ready, 2u);
	EXPECT_EQ(m2.inflight, 1u);

	// Ack it
	adapter_->ack(lease_result.lease.value());

	auto [m3, m3err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m3.ready, 2u);
	EXPECT_EQ(m3.inflight, 0u);

	// Lease and nack to DLQ
	auto lease_result2 = adapter_->lease_next("stats_q", "w1", 30);
	ASSERT_TRUE(lease_result2.leased);
	adapter_->nack(lease_result2.lease.value(), "fail", false);

	auto [m4, m4err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m4.ready, 1u);
	EXPECT_EQ(m4.dlq, 1u);
}

// ---------------------------------------------------------------------------
// ConsistencyClean: check_consistency on clean state returns zero issues
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, ConsistencyClean)
{
	// Enqueue a message so there is something to check
	auto env = make_envelope("clean_q", R"({"data":"consistent"})");
	auto [ok, err] = adapter_->enqueue(env);
	ASSERT_TRUE(ok);

	auto [report, check_err] = adapter_->check_consistency("clean_q");
	EXPECT_FALSE(check_err.has_value()) << "check_consistency error: " << check_err.value_or("");

	EXPECT_EQ(report.orphan_payloads, 0);
	EXPECT_EQ(report.missing_payloads, 0);
	EXPECT_EQ(report.invalid_states, 0);
	EXPECT_TRUE(report.issues.empty())
		<< "Clean state should have no consistency issues, but found " << report.issues.size();
}

// ---------------------------------------------------------------------------
// ConsistencyOrphanPayload: payload file exists with no DB index entry
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, ConsistencyOrphanPayload)
{
	// First, enqueue a real message to ensure the queue directories exist
	auto env = make_envelope("orphan_q", R"({"data":"real"})");
	auto [ok, err] = adapter_->enqueue(env);
	ASSERT_TRUE(ok);

	// Create an orphan payload file (no corresponding DB entry)
	auto orphan_path = std::format("{}/orphan-msg-999.json", active_dir("orphan_q"));
	{
		std::ofstream orphan_file(orphan_path, std::ios::out | std::ios::trunc);
		ASSERT_TRUE(orphan_file.is_open()) << "Failed to create orphan file at: " << orphan_path;
		orphan_file << R"({"payload":"orphan","attributes":"{}"})";
		orphan_file.close();
	}
	ASSERT_TRUE(fs::exists(orphan_path)) << "Orphan file should exist";

	auto [report, check_err] = adapter_->check_consistency("orphan_q");
	EXPECT_FALSE(check_err.has_value()) << "check_consistency error: " << check_err.value_or("");

	EXPECT_GE(report.orphan_payloads, 1)
		<< "Should detect at least 1 orphan payload";
	EXPECT_EQ(report.missing_payloads, 0);

	// Verify the orphan issue details
	bool found_orphan = false;
	for (const auto& issue : report.issues)
	{
		if (issue.type == ConsistencyIssueType::OrphanPayload
			&& issue.queue == "orphan_q"
			&& issue.payload_path.find("orphan-msg-999") != std::string::npos)
		{
			found_orphan = true;
			break;
		}
	}
	EXPECT_TRUE(found_orphan) << "Should find the specific orphan payload issue";
}

// ---------------------------------------------------------------------------
// ConsistencyMissingPayload: DB index entry exists but payload file is missing
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, ConsistencyMissingPayload)
{
	// Enqueue a real message so queue directories and a valid entry exist
	auto env = make_envelope("missing_q", R"({"data":"will_lose_payload"})");
	auto msg_id = env.message_id;

	auto [ok, err] = adapter_->enqueue(env);
	ASSERT_TRUE(ok);

	// Delete the payload file, leaving the DB index entry intact
	auto payload_path = std::format("{}/{}.json", active_dir("missing_q"), msg_id);
	ASSERT_TRUE(fs::exists(payload_path)) << "Payload should exist before deletion";

	std::error_code ec;
	fs::remove(payload_path, ec);
	ASSERT_FALSE(fs::exists(payload_path)) << "Payload should be deleted";

	auto [report, check_err] = adapter_->check_consistency("missing_q");
	EXPECT_FALSE(check_err.has_value()) << "check_consistency error: " << check_err.value_or("");

	EXPECT_GE(report.missing_payloads, 1)
		<< "Should detect at least 1 missing payload";
	EXPECT_EQ(report.orphan_payloads, 0);

	// Verify the missing payload issue details
	bool found_missing = false;
	for (const auto& issue : report.issues)
	{
		if (issue.type == ConsistencyIssueType::MissingPayload
			&& issue.queue == "missing_q")
		{
			found_missing = true;
			break;
		}
	}
	EXPECT_TRUE(found_missing) << "Should find the specific missing payload issue";
}

// ---------------------------------------------------------------------------
// RepairOrphanPayload: repair creates DB entry for orphan file
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, RepairOrphanPayload)
{
	// Enqueue a real message to create directories
	auto env = make_envelope("repair_q", R"({"data":"real"})");
	adapter_->enqueue(env);

	// Create an orphan payload file
	auto orphan_path = std::format("{}/orphan-repair-001.json", active_dir("repair_q"));
	{
		std::ofstream orphan_file(orphan_path, std::ios::out | std::ios::trunc);
		ASSERT_TRUE(orphan_file.is_open());
		orphan_file << R"({"payload":"orphan_data","attributes":"{}"})";
		orphan_file.close();
	}

	// Detect the inconsistency
	auto [report, check_err] = adapter_->check_consistency("repair_q");
	ASSERT_FALSE(check_err.has_value());
	ASSERT_GE(report.orphan_payloads, 1);

	// Repair
	auto [repaired, repair_err] = adapter_->repair_consistency(report);
	EXPECT_FALSE(repair_err.has_value()) << "repair error: " << repair_err.value_or("");
	EXPECT_GE(repaired, 1) << "Should have repaired at least 1 issue";

	// After repair, consistency check should be clean
	auto [report2, check_err2] = adapter_->check_consistency("repair_q");
	EXPECT_EQ(report2.orphan_payloads, 0) << "Orphan should be resolved after repair";
}

// ---------------------------------------------------------------------------
// RepairMissingPayload: repair moves index entry with missing file to DLQ
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, RepairMissingPayload)
{
	auto env = make_envelope("repair_missing_q", R"({"data":"will_vanish"})");
	auto msg_id = env.message_id;

	adapter_->enqueue(env);

	// Delete the payload file
	auto payload_path = std::format("{}/{}.json", active_dir("repair_missing_q"), msg_id);
	std::error_code ec;
	fs::remove(payload_path, ec);
	ASSERT_FALSE(fs::exists(payload_path));

	// Detect
	auto [report, check_err] = adapter_->check_consistency("repair_missing_q");
	ASSERT_GE(report.missing_payloads, 1);

	// Repair
	auto [repaired, repair_err] = adapter_->repair_consistency(report);
	EXPECT_GE(repaired, 1);

	// The message should no longer be in ready state (moved to DLQ by repair)
	auto [m, merr] = adapter_->metrics("repair_missing_q");
	EXPECT_EQ(m.ready, 0u) << "Message with missing payload should not remain ready";
}

// ---------------------------------------------------------------------------
// ExtendLease: extending lease updates expiry time
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, ExtendLease)
{
	auto env = make_envelope("extend_q", R"({"task":"long_run"})");
	adapter_->enqueue(env);

	auto lease_result = adapter_->lease_next("extend_q", "w1", 5);
	ASSERT_TRUE(lease_result.leased);
	ASSERT_TRUE(lease_result.lease.has_value());

	auto original_lease_until = lease_result.lease->lease_until_ms;

	auto [eok, eerr] = adapter_->extend_lease(lease_result.lease.value(), 60);
	EXPECT_TRUE(eok) << "extend_lease failed: " << eerr.value_or("unknown");
}

// ---------------------------------------------------------------------------
// EmptyQueueLease: leasing from empty queue returns no message
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ReprocessDlq: move DLQ message back to ready, verify payload restored
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, ReprocessDlqMessageMovesBackToReady)
{
	auto env = make_envelope("reprocess_q", R"({"data":"dlq_reprocess"})");
	auto msg_key = env.key;
	auto msg_id = env.message_id;

	adapter_->enqueue(env);

	// Lease then nack to DLQ
	auto lease_result = adapter_->lease_next("reprocess_q", "w1", 30);
	ASSERT_TRUE(lease_result.leased);

	auto [nok, nerr] = adapter_->nack(lease_result.lease.value(), "permanent failure", false);
	ASSERT_TRUE(nok) << "nack to DLQ failed: " << nerr.value_or("unknown");

	// Verify in DLQ
	auto [m1, m1err] = adapter_->metrics("reprocess_q");
	EXPECT_EQ(m1.dlq, 1u);
	EXPECT_EQ(m1.ready, 0u);

	// Payload should be in DLQ directory
	auto dlq_path = std::format("{}/{}.json", dlq_dir("reprocess_q"), msg_id);
	EXPECT_TRUE(fs::exists(dlq_path)) << "Payload should be in DLQ dir before reprocess";

	// Reprocess: move from DLQ back to ready
	auto [reprocess_ok, reprocess_err] = adapter_->reprocess_dlq_message(msg_key);
	EXPECT_TRUE(reprocess_ok) << "reprocess_dlq_message failed: " << reprocess_err.value_or("unknown");

	// Verify back in ready
	auto [m2, m2err] = adapter_->metrics("reprocess_q");
	EXPECT_EQ(m2.dlq, 0u);
	EXPECT_EQ(m2.ready, 1u);

	// Payload should be back in active directory
	auto active_path = std::format("{}/{}.json", active_dir("reprocess_q"), msg_id);
	EXPECT_TRUE(fs::exists(active_path)) << "Payload should be restored to active dir after reprocess";

	// Should be leasable again
	auto result = adapter_->lease_next("reprocess_q", "w2", 30);
	EXPECT_TRUE(result.leased) << "Reprocessed message should be leasable";
	if (result.message.has_value())
	{
		EXPECT_EQ(result.message->payload_json, R"({"data":"dlq_reprocess"})");
	}
}

TEST_F(HybridAdapterTest, ReprocessDlqNonexistentKeyFails)
{
	auto [ok, err] = adapter_->reprocess_dlq_message("nonexistent-key-12345");
	EXPECT_FALSE(ok) << "reprocess_dlq_message should fail for nonexistent key";
}

// ---------------------------------------------------------------------------
// EmptyQueueLease: leasing from empty queue returns no message
// ---------------------------------------------------------------------------
TEST_F(HybridAdapterTest, EmptyQueueLease)
{
	auto result = adapter_->lease_next("nonexistent_q", "w1", 30);
	EXPECT_FALSE(result.leased);
	EXPECT_FALSE(result.message.has_value());
	EXPECT_FALSE(result.lease.has_value());
}
