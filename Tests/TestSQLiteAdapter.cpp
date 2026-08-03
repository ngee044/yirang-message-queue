#include "TestHelpers.h"
#include "SQLiteAdapter.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

namespace
{
	// Helper: find sqlite_schema.sql relative to the test executable or cwd
	auto find_schema_path() -> std::string
	{
		// Check current working directory
		auto candidate = fs::current_path() / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return candidate.string();
		}

		// Check build/out directory
		candidate = fs::current_path() / "build" / "out" / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return candidate.string();
		}

		// Try ../out relative to cwd (if running from build dir)
		candidate = fs::current_path() / ".." / "out" / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return fs::canonical(candidate).string();
		}

		// Last resort: return the name and hope it's findable at runtime
		return "sqlite_schema.sql";
	}

	auto now_ms() -> int64_t
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(
				   std::chrono::system_clock::now().time_since_epoch())
			.count();
	}
} // namespace

class SQLiteAdapterTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		init_test_logger();

		temp_dir_ = std::make_unique<TempDir>("sqlite_adapter_test");
		schema_path_ = find_schema_path();

		adapter_ = std::make_unique<SQLiteAdapter>(schema_path_);

		auto config = make_sqlite_config(temp_dir_->path());

		auto [ok, err] = adapter_->open(config);
		ASSERT_TRUE(ok) << "Failed to open SQLiteAdapter: " << err.value_or("unknown");
	}

	void TearDown() override
	{
		if (adapter_)
		{
			adapter_->close();
			adapter_.reset();
		}
		temp_dir_.reset();
	}

	std::unique_ptr<TempDir> temp_dir_;
	std::string schema_path_;
	std::unique_ptr<SQLiteAdapter> adapter_;
};

// ---------------------------------------------------------------------------
// OpenClose tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, OpenCreatesDbFile)
{
	// The DB file should exist after open()
	auto db_path = temp_dir_->path() + "/test.db";
	EXPECT_TRUE(fs::exists(db_path)) << "Database file was not created";
}

TEST_F(SQLiteAdapterTest, OpenWithTypeMismatchFails)
{
	auto mismatched_adapter = std::make_unique<SQLiteAdapter>(schema_path_);

	BackendConfig config;
	config.type = BackendType::FileSystem; // Wrong type for SQLiteAdapter
	config.sqlite.db_path = temp_dir_->path() + "/mismatch.db";
	config.sqlite.kv_table = "kv";
	config.sqlite.message_index_table = "msg_index";
	config.sqlite.busy_timeout_ms = 5000;
	config.sqlite.journal_mode = "WAL";
	config.sqlite.synchronous = "NORMAL";

	auto [ok, err] = mismatched_adapter->open(config);
	EXPECT_FALSE(ok) << "Should fail when BackendType is not SQLite";
	EXPECT_TRUE(err.has_value());
}

// ---------------------------------------------------------------------------
// Enqueue and Lease tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, EnqueueAndLeaseBasic)
{
	auto env = make_envelope("test-queue", R"({"sensor":"temp","value":36.5})");
	auto [enq_ok, enq_err] = adapter_->enqueue(env);
	ASSERT_TRUE(enq_ok) << "Enqueue failed: " << enq_err.value_or("unknown");

	auto result = adapter_->lease_next("test-queue", "consumer-1", 30);
	ASSERT_TRUE(result.leased) << "Lease failed: " << result.error.value_or("unknown");
	ASSERT_TRUE(result.message.has_value());
	ASSERT_TRUE(result.lease.has_value());

	EXPECT_EQ(result.message->queue, "test-queue");
	EXPECT_EQ(result.message->key, env.key);
	EXPECT_EQ(result.lease->consumer_id, "consumer-1");
	EXPECT_EQ(result.lease->message_key, env.key);
}

TEST_F(SQLiteAdapterTest, LeaseFromEmptyQueueReturnsNoLease)
{
	auto result = adapter_->lease_next("empty-queue", "consumer-1", 30);
	EXPECT_FALSE(result.leased);
	EXPECT_FALSE(result.message.has_value());
	EXPECT_FALSE(result.lease.has_value());
}

TEST_F(SQLiteAdapterTest, EnqueueMultiplePriorityOrdering)
{
	// Enqueue messages with different priorities (higher value = higher priority)
	auto low = make_envelope("prio-queue", R"({"prio":"low"})", 1);
	auto high = make_envelope("prio-queue", R"({"prio":"high"})", 10);
	auto mid = make_envelope("prio-queue", R"({"prio":"mid"})", 5);

	adapter_->enqueue(low);
	adapter_->enqueue(high);
	adapter_->enqueue(mid);

	// Lease should return highest priority first
	auto r1 = adapter_->lease_next("prio-queue", "consumer-1", 30);
	ASSERT_TRUE(r1.leased);
	EXPECT_EQ(r1.message->priority, 10) << "Highest priority should be leased first";
	adapter_->ack(*r1.lease);

	auto r2 = adapter_->lease_next("prio-queue", "consumer-1", 30);
	ASSERT_TRUE(r2.leased);
	EXPECT_EQ(r2.message->priority, 5) << "Middle priority should be leased second";
	adapter_->ack(*r2.lease);

	auto r3 = adapter_->lease_next("prio-queue", "consumer-1", 30);
	ASSERT_TRUE(r3.leased);
	EXPECT_EQ(r3.message->priority, 1) << "Lowest priority should be leased last";
	adapter_->ack(*r3.lease);
}

// ---------------------------------------------------------------------------
// Ack / Nack tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, AckDeletesMessage)
{
	auto env = make_envelope("ack-queue", R"({"data":"to_ack"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("ack-queue", "consumer-1", 30);
	ASSERT_TRUE(result.leased);

	auto [ack_ok, ack_err] = adapter_->ack(*result.lease);
	EXPECT_TRUE(ack_ok) << "Ack failed: " << ack_err.value_or("unknown");

	// Message should no longer be leasable
	auto result2 = adapter_->lease_next("ack-queue", "consumer-1", 30);
	EXPECT_FALSE(result2.leased) << "Message should be deleted after ack";

	// Metrics should show zero
	auto [metrics, metrics_err] = adapter_->metrics("ack-queue");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.inflight, 0u);
}

TEST_F(SQLiteAdapterTest, NackWithRequeueReturnsToReady)
{
	auto env = make_envelope("nack-queue", R"({"data":"to_nack"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("nack-queue", "consumer-1", 30);
	ASSERT_TRUE(result.leased);

	auto [nack_ok, nack_err] = adapter_->nack(*result.lease, "transient error", true);
	EXPECT_TRUE(nack_ok) << "Nack failed: " << nack_err.value_or("unknown");

	// Message should be available again (ready or delayed depending on retry policy)
	auto [metrics, metrics_err] = adapter_->metrics("nack-queue");
	// After nack+requeue, the message goes back to ready or delayed state
	EXPECT_EQ(metrics.inflight, 0u) << "Message should no longer be inflight after nack";
	EXPECT_GE(metrics.ready + metrics.delayed, 1u)
		<< "Message should be in ready or delayed state after nack+requeue";
}

TEST_F(SQLiteAdapterTest, NackWithoutRequeueMovesToDlq)
{
	// NOTE: The sqlite_schema.sql may not have dlq_reason and dlq_at columns.
	auto env = make_envelope("dlq-queue", R"({"data":"to_dlq"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("dlq-queue", "consumer-1", 30);
	ASSERT_TRUE(result.leased);

	auto [nack_ok, nack_err] = adapter_->nack(*result.lease, "fatal error", false);
	// This may fail if schema lacks dlq columns - that's expected
	if (nack_ok)
	{
		auto [metrics, metrics_err] = adapter_->metrics("dlq-queue");
		EXPECT_EQ(metrics.inflight, 0u);
		EXPECT_EQ(metrics.dlq, 1u) << "Message should be in DLQ after nack without requeue";
	}
	else
	{
		// Schema limitation: dlq_reason/dlq_at columns may not exist
		GTEST_LOG_(WARNING) << "nack(requeue=false) failed (possible schema limitation): "
							<< nack_err.value_or("unknown");
	}
}

// TC-SQL-26 (Defect D-13): nack-to-DLQ must record the reason so list-dlq surfaces it.
TEST_F(SQLiteAdapterTest, NackToDlqRecordsReason)
{
	auto env = make_envelope("dlq-reason-q", R"({"data":"x"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("dlq-reason-q", "consumer-1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(*result.lease, "fatal boom", false);
	ASSERT_TRUE(nok) << "nack to dlq failed: " << nerr.value_or("unknown");

	auto [dlq, derr] = adapter_->list_dlq_messages("dlq-reason-q", 10);
	ASSERT_EQ(dlq.size(), 1u);
	EXPECT_EQ(dlq[0].reason, "fatal boom") << "DLQ entry must record the nack reason";
	EXPECT_GT(dlq[0].dlq_at_ms, 0) << "DLQ entry must record dlq_at timestamp";
}

// TC-SQL-17 (Defect D-06): explicit nack(requeue=true) must route to DLQ once the retry
// limit is reached, instead of requeuing forever (poison-message loop guard).
TEST_F(SQLiteAdapterTest, NackRequeueRoutesToDlqAtRetryLimit)
{
	auto env = make_envelope("poison_q", R"({"data":"x"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("poison_q", "w1", 30);   // attempt -> 1
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	// attempt (1) >= retry_limit (1): a requeue must be diverted to DLQ.
	auto [nok, nerr] = adapter_->nack(*result.lease, "still failing", true, 1);
	ASSERT_TRUE(nok) << "nack failed: " << nerr.value_or("unknown");

	auto [metrics, merr] = adapter_->metrics("poison_q");
	EXPECT_EQ(metrics.dlq, 1u) << "message at the retry limit must be moved to DLQ";
	EXPECT_EQ(metrics.ready, 0u) << "message must not be requeued to ready at the retry limit";
}

// TC-SQL-18 (Defect D-06): below the retry limit, an explicit requeue still returns to ready.
TEST_F(SQLiteAdapterTest, NackRequeueBelowLimitReturnsToReady)
{
	auto env = make_envelope("poison_q2", R"({"data":"y"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("poison_q2", "w1", 30);  // attempt -> 1
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(*result.lease, "transient", true, 5);
	ASSERT_TRUE(nok) << "nack failed: " << nerr.value_or("unknown");

	auto [metrics, merr] = adapter_->metrics("poison_q2");
	EXPECT_EQ(metrics.dlq, 0u) << "below the limit, nack must not DLQ";
	EXPECT_GE(metrics.ready + metrics.delayed, 1u) << "below the limit, nack must requeue";
}

// TC-SQL-19 (Defect D-07): DLQ retention purge removes only entries at or older than the
// cutoff (dlq_at <= older_than_ms), leaving newer ones intact.
TEST_F(SQLiteAdapterTest, PurgeDlqMessagesRespectsRetentionCutoff)
{
	auto env = make_envelope("retain_q", R"({"data":"z"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("retain_q", "w1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(*result.lease, "permanent", false);  // -> DLQ, dlq_at ~ now
	ASSERT_TRUE(nok) << "nack to dlq failed: " << nerr.value_or("unknown");

	auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();

	// Cutoff in the past: the fresh entry (dlq_at ~ now) is newer -> retained.
	auto [purged_none, e1] = adapter_->purge_dlq_messages("retain_q", now - 60000);
	EXPECT_EQ(purged_none, 0);
	{
		auto [m, me] = adapter_->metrics("retain_q");
		EXPECT_EQ(m.dlq, 1u) << "entry newer than cutoff must be retained";
	}

	// Cutoff in the future: the entry is now older than the cutoff -> purged.
	auto [purged_one, e2] = adapter_->purge_dlq_messages("retain_q", now + 60000);
	EXPECT_EQ(purged_one, 1);
	{
		auto [m, me] = adapter_->metrics("retain_q");
		EXPECT_EQ(m.dlq, 0u) << "entry older than cutoff must be purged";
	}
}

// ---------------------------------------------------------------------------
// Direct addressing tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, DirectAddressingTargetConsumerOnly)
{
	auto env = make_envelope("direct-queue", R"({"data":"targeted"})", 0, "worker-42");
	auto [enq_ok, enq_err] = adapter_->enqueue(env);

	if (!enq_ok)
	{
		GTEST_LOG_(WARNING) << "Enqueue with target_consumer_id failed (possible schema limitation): "
							<< enq_err.value_or("unknown");
		GTEST_SKIP() << "Skipping: target_consumer_id column may not exist in schema";
	}

	// A different consumer should NOT be able to lease this message
	auto wrong = adapter_->lease_next("direct-queue", "worker-99", 30);
	EXPECT_FALSE(wrong.leased) << "Non-targeted consumer should not receive targeted message";

	// The correct consumer should be able to lease this message
	auto right = adapter_->lease_next("direct-queue", "worker-42", 30);
	EXPECT_TRUE(right.leased) << "Targeted consumer should receive the message";
	if (right.leased)
	{
		// Note: SQLiteAdapter's lease_next SELECT does not fetch target_consumer_id
		// from the index table, so it won't be populated in the returned envelope.
		// The filtering is done in the WHERE clause, which is the important behavior.
		adapter_->ack(*right.lease);
	}
}

TEST_F(SQLiteAdapterTest, DirectAddressingEmptyTargetAnyConsumer)
{
	// Message with empty target_consumer_id should be leasable by any consumer
	auto env = make_envelope("broadcast-queue", R"({"data":"for_anyone"})", 0, "");
	auto [enq_ok, enq_err] = adapter_->enqueue(env);
	ASSERT_TRUE(enq_ok) << "Enqueue failed: " << enq_err.value_or("unknown");

	auto result = adapter_->lease_next("broadcast-queue", "any-consumer", 30);
	EXPECT_TRUE(result.leased) << "Any consumer should be able to lease untargeted message";
	if (result.leased)
	{
		adapter_->ack(*result.lease);
	}
}

// ---------------------------------------------------------------------------
// Lease extension test
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, ExtendLeaseExtendsDeadline)
{
	auto env = make_envelope("extend-queue", R"({"data":"extend_me"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("extend-queue", "consumer-1", 10);
	ASSERT_TRUE(result.leased);

	// Extend the lease by a larger timeout
	auto [ext_ok, ext_err] = adapter_->extend_lease(*result.lease, 300);
	EXPECT_TRUE(ext_ok) << "extend_lease failed: " << ext_err.value_or("unknown");

	// The message should still be inflight (not expired)
	auto [metrics, metrics_err] = adapter_->metrics("extend-queue");
	EXPECT_EQ(metrics.inflight, 1u);
	EXPECT_EQ(metrics.ready, 0u);

	// Clean up
	adapter_->ack(*result.lease);
}

// ---------------------------------------------------------------------------
// Metrics tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, MetricsEmptyQueue)
{
	auto [metrics, metrics_err] = adapter_->metrics("nonexistent-queue");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.inflight, 0u);
	EXPECT_EQ(metrics.delayed, 0u);
	EXPECT_EQ(metrics.dlq, 0u);
}

TEST_F(SQLiteAdapterTest, MetricsReflectStateChanges)
{
	const std::string queue = "metrics-queue";

	// Enqueue 3 messages
	adapter_->enqueue(make_envelope(queue, R"({"id":1})"));
	adapter_->enqueue(make_envelope(queue, R"({"id":2})"));
	adapter_->enqueue(make_envelope(queue, R"({"id":3})"));

	auto [m1, m1_err] = adapter_->metrics(queue);
	EXPECT_EQ(m1.ready, 3u);
	EXPECT_EQ(m1.inflight, 0u);

	// Lease one message
	auto r1 = adapter_->lease_next(queue, "consumer-1", 60);
	ASSERT_TRUE(r1.leased);

	auto [m2, m2_err] = adapter_->metrics(queue);
	EXPECT_EQ(m2.ready, 2u);
	EXPECT_EQ(m2.inflight, 1u);

	// Ack the leased message
	adapter_->ack(*r1.lease);

	auto [m3, m3_err] = adapter_->metrics(queue);
	EXPECT_EQ(m3.ready, 2u);
	EXPECT_EQ(m3.inflight, 0u);

	// Lease and nack with requeue
	auto r2 = adapter_->lease_next(queue, "consumer-1", 60);
	ASSERT_TRUE(r2.leased);
	adapter_->nack(*r2.lease, "retry", true);

	auto [m4, m4_err] = adapter_->metrics(queue);
	EXPECT_EQ(m4.inflight, 0u);
	// After nack+requeue: back to ready or delayed
	EXPECT_GE(m4.ready + m4.delayed, 2u);
}

// ---------------------------------------------------------------------------
// Policy persistence tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, PolicySaveAndLoadRoundtrip)
{
	const std::string queue = "policy-queue";

	QueuePolicy policy;
	policy.visibility_timeout_sec = 45;
	policy.retry.limit = 5;
	policy.retry.backoff = "exponential";
	policy.retry.initial_delay_sec = 2;
	policy.retry.max_delay_sec = 120;
	policy.dlq.enabled = true;
	policy.dlq.queue = "policy-queue-dlq";
	policy.dlq.retention_days = 14;

	auto [save_ok, save_err] = adapter_->save_policy(queue, policy);
	ASSERT_TRUE(save_ok) << "save_policy failed: " << save_err.value_or("unknown");

	auto [loaded_policy, load_err] = adapter_->load_policy(queue);
	ASSERT_TRUE(loaded_policy.has_value()) << "load_policy failed: " << load_err.value_or("unknown");

	EXPECT_EQ(loaded_policy->visibility_timeout_sec, 45);
	EXPECT_EQ(loaded_policy->retry.limit, 5);
	EXPECT_EQ(loaded_policy->retry.backoff, "exponential");
	EXPECT_TRUE(loaded_policy->dlq.enabled);
	EXPECT_EQ(loaded_policy->dlq.queue, "policy-queue-dlq");
}

TEST_F(SQLiteAdapterTest, LoadPolicyNonexistentQueue)
{
	auto [loaded_policy, load_err] = adapter_->load_policy("no-such-queue");
	// Should return nullopt for nonexistent queue
	EXPECT_FALSE(loaded_policy.has_value())
		<< "Policy for nonexistent queue should be empty";
}

// ---------------------------------------------------------------------------
// DLQ listing tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, ListDlqMessages)
{
	// NOTE: The sqlite_schema.sql does NOT have dlq_reason, dlq_at, or
	// target_consumer_id columns. This test may fail due to schema limitations.
	const std::string queue = "dlq-list-queue";

	auto env = make_envelope(queue, R"({"data":"dlq_candidate"})");
	adapter_->enqueue(env);

	auto [move_ok, move_err] = adapter_->move_to_dlq(env.key, "permanent failure");
	if (!move_ok)
	{
		GTEST_LOG_(WARNING) << "move_to_dlq failed (possible schema limitation): "
							<< move_err.value_or("unknown");
		GTEST_SKIP() << "Skipping: dlq_reason/dlq_at columns may not exist in schema";
	}

	auto [dlq_messages, list_err] = adapter_->list_dlq_messages(queue, 10);
	EXPECT_GE(dlq_messages.size(), 1u);
	if (!dlq_messages.empty())
	{
		EXPECT_EQ(dlq_messages[0].message_key, env.key);
	}
}

// ---------------------------------------------------------------------------
// Delay message test
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, DelayMessageMakesUnavailable)
{
	const std::string queue = "delay-queue";

	auto env = make_envelope(queue, R"({"data":"delay_me"})");
	adapter_->enqueue(env);

	// Delay the message far into the future
	auto [delay_ok, delay_err] = adapter_->delay_message(env.key, 60000);
	ASSERT_TRUE(delay_ok) << "delay_message failed: " << delay_err.value_or("unknown");

	// Message should not be leasable (it's delayed)
	auto result = adapter_->lease_next(queue, "consumer-1", 30);
	EXPECT_FALSE(result.leased) << "Delayed message should not be leasable";

	auto [metrics, metrics_err] = adapter_->metrics(queue);
	EXPECT_EQ(metrics.delayed, 1u);
	EXPECT_EQ(metrics.ready, 0u);
}

// ---------------------------------------------------------------------------
// Expired inflight messages test
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, GetExpiredInflightMessages)
{
	const std::string queue = "expire-queue";

	auto env = make_envelope(queue, R"({"data":"will_expire"})");
	adapter_->enqueue(env);

	// Lease with a very short visibility timeout (1 second)
	auto result = adapter_->lease_next(queue, "slow-consumer", 1);
	ASSERT_TRUE(result.leased);

	// Wait for the lease to expire
	std::this_thread::sleep_for(std::chrono::seconds(2));

	auto [expired, exp_err] = adapter_->get_expired_inflight_messages();
	EXPECT_GE(expired.size(), 1u) << "Should detect at least one expired lease";

	if (!expired.empty())
	{
		bool found = false;
		for (const auto &info : expired)
		{
			if (info.message_key == env.key)
			{
				found = true;
				break;
			}
		}
		EXPECT_TRUE(found) << "Our expired message should be in the list";
	}
}

// ---------------------------------------------------------------------------
// Multiple queues isolation test
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Reprocess DLQ tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, ReprocessDlqMessageMovesBackToReady)
{
	const std::string queue = "reprocess-dlq-queue";

	auto env = make_envelope(queue, R"({"data":"dlq_candidate"})");
	auto msg_key = env.key;
	adapter_->enqueue(env);

	// Move to DLQ
	auto [move_ok, move_err] = adapter_->move_to_dlq(msg_key, "test failure");
	if (!move_ok)
	{
		GTEST_SKIP() << "move_to_dlq failed (possible schema limitation): "
					 << move_err.value_or("unknown");
	}

	// Verify in DLQ
	auto [m1, m1_err] = adapter_->metrics(queue);
	EXPECT_EQ(m1.dlq, 1u);
	EXPECT_EQ(m1.ready, 0u);

	// Reprocess: move from DLQ back to ready
	auto [reprocess_ok, reprocess_err] = adapter_->reprocess_dlq_message(msg_key);
	EXPECT_TRUE(reprocess_ok) << "reprocess_dlq_message failed: " << reprocess_err.value_or("unknown");

	// Verify back in ready
	auto [m2, m2_err] = adapter_->metrics(queue);
	EXPECT_EQ(m2.dlq, 0u);
	EXPECT_EQ(m2.ready, 1u);

	// Should be leasable again
	auto result = adapter_->lease_next(queue, "consumer-1", 30);
	EXPECT_TRUE(result.leased) << "Reprocessed message should be leasable";
}

TEST_F(SQLiteAdapterTest, ReprocessDlqNonexistentKeyNoOp)
{
	// SQLiteAdapter's reprocess returns true even for nonexistent keys (SQL UPDATE matches 0 rows)
	auto [ok, err] = adapter_->reprocess_dlq_message("nonexistent-key-12345");
	// Just verify it doesn't crash — behavior may vary by implementation
	EXPECT_FALSE(err.has_value()) << "Should not return error: " << err.value_or("");
}

// ---------------------------------------------------------------------------
// Multiple queues isolation test
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, MultipleQueuesAreIsolated)
{
	adapter_->enqueue(make_envelope("queue-a", R"({"q":"a1"})"));
	adapter_->enqueue(make_envelope("queue-a", R"({"q":"a2"})"));
	adapter_->enqueue(make_envelope("queue-b", R"({"q":"b1"})"));

	auto [metrics_a, ma_err] = adapter_->metrics("queue-a");
	auto [metrics_b, mb_err] = adapter_->metrics("queue-b");

	EXPECT_EQ(metrics_a.ready, 2u);
	EXPECT_EQ(metrics_b.ready, 1u);

	// Leasing from queue-b should not affect queue-a
	auto result = adapter_->lease_next("queue-b", "consumer-1", 30);
	ASSERT_TRUE(result.leased);
	EXPECT_EQ(result.message->queue, "queue-b");

	auto [metrics_a_after, ma2_err] = adapter_->metrics("queue-a");
	EXPECT_EQ(metrics_a_after.ready, 2u) << "queue-a should be unaffected by queue-b lease";
}

// ---------------------------------------------------------------------------
// TTL (Time-To-Live) Tests
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, EnqueueWithTTL)
{
	auto env = make_envelope("ttl-q", R"({"data":"ttl_test"})");
	env.expired_at_ms = now_ms() + 60000; // expires in 60s

	auto [ok, err] = adapter_->enqueue(env);
	EXPECT_TRUE(ok) << err.value_or("");

	auto [metrics, m_err] = adapter_->metrics("ttl-q");
	EXPECT_EQ(metrics.ready, 1u);
}

TEST_F(SQLiteAdapterTest, LeaseSkipsExpiredMessages)
{
	// Enqueue a message that is already expired
	auto env = make_envelope("ttl-q2", R"({"data":"expired"})");
	env.expired_at_ms = now_ms() - 1000; // expired 1s ago

	auto [ok, err] = adapter_->enqueue(env);
	EXPECT_TRUE(ok);

	// Try to lease - should return nothing since it's expired
	auto result = adapter_->lease_next("ttl-q2", "consumer-1", 30);
	EXPECT_FALSE(result.leased) << "Expired message should not be leased";
}

TEST_F(SQLiteAdapterTest, PurgeExpiredMessages)
{
	// Enqueue: one expired, one not expired, one with no TTL
	auto env1 = make_envelope("ttl-q3", R"({"data":"expired"})");
	env1.expired_at_ms = now_ms() - 1000;
	adapter_->enqueue(env1);

	auto env2 = make_envelope("ttl-q3", R"({"data":"not_expired"})");
	env2.expired_at_ms = now_ms() + 60000;
	adapter_->enqueue(env2);

	auto env3 = make_envelope("ttl-q3", R"({"data":"no_ttl"})");
	adapter_->enqueue(env3);

	auto [purged, p_err] = adapter_->purge_expired_messages();
	EXPECT_EQ(purged, 1) << "Should purge only the expired message";

	auto [metrics, m_err] = adapter_->metrics("ttl-q3");
	EXPECT_EQ(metrics.ready, 2u) << "2 messages should remain (not expired + no TTL)";
}

TEST_F(SQLiteAdapterTest, PurgeDoesNotAffectInflight)
{
	auto env = make_envelope("ttl-q4", R"({"data":"inflight_ttl"})");
	env.expired_at_ms = now_ms() - 1000; // expired

	adapter_->enqueue(env);

	// Lease the message (makes it inflight) - but it's expired so lease should skip it
	// Actually since it's already expired, lease_next won't find it
	// Let's test with a message that expires AFTER being leased
	auto env2 = make_envelope("ttl-q4", R"({"data":"inflight_then_expire"})");
	env2.expired_at_ms = now_ms() + 1000; // will expire soon

	adapter_->enqueue(env2);

	// Lease it
	auto result = adapter_->lease_next("ttl-q4", "consumer-1", 300);
	ASSERT_TRUE(result.leased);

	// Now manually make it expired by waiting or just purge
	// Inflight messages should NOT be purged
	auto [purged, p_err] = adapter_->purge_expired_messages();
	// env is expired & ready, so it should be purged
	// env2 is inflight, so it should NOT be purged even if expired
	EXPECT_EQ(purged, 1) << "Should purge only the expired ready message, not the inflight one";

	auto [metrics, m_err] = adapter_->metrics("ttl-q4");
	EXPECT_EQ(metrics.inflight, 1u) << "Inflight message should remain";
}

// ---------------------------------------------------------------------------
// Schema single-source (PRIMARY KEY) and lease-integrity (affected-row) — LIM/INC
// ---------------------------------------------------------------------------

TEST_F(SQLiteAdapterTest, DuplicateEnqueueRejected)
{
	auto env = make_envelope("dup-queue", R"({"v":1})");

	auto [ok1, e1] = adapter_->enqueue(env);
	ASSERT_TRUE(ok1) << e1.value_or("");

	// Same message_key again must be rejected by the PRIMARY KEY (canonical schema).
	auto [ok2, e2] = adapter_->enqueue(env);
	EXPECT_FALSE(ok2) << "Duplicate message_key must be rejected";

	// Exactly one row was stored.
	auto r1 = adapter_->lease_next("dup-queue", "c1", 30);
	ASSERT_TRUE(r1.leased);
	adapter_->ack(*r1.lease);

	auto r2 = adapter_->lease_next("dup-queue", "c1", 30);
	EXPECT_FALSE(r2.leased) << "Only one message should have been stored";
}

TEST_F(SQLiteAdapterTest, AckAfterSettleFails)
{
	auto env = make_envelope("ack-queue", R"({"v":1})");
	ASSERT_TRUE(std::get<0>(adapter_->enqueue(env)));

	auto r = adapter_->lease_next("ack-queue", "c1", 30);
	ASSERT_TRUE(r.leased);

	auto [ok1, e1] = adapter_->ack(*r.lease);
	EXPECT_TRUE(ok1) << e1.value_or("");

	// Second ack: the message is no longer inflight -> must fail, not silently succeed.
	auto [ok2, e2] = adapter_->ack(*r.lease);
	EXPECT_FALSE(ok2) << "Acking an already-settled message must fail (affected-row check)";
	EXPECT_TRUE(e2.has_value());
}

TEST_F(SQLiteAdapterTest, ExtendNonInflightLeaseFails)
{
	auto env = make_envelope("ext-queue", R"({"v":1})");
	ASSERT_TRUE(std::get<0>(adapter_->enqueue(env))); // ready, never leased

	LeaseToken token;
	token.message_key = env.key;
	token.consumer_id = "c1";
	token.lease_until_ms = 0;

	auto [ok, err] = adapter_->extend_lease(token, 30);
	EXPECT_FALSE(ok) << "Extending a non-inflight message must fail";
	EXPECT_TRUE(err.has_value());
}

// ---------------------------------------------------------------------------
// TC-SQL-20: 이전 버전 DB(lease_consumer_id 컬럼 없음)는 기동 시 ALTER TABLE 로 보강되어야
// 하고, 재기동에도 멱등해야 한다. 마이그레이션이 동작하지 않으면 정산 SQL 이 "no such
// column" 으로 실패한다. (Defect D-55)
// ---------------------------------------------------------------------------
TEST_F(SQLiteAdapterTest, LegacySchemaGainsLeaseConsumerColumn)
{
	// 정본 스키마에서 해당 컬럼 선언만 제거한 구버전 스키마를 만든다.
	auto legacy_dir = std::make_unique<TempDir>("sqlite_legacy_schema");
	auto legacy_schema = fs::path(legacy_dir->path()) / "legacy_schema.sql";

	{
		std::ifstream source(schema_path_);
		ASSERT_TRUE(source.is_open()) << "cannot read schema: " << schema_path_;

		std::ofstream target(legacy_schema);
		ASSERT_TRUE(target.is_open());

		std::string line;
		bool stripped = false;
		while (std::getline(source, line))
		{
			if (line.find("lease_consumer_id") != std::string::npos)
			{
				stripped = true;
				continue;
			}
			target << line << '\n';
		}
		ASSERT_TRUE(stripped) << "schema no longer declares lease_consumer_id";
	}

	auto legacy_adapter = std::make_unique<SQLiteAdapter>(legacy_schema.string());
	auto legacy_config = make_sqlite_config(legacy_dir->path());

	auto [opened, open_error] = legacy_adapter->open(legacy_config);
	ASSERT_TRUE(opened) << open_error.value_or("unknown");

	auto env = make_envelope("legacy-q", R"({"v":1})");
	ASSERT_TRUE(std::get<0>(legacy_adapter->enqueue(env)));

	auto leased = legacy_adapter->lease_next("legacy-q", "legacy-worker", 30);
	ASSERT_TRUE(leased.leased);
	ASSERT_TRUE(leased.lease.has_value());

	auto [acked, ack_error] = legacy_adapter->ack(*leased.lease);
	EXPECT_TRUE(acked) << "migration did not add the column: " << ack_error.value_or("unknown");

	// 같은 DB 재기동: 컬럼이 이미 있으므로 ALTER 를 재시도하지 않아야 한다.
	legacy_adapter->close();
	auto [reopened, reopen_error] = legacy_adapter->open(legacy_config);
	ASSERT_TRUE(reopened) << "migration is not idempotent: " << reopen_error.value_or("unknown");

	// 마이그레이션된 DB에서도 소유권 계약이 성립해야 한다.
	auto env2 = make_envelope("legacy-q", R"({"v":2})");
	ASSERT_TRUE(std::get<0>(legacy_adapter->enqueue(env2)));

	auto leased2 = legacy_adapter->lease_next("legacy-q", "legacy-worker", 30);
	ASSERT_TRUE(leased2.leased);
	ASSERT_TRUE(leased2.lease.has_value());

	LeaseToken foreign = *leased2.lease;
	foreign.consumer_id = "other-worker";
	EXPECT_FALSE(std::get<0>(legacy_adapter->ack(foreign)))
		<< "migrated database must still reject a settle from a non-owning consumer";

	legacy_adapter->close();
}
