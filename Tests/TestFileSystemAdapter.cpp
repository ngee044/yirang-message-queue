#include "TestHelpers.h"
#include "FileSystemAdapter.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

class FileSystemAdapterTest : public ::testing::Test
{
protected:
	std::unique_ptr<TempDir> temp_dir_;
	std::unique_ptr<FileSystemAdapter> adapter_;

	void SetUp() override
	{
		init_test_logger();

		temp_dir_ = std::make_unique<TempDir>("fs_adapter_test");
		adapter_ = std::make_unique<FileSystemAdapter>();

		auto config = make_fs_config(temp_dir_->path());
		auto [ok, err] = adapter_->open(config);
		ASSERT_TRUE(ok) << "Failed to open adapter: " << err.value_or("unknown");
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
};

// ---------------------------------------------------------------------------
// OpenClose: directories are created after open, type mismatch error
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, OpenClose)
{
	auto root = temp_dir_->path() + "/fs";
	// open() creates root and meta directories only
	EXPECT_TRUE(fs::exists(root));
	EXPECT_TRUE(fs::exists(root + "/meta"));

	// Per-queue directories are created lazily on first enqueue
	// Directory structure: root/{queue}/{inbox|processing|archive|dlq|delayed}
	auto env = make_envelope("open_close_q", R"({"data":"test"})");
	adapter_->enqueue(env);
	EXPECT_TRUE(fs::exists(root + "/open_close_q/inbox"));
	EXPECT_TRUE(fs::exists(root + "/open_close_q/processing"));
	EXPECT_TRUE(fs::exists(root + "/open_close_q/archive"));
	EXPECT_TRUE(fs::exists(root + "/open_close_q/dlq"));
}

// ---------------------------------------------------------------------------
// EnqueueAndLease: enqueue creates file, lease returns message
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, EnqueueAndLease)
{
	auto env = make_envelope("telemetry", R"({"temp":36.5})");
	auto original_key = env.key;

	auto [ok, err] = adapter_->enqueue(env);
	ASSERT_TRUE(ok) << "enqueue failed: " << err.value_or("unknown");

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
}

// ---------------------------------------------------------------------------
// EnqueueMultipleAndLease: order by priority
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, EnqueueMultipleAndLease)
{
	// FileSystemAdapter does not sort by priority — it returns messages
	// in directory listing order (typically alphabetical by filename).
	// This test verifies all 3 messages are leased successfully.
	auto env1 = make_envelope("events", R"({"type":"first"})");
	auto env2 = make_envelope("events", R"({"type":"second"})");
	auto env3 = make_envelope("events", R"({"type":"third"})");

	adapter_->enqueue(env1);
	adapter_->enqueue(env2);
	adapter_->enqueue(env3);

	std::vector<std::string> leased_payloads;
	for (int i = 0; i < 3; i++)
	{
		auto r = adapter_->lease_next("events", "w1", 30);
		ASSERT_TRUE(r.leased) << "Lease " << i << " failed";
		ASSERT_TRUE(r.message.has_value());
		leased_payloads.push_back(r.message->payload_json);
	}
	EXPECT_EQ(leased_payloads.size(), 3u);

	// Fourth lease should fail (no more messages)
	auto r4 = adapter_->lease_next("events", "w1", 30);
	EXPECT_FALSE(r4.leased);
}

// ---------------------------------------------------------------------------
// AckMovesToArchive: ack moves file from processing to archive
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, AckMovesToArchive)
{
	auto env = make_envelope("jobs", R"({"action":"build"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("jobs", "w1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [aok, aerr] = adapter_->ack(result.lease.value());
	EXPECT_TRUE(aok) << "ack failed: " << aerr.value_or("unknown");

	// Should not be leasable anymore
	auto result2 = adapter_->lease_next("jobs", "w1", 30);
	EXPECT_FALSE(result2.leased) << "Message should be deleted after ack";
}

// ---------------------------------------------------------------------------
// NackRequeue: nack with requeue returns to inbox
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, NackRequeue)
{
	auto env = make_envelope("tasks", R"({"step":"validate"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("tasks", "w1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(result.lease.value(), "transient failure", true);
	EXPECT_TRUE(nok) << "nack requeue failed: " << nerr.value_or("unknown");

	// Should be able to lease it again
	auto result2 = adapter_->lease_next("tasks", "w2", 30);
	ASSERT_TRUE(result2.leased);
	ASSERT_TRUE(result2.message.has_value());
	EXPECT_EQ(result2.message->payload_json, R"({"step":"validate"})");
}

// ---------------------------------------------------------------------------
// NackToDlq: nack without requeue moves to dlq
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, NackToDlq)
{
	auto env = make_envelope("orders", R"({"id":"ORD-001"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("orders", "w1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [nok, nerr] = adapter_->nack(result.lease.value(), "permanent failure", false);
	EXPECT_TRUE(nok) << "nack to dlq failed: " << nerr.value_or("unknown");

	// Should not be able to lease anything from the main queue
	auto result2 = adapter_->lease_next("orders", "w1", 30);
	EXPECT_FALSE(result2.leased);
}

// ---------------------------------------------------------------------------
// ExtendLease: extend lease updates expiry
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, ExtendLease)
{
	auto env = make_envelope("work", R"({"task":"long_run"})");
	adapter_->enqueue(env);

	auto result = adapter_->lease_next("work", "w1", 5);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	auto [eok, eerr] = adapter_->extend_lease(result.lease.value(), 60);
	EXPECT_TRUE(eok) << "extend_lease failed: " << eerr.value_or("unknown");
}

// ---------------------------------------------------------------------------
// Metrics: file count based metrics
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, Metrics)
{
	// Empty queue
	auto [m0, m0err] = adapter_->metrics("stats_q");
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
	auto result = adapter_->lease_next("stats_q", "w1", 30);
	ASSERT_TRUE(result.leased);

	auto [m2, m2err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m2.ready, 2u);
	EXPECT_EQ(m2.inflight, 1u);

	// Ack it
	adapter_->ack(result.lease.value());

	auto [m3, m3err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m3.ready, 2u);
	EXPECT_EQ(m3.inflight, 0u);

	// Lease and nack to DLQ
	auto result2 = adapter_->lease_next("stats_q", "w1", 30);
	ASSERT_TRUE(result2.leased);
	adapter_->nack(result2.lease.value(), "fail", false);

	auto [m4, m4err] = adapter_->metrics("stats_q");
	EXPECT_EQ(m4.ready, 1u);
	EXPECT_EQ(m4.dlq, 1u);
}

// ---------------------------------------------------------------------------
// PolicySaveLoad: save/load policy roundtrip
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, PolicySaveLoad)
{
	QueuePolicy policy;
	policy.visibility_timeout_sec = 45;
	policy.retry.limit = 5;
	policy.retry.backoff = "exponential";
	policy.retry.initial_delay_sec = 2;
	policy.retry.max_delay_sec = 120;
	policy.dlq.enabled = true;
	policy.dlq.queue = "policy_q_dlq";
	policy.dlq.retention_days = 14;

	auto [sok, serr] = adapter_->save_policy("policy_q", policy);
	ASSERT_TRUE(sok) << "save_policy failed: " << serr.value_or("unknown");

	auto [loaded, lerr] = adapter_->load_policy("policy_q");
	ASSERT_TRUE(loaded.has_value()) << "load_policy failed: " << lerr.value_or("unknown");

	EXPECT_EQ(loaded->visibility_timeout_sec, 45);
	EXPECT_EQ(loaded->retry.limit, 5);
	EXPECT_EQ(loaded->retry.backoff, "exponential");
	EXPECT_TRUE(loaded->dlq.enabled);
	EXPECT_EQ(loaded->dlq.queue, "policy_q_dlq");
	EXPECT_EQ(loaded->dlq.retention_days, 14);
}

// ---------------------------------------------------------------------------
// RecoverExpiredLeases: lease a message, wait, recover
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, RecoverExpiredLeases)
{
	auto env = make_envelope("recover_q", R"({"data":"recover_me"})");
	adapter_->enqueue(env);

	// Lease with very short timeout (1 second)
	auto result = adapter_->lease_next("recover_q", "w1", 1);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.lease.has_value());

	// Wait for lease to expire
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	// Now recover should find the expired lease
	auto [count, rerr] = adapter_->recover_expired_leases();
	EXPECT_GE(count, 1);

	// Message should be leasable again
	auto result2 = adapter_->lease_next("recover_q", "w2", 30);
	ASSERT_TRUE(result2.leased);
	ASSERT_TRUE(result2.message.has_value());
	EXPECT_EQ(result2.message->payload_json, R"({"data":"recover_me"})");
}

// ---------------------------------------------------------------------------
// ProcessDelayedMessages: enqueue with delay, process
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, ProcessDelayedMessages)
{
	auto env = make_envelope("delay_q", R"({"data":"delayed"})");
	adapter_->enqueue(env);

	// Must lease first to create lease meta (required by delay_message)
	auto lease_result = adapter_->lease_next("delay_q", "w1", 30);
	ASSERT_TRUE(lease_result.leased);

	// Now delay the message (moves from processing to delayed)
	auto [dok, derr] = adapter_->delay_message(env.key, 500);
	ASSERT_TRUE(dok) << "delay_message failed: " << derr.value_or("unknown");

	// Process delayed - should not be ready yet
	auto [pcount0, perr0] = adapter_->process_delayed_messages();

	// Wait for delay to pass
	std::this_thread::sleep_for(std::chrono::milliseconds(700));

	// Now process should move it back to ready
	auto [pcount, perr] = adapter_->process_delayed_messages();
	EXPECT_GE(pcount, 1);

	// Should be leasable again
	auto result = adapter_->lease_next("delay_q", "w2", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.message.has_value());
	EXPECT_EQ(result.message->payload_json, R"({"data":"delayed"})");
}

// ---------------------------------------------------------------------------
// MoveToDlq: directly move a message to DLQ
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, MoveToDlq)
{
	auto env = make_envelope("dlq_test_q", R"({"data":"poison"})");
	auto msg_key = env.key;
	adapter_->enqueue(env);

	// Must lease first to create lease meta (required by move_to_dlq)
	auto lease_result = adapter_->lease_next("dlq_test_q", "w1", 30);
	ASSERT_TRUE(lease_result.leased);

	auto [dok, derr] = adapter_->move_to_dlq(msg_key, "poison pill");
	EXPECT_TRUE(dok) << "move_to_dlq failed: " << derr.value_or("unknown");

	// DLQ metric should reflect the move
	auto [m, merr] = adapter_->metrics("dlq_test_q");
	EXPECT_EQ(m.dlq, 1u);
	EXPECT_EQ(m.ready, 0u);
}

// ---------------------------------------------------------------------------
// TargetConsumerId: only targeted consumer can lease
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ReprocessDlq: move DLQ message back to inbox
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, ReprocessDlqMessageMovesBackToReady)
{
	auto env = make_envelope("reprocess_q", R"({"data":"dlq_reprocess"})");
	auto msg_key = env.key;
	adapter_->enqueue(env);

	// Lease then move to DLQ
	auto lease_result = adapter_->lease_next("reprocess_q", "w1", 30);
	ASSERT_TRUE(lease_result.leased);

	auto [move_ok, move_err] = adapter_->move_to_dlq(msg_key, "test failure");
	ASSERT_TRUE(move_ok) << "move_to_dlq failed: " << move_err.value_or("unknown");

	// Verify in DLQ
	auto [m1, m1err] = adapter_->metrics("reprocess_q");
	EXPECT_EQ(m1.dlq, 1u);
	EXPECT_EQ(m1.ready, 0u);

	// Reprocess: move from DLQ back to inbox
	auto [reprocess_ok, reprocess_err] = adapter_->reprocess_dlq_message(msg_key);
	EXPECT_TRUE(reprocess_ok) << "reprocess_dlq_message failed: " << reprocess_err.value_or("unknown");

	// Verify back in ready
	auto [m2, m2err] = adapter_->metrics("reprocess_q");
	EXPECT_EQ(m2.dlq, 0u);
	EXPECT_EQ(m2.ready, 1u);

	// Should be leasable again
	auto result = adapter_->lease_next("reprocess_q", "w2", 30);
	EXPECT_TRUE(result.leased) << "Reprocessed message should be leasable";
	if (result.message.has_value())
	{
		EXPECT_EQ(result.message->payload_json, R"({"data":"dlq_reprocess"})");
	}
}

TEST_F(FileSystemAdapterTest, ReprocessDlqNonexistentKeyFails)
{
	auto [ok, err] = adapter_->reprocess_dlq_message("nonexistent-key-12345");
	EXPECT_FALSE(ok) << "reprocess_dlq_message should fail for nonexistent key";
}

// ---------------------------------------------------------------------------
// TargetConsumerId: only targeted consumer can lease
// ---------------------------------------------------------------------------
TEST_F(FileSystemAdapterTest, TargetConsumerId)
{
	auto env = make_envelope("targeted_q", R"({"msg":"for_worker2"})", 0, "worker-02");
	adapter_->enqueue(env);

	// worker-01 should not receive this message
	auto result1 = adapter_->lease_next("targeted_q", "worker-01", 30);
	EXPECT_FALSE(result1.leased) << "worker-01 should not receive targeted message";

	// worker-02 should receive it
	auto result2 = adapter_->lease_next("targeted_q", "worker-02", 30);
	ASSERT_TRUE(result2.leased);
	ASSERT_TRUE(result2.message.has_value());
	EXPECT_EQ(result2.message->payload_json, R"({"msg":"for_worker2"})");
	EXPECT_EQ(result2.message->target_consumer_id, "worker-02");
}
