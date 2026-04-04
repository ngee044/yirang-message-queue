#include "TestHelpers.h"

#include "FileSystemAdapter.h"
#include "HybridAdapter.h"
#include "SQLiteAdapter.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

namespace
{
	auto find_schema_path() -> std::string
	{
		namespace fs = std::filesystem;

		auto candidate = fs::current_path() / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return candidate.string();
		}

		candidate = fs::current_path() / "build" / "out" / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return candidate.string();
		}

		candidate = fs::current_path() / ".." / "out" / "sqlite_schema.sql";
		if (fs::exists(candidate))
		{
			return candidate.string();
		}

		return "sqlite_schema.sql";
	}
}

// ============================================================
// Backend Contract Test (P0-4)
//
// Verifies that all 3 backends (SQLite, FileSystem, Hybrid)
// behave identically for the same operations.
// Related: FR-STR-03, TECH_DEBT P0-4
// ============================================================

enum class TestBackendType
{
	SQLite,
	FileSystem,
	Hybrid
};

class BackendContractTest : public ::testing::TestWithParam<TestBackendType>
{
protected:
	void SetUp() override
	{
		init_test_logger();
		temp_dir_ = std::make_unique<TempDir>("contract_test_");

		auto type = GetParam();
		switch (type)
		{
		case TestBackendType::SQLite:
		{
			auto schema = find_schema_path();
			auto adapter = std::make_unique<SQLiteAdapter>(schema);
			config_ = make_sqlite_config(temp_dir_->path());
			backend_ = std::move(adapter);
			break;
		}
		case TestBackendType::FileSystem:
		{
			auto adapter = std::make_unique<FileSystemAdapter>();
			config_ = make_fs_config(temp_dir_->path());
			backend_ = std::move(adapter);
			break;
		}
		case TestBackendType::Hybrid:
		{
			auto schema = find_schema_path();
			auto adapter = std::make_unique<HybridAdapter>(schema);
			config_ = make_hybrid_config(temp_dir_->path());
			backend_ = std::move(adapter);
			break;
		}
		}

		auto [ok, error] = backend_->open(config_);
		ASSERT_TRUE(ok) << error.value_or("unknown error");
	}

	void TearDown() override
	{
		if (backend_)
		{
			backend_->close();
		}
	}

	std::unique_ptr<TempDir> temp_dir_;
	std::unique_ptr<BackendAdapter> backend_;
	BackendConfig config_;
};

// ============================================================
// TC-CONTRACT-01: Enqueue and Lease
// ============================================================
TEST_P(BackendContractTest, EnqueueAndLeaseNext)
{
	auto envelope = make_envelope("contract-q", R"({"test":"enqueue-lease"})");

	auto [ok, error] = backend_->enqueue(envelope);
	ASSERT_TRUE(ok) << error.value_or("");

	auto result = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(result.leased);
	ASSERT_TRUE(result.message.has_value());
	ASSERT_TRUE(result.lease.has_value());
	EXPECT_EQ(result.message->queue, "contract-q");
	EXPECT_FALSE(result.lease->lease_id.empty());
	EXPECT_FALSE(result.lease->message_key.empty());
}

// ============================================================
// TC-CONTRACT-02: ACK deletes message
// ============================================================
TEST_P(BackendContractTest, AckDeletesMessage)
{
	auto envelope = make_envelope("contract-q", R"({"test":"ack"})");
	backend_->enqueue(envelope);

	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(lease.leased);

	auto [ok, error] = backend_->ack(lease.lease.value());
	ASSERT_TRUE(ok) << error.value_or("");

	// Queue should be empty now
	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.inflight, 0u);
}

// ============================================================
// TC-CONTRACT-03: NACK with requeue
// ============================================================
TEST_P(BackendContractTest, NackWithRequeue)
{
	auto envelope = make_envelope("contract-q", R"({"test":"nack-requeue"})");
	backend_->enqueue(envelope);

	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(lease.leased);

	auto [ok, error] = backend_->nack(lease.lease.value(), "test failure", true);
	ASSERT_TRUE(ok) << error.value_or("");

	// Message should be back to ready (or delayed)
	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.inflight, 0u);
	EXPECT_GE(metrics.ready + metrics.delayed, 1u);
}

// ============================================================
// TC-CONTRACT-04: NACK without requeue → DLQ
// ============================================================
TEST_P(BackendContractTest, NackWithoutRequeueToDlq)
{
	auto envelope = make_envelope("contract-q", R"({"test":"nack-dlq"})");
	backend_->enqueue(envelope);

	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(lease.leased);

	auto [ok, error] = backend_->nack(lease.lease.value(), "permanent failure", false);
	ASSERT_TRUE(ok) << error.value_or("");

	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.inflight, 0u);
	EXPECT_EQ(metrics.dlq, 1u);
}

// ============================================================
// TC-CONTRACT-05: Target consumer filtering
// ============================================================
TEST_P(BackendContractTest, TargetConsumerFiltering)
{
	auto envelope = make_envelope("contract-q", R"({"test":"target"})", 0, "specific-worker");
	backend_->enqueue(envelope);

	// Wrong consumer should get nothing
	auto wrong = backend_->lease_next("contract-q", "other-worker", 30);
	EXPECT_FALSE(wrong.leased);

	// Correct consumer should get the message
	auto correct = backend_->lease_next("contract-q", "specific-worker", 30);
	EXPECT_TRUE(correct.leased);
}

// ============================================================
// TC-CONTRACT-06: Priority ordering
// ============================================================
TEST_P(BackendContractTest, PriorityOrdering)
{
	auto low = make_envelope("contract-q", R"({"priority":"low"})", 1);
	auto high = make_envelope("contract-q", R"({"priority":"high"})", 10);
	auto medium = make_envelope("contract-q", R"({"priority":"medium"})", 5);

	// Enqueue in random order
	backend_->enqueue(low);
	backend_->enqueue(high);
	backend_->enqueue(medium);

	// Should consume in priority DESC order: high(10), medium(5), low(1)
	auto first = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(first.leased);
	EXPECT_EQ(first.message->priority, 10);
	backend_->ack(first.lease.value());

	auto second = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(second.leased);
	EXPECT_EQ(second.message->priority, 5);
	backend_->ack(second.lease.value());

	auto third = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(third.leased);
	EXPECT_EQ(third.message->priority, 1);
	backend_->ack(third.lease.value());
}

// ============================================================
// TC-CONTRACT-07: Delayed message not visible until available
// ============================================================
TEST_P(BackendContractTest, DelayedMessageActivation)
{
	auto envelope = make_envelope("contract-q", R"({"test":"delayed"})");
	envelope.available_at_ms = envelope.created_at_ms + 60000; // 60s in the future

	backend_->enqueue(envelope);

	// Should not be leasable yet
	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	EXPECT_FALSE(lease.leased);

	// Check metrics: should be delayed
	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.delayed, 1u);
}

// ============================================================
// TC-CONTRACT-08: TTL purge
// ============================================================
TEST_P(BackendContractTest, TtlPurge)
{
	auto envelope = make_envelope("contract-q", R"({"test":"ttl"})");
	envelope.expired_at_ms = 1; // Already expired (epoch ms = 1)

	backend_->enqueue(envelope);

	auto [purged, error] = backend_->purge_expired_messages();
	EXPECT_GE(purged, 1);

	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 0u);
}

// ============================================================
// TC-CONTRACT-09: DLQ reprocess
// ============================================================
TEST_P(BackendContractTest, DlqReprocess)
{
	auto envelope = make_envelope("contract-q", R"({"test":"dlq-reprocess"})");
	backend_->enqueue(envelope);

	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(lease.leased);
	backend_->nack(lease.lease.value(), "fail", false);

	auto [metrics_before, _] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics_before.dlq, 1u);

	auto [ok, error] = backend_->reprocess_dlq_message(lease.lease->message_key);
	ASSERT_TRUE(ok) << error.value_or("");

	auto [metrics_after, __] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics_after.dlq, 0u);
	EXPECT_EQ(metrics_after.ready, 1u);
}

// ============================================================
// TC-CONTRACT-10: Metrics accuracy
// ============================================================
TEST_P(BackendContractTest, MetricsAccuracy)
{
	// Enqueue 3 messages
	backend_->enqueue(make_envelope("contract-q", R"({"n":1})"));
	backend_->enqueue(make_envelope("contract-q", R"({"n":2})"));
	backend_->enqueue(make_envelope("contract-q", R"({"n":3})"));

	auto [m1, _1] = backend_->metrics("contract-q");
	EXPECT_EQ(m1.ready, 3u);
	EXPECT_EQ(m1.inflight, 0u);

	// Lease one
	auto lease = backend_->lease_next("contract-q", "worker-1", 30);
	ASSERT_TRUE(lease.leased);

	auto [m2, _2] = backend_->metrics("contract-q");
	EXPECT_EQ(m2.ready, 2u);
	EXPECT_EQ(m2.inflight, 1u);

	// ACK
	backend_->ack(lease.lease.value());

	auto [m3, _3] = backend_->metrics("contract-q");
	EXPECT_EQ(m3.ready, 2u);
	EXPECT_EQ(m3.inflight, 0u);
}

// ============================================================
// TC-CONTRACT-11: Policy save and load
// ============================================================
TEST_P(BackendContractTest, PolicySaveLoad)
{
	QueuePolicy policy;
	policy.visibility_timeout_sec = 60;
	policy.ttl_sec = 3600;
	policy.retry.limit = 3;
	policy.retry.backoff = "exponential";
	policy.retry.initial_delay_sec = 2;
	policy.retry.max_delay_sec = 120;
	policy.dlq.enabled = true;
	policy.dlq.queue = "test-dlq";
	policy.dlq.retention_days = 7;

	auto [save_ok, save_error] = backend_->save_policy("contract-q", policy);
	ASSERT_TRUE(save_ok) << save_error.value_or("");

	auto [loaded, load_error] = backend_->load_policy("contract-q");
	ASSERT_TRUE(loaded.has_value()) << load_error.value_or("");

	EXPECT_EQ(loaded->visibility_timeout_sec, 60);
	EXPECT_EQ(loaded->ttl_sec, 3600);
	EXPECT_EQ(loaded->retry.limit, 3);
	EXPECT_EQ(loaded->retry.backoff, "exponential");
	EXPECT_EQ(loaded->dlq.enabled, true);
}

// ============================================================
// TC-CONTRACT-12: Empty queue lease returns empty
// ============================================================
TEST_P(BackendContractTest, EmptyQueueLeaseReturnsEmpty)
{
	auto result = backend_->lease_next("nonexistent-queue", "worker-1", 30);
	EXPECT_FALSE(result.leased);
	EXPECT_FALSE(result.message.has_value());
}

// ============================================================
// TC-CONTRACT-13: Batch enqueue
// ============================================================
TEST_P(BackendContractTest, BatchEnqueue)
{
	std::vector<MessageEnvelope> messages;
	for (int i = 0; i < 5; ++i)
	{
		messages.push_back(make_envelope("contract-q", std::format(R"({{"batch":{}}})", i)));
	}

	auto [count, error] = backend_->batch_enqueue(messages);
	EXPECT_EQ(count, 5);

	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 5u);
}

// ============================================================
// TC-CONTRACT-14: Batch lease next
// ============================================================
TEST_P(BackendContractTest, BatchLeaseNext)
{
	for (int i = 0; i < 3; ++i)
	{
		backend_->enqueue(make_envelope("contract-q", std::format(R"({{"n":{}}})", i)));
	}

	auto [results, error] = backend_->batch_lease_next("contract-q", "worker-1", 30, 10);
	EXPECT_EQ(static_cast<int32_t>(results.size()), 3);

	for (const auto& r : results)
	{
		EXPECT_TRUE(r.leased);
	}

	auto [metrics, m_error] = backend_->metrics("contract-q");
	EXPECT_EQ(metrics.ready, 0u);
	EXPECT_EQ(metrics.inflight, 3u);
}

// ============================================================
// Instantiate tests for all 3 backends
// ============================================================
INSTANTIATE_TEST_SUITE_P(
	AllBackends,
	BackendContractTest,
	::testing::Values(
		TestBackendType::SQLite,
		TestBackendType::FileSystem,
		TestBackendType::Hybrid
	),
	[](const ::testing::TestParamInfo<TestBackendType>& info) -> std::string
	{
		switch (info.param)
		{
		case TestBackendType::SQLite: return "SQLite";
		case TestBackendType::FileSystem: return "FileSystem";
		case TestBackendType::Hybrid: return "Hybrid";
		default: return "Unknown";
		}
	}
);
