#include "TestHelpers.h"
#include "QueueManager.h"

#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <chrono>
#include <mutex>

// ---------------------------------------------------------------------------
// MockBackendAdapter
// ---------------------------------------------------------------------------
class MockBackendAdapter : public BackendAdapter
{
public:
	MockBackendAdapter(void) = default;
	~MockBackendAdapter(void) override = default;

	// -- tracking structures --------------------------------------------------

	struct SavePolicyRecord
	{
		std::string queue;
		QueuePolicy policy;
	};

	struct DelayMessageRecord
	{
		std::string message_key;
		int64_t delay_ms = 0;
	};

	struct MoveToDlqRecord
	{
		std::string message_key;
		std::string reason;
	};

	// -- accessors for recorded calls -----------------------------------------

	auto get_save_policy_calls(void) -> std::vector<SavePolicyRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return save_policy_calls_;
	}

	auto get_delay_message_calls(void) -> std::vector<DelayMessageRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return delay_message_calls_;
	}

	auto get_move_to_dlq_calls(void) -> std::vector<MoveToDlqRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return move_to_dlq_calls_;
	}

	auto set_expired_inflight_messages(const std::vector<ExpiredLeaseInfo>& messages) -> void
	{
		std::lock_guard<std::mutex> lock(mutex_);
		expired_inflight_messages_ = messages;
	}

	auto clear_expired_inflight_messages(void) -> void
	{
		std::lock_guard<std::mutex> lock(mutex_);
		expired_inflight_messages_.clear();
	}

	auto clear_all_records(void) -> void
	{
		std::lock_guard<std::mutex> lock(mutex_);
		save_policy_calls_.clear();
		delay_message_calls_.clear();
		move_to_dlq_calls_.clear();
	}

	// -- BackendAdapter interface ---------------------------------------------

	auto open(const BackendConfig& /*config*/) -> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto close(void) -> void override {}

	auto enqueue(const MessageEnvelope& /*message*/) -> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto lease_next(const std::string& /*queue*/, const std::string& /*consumer_id*/, const int32_t& /*visibility_timeout_sec*/)
		-> LeaseResult override
	{
		return { false, std::nullopt, std::nullopt, "no messages" };
	}

	auto ack(const LeaseToken& /*lease*/) -> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto nack(const LeaseToken& /*lease*/, const std::string& /*reason*/, const bool& /*requeue*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto extend_lease(const LeaseToken& /*lease*/, const int32_t& /*visibility_timeout_sec*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto load_policy(const std::string& /*queue*/)
		-> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>> override
	{
		return { std::nullopt, std::nullopt };
	}

	auto save_policy(const std::string& queue, const QueuePolicy& policy)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		save_policy_calls_.push_back({ queue, policy });
		return { true, std::nullopt };
	}

	auto metrics(const std::string& /*queue*/)
		-> std::tuple<QueueMetrics, std::optional<std::string>> override
	{
		return { QueueMetrics{}, std::nullopt };
	}

	auto recover_expired_leases(void)
		-> std::tuple<int32_t, std::optional<std::string>> override
	{
		return { 0, std::nullopt };
	}

	auto process_delayed_messages(void)
		-> std::tuple<int32_t, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		process_delayed_call_count_++;
		return { 0, std::nullopt };
	}

	auto get_expired_inflight_messages(void)
		-> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return { expired_inflight_messages_, std::nullopt };
	}

	auto delay_message(const std::string& message_key, int64_t delay_ms)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		delay_message_calls_.push_back({ message_key, delay_ms });
		return { true, std::nullopt };
	}

	auto move_to_dlq(const std::string& message_key, const std::string& reason)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		move_to_dlq_calls_.push_back({ message_key, reason });
		return { true, std::nullopt };
	}

	auto list_dlq_messages(const std::string& /*queue*/, int32_t /*limit*/)
		-> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>> override
	{
		return { std::vector<DlqMessageInfo>{}, std::nullopt };
	}

	auto reprocess_dlq_message(const std::string& /*message_key*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto get_process_delayed_call_count(void) -> int32_t
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return process_delayed_call_count_;
	}

private:
	mutable std::mutex mutex_;
	std::vector<SavePolicyRecord> save_policy_calls_;
	std::vector<DelayMessageRecord> delay_message_calls_;
	std::vector<MoveToDlqRecord> move_to_dlq_calls_;
	std::vector<ExpiredLeaseInfo> expired_inflight_messages_;
	int32_t process_delayed_call_count_ = 0;
};

// ---------------------------------------------------------------------------
// Test Fixture
// ---------------------------------------------------------------------------
class QueueManagerTest : public ::testing::Test
{
protected:
	auto SetUp(void) -> void override
	{
		init_test_logger();

		mock_backend_ = std::make_shared<MockBackendAdapter>();

		QueueManagerConfig config;
		config.lease_sweep_interval_ms = 50;
		config.retry_sweep_interval_ms = 50;
		config_ = config;
	}

	auto TearDown(void) -> void override
	{
		if (queue_manager_)
		{
			queue_manager_->stop();
			queue_manager_.reset();
		}
		mock_backend_.reset();
	}

	auto create_manager(void) -> std::unique_ptr<QueueManager>
	{
		return std::make_unique<QueueManager>(mock_backend_, config_);
	}

	auto make_policy(
		int32_t visibility_timeout_sec = 30,
		int32_t retry_limit = 3,
		const std::string& backoff = "exponential",
		int32_t initial_delay_sec = 1,
		int32_t max_delay_sec = 60,
		bool dlq_enabled = true,
		const std::string& dlq_queue = "test-dlq",
		int32_t retention_days = 7
	) -> QueuePolicy
	{
		QueuePolicy policy;
		policy.visibility_timeout_sec = visibility_timeout_sec;
		policy.retry.limit = retry_limit;
		policy.retry.backoff = backoff;
		policy.retry.initial_delay_sec = initial_delay_sec;
		policy.retry.max_delay_sec = max_delay_sec;
		policy.dlq.enabled = dlq_enabled;
		policy.dlq.queue = dlq_queue;
		policy.dlq.retention_days = retention_days;
		return policy;
	}

	auto wait_for_sweep(int cycles = 3) -> void
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(config_.lease_sweep_interval_ms * cycles));
	}

	std::shared_ptr<MockBackendAdapter> mock_backend_;
	std::unique_ptr<QueueManager> queue_manager_;
	QueueManagerConfig config_;
};

// ---------------------------------------------------------------------------
// Basic Tests
// ---------------------------------------------------------------------------

TEST_F(QueueManagerTest, RegisterAndGetPolicy)
{
	queue_manager_ = create_manager();

	auto policy = make_policy(30, 5, "exponential", 2, 120, true, "my-dlq", 14);
	queue_manager_->register_queue("telemetry", policy);

	auto retrieved = queue_manager_->get_policy("telemetry");
	ASSERT_TRUE(retrieved.has_value());
	EXPECT_EQ(retrieved->visibility_timeout_sec, 30);
	EXPECT_EQ(retrieved->retry.limit, 5);
	EXPECT_EQ(retrieved->retry.backoff, "exponential");
	EXPECT_EQ(retrieved->retry.initial_delay_sec, 2);
	EXPECT_EQ(retrieved->retry.max_delay_sec, 120);
	EXPECT_TRUE(retrieved->dlq.enabled);
	EXPECT_EQ(retrieved->dlq.queue, "my-dlq");
	EXPECT_EQ(retrieved->dlq.retention_days, 14);
}

TEST_F(QueueManagerTest, GetPolicyUnregistered)
{
	queue_manager_ = create_manager();

	auto policy = queue_manager_->get_policy("nonexistent-queue");
	EXPECT_FALSE(policy.has_value());
}

TEST_F(QueueManagerTest, StartAndStop)
{
	queue_manager_ = create_manager();

	auto [started, start_err] = queue_manager_->start();
	EXPECT_TRUE(started);
	EXPECT_FALSE(start_err.has_value());

	// Allow sweep workers to run at least once
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	queue_manager_->stop();
}

TEST_F(QueueManagerTest, DoubleStart)
{
	queue_manager_ = create_manager();

	auto [first_ok, first_err] = queue_manager_->start();
	EXPECT_TRUE(first_ok);
	EXPECT_FALSE(first_err.has_value());

	auto [second_ok, second_err] = queue_manager_->start();
	EXPECT_FALSE(second_ok);
	ASSERT_TRUE(second_err.has_value());
	EXPECT_EQ(second_err.value(), "already running");

	queue_manager_->stop();
}

TEST_F(QueueManagerTest, StopWithoutStart)
{
	queue_manager_ = create_manager();

	// Should not crash or throw
	EXPECT_NO_THROW(queue_manager_->stop());
}

TEST_F(QueueManagerTest, RegisterCallsSavePolicy)
{
	queue_manager_ = create_manager();

	auto policy = make_policy();
	queue_manager_->register_queue("events", policy);

	auto calls = mock_backend_->get_save_policy_calls();
	ASSERT_EQ(calls.size(), 1u);
	EXPECT_EQ(calls[0].queue, "events");
	EXPECT_EQ(calls[0].policy.visibility_timeout_sec, policy.visibility_timeout_sec);
	EXPECT_EQ(calls[0].policy.retry.limit, policy.retry.limit);
}

TEST_F(QueueManagerTest, MultipleQueues)
{
	queue_manager_ = create_manager();

	auto policy_a = make_policy(10, 3, "exponential", 1, 30, true, "dlq-a", 7);
	auto policy_b = make_policy(60, 5, "linear", 2, 120, false, "", 0);
	auto policy_c = make_policy(45, 10, "fixed", 5, 300, true, "dlq-c", 14);

	queue_manager_->register_queue("queue-a", policy_a);
	queue_manager_->register_queue("queue-b", policy_b);
	queue_manager_->register_queue("queue-c", policy_c);

	// Verify each queue has its own correct policy
	auto retrieved_a = queue_manager_->get_policy("queue-a");
	ASSERT_TRUE(retrieved_a.has_value());
	EXPECT_EQ(retrieved_a->visibility_timeout_sec, 10);
	EXPECT_EQ(retrieved_a->retry.limit, 3);
	EXPECT_EQ(retrieved_a->retry.backoff, "exponential");
	EXPECT_TRUE(retrieved_a->dlq.enabled);

	auto retrieved_b = queue_manager_->get_policy("queue-b");
	ASSERT_TRUE(retrieved_b.has_value());
	EXPECT_EQ(retrieved_b->visibility_timeout_sec, 60);
	EXPECT_EQ(retrieved_b->retry.limit, 5);
	EXPECT_EQ(retrieved_b->retry.backoff, "linear");
	EXPECT_FALSE(retrieved_b->dlq.enabled);

	auto retrieved_c = queue_manager_->get_policy("queue-c");
	ASSERT_TRUE(retrieved_c.has_value());
	EXPECT_EQ(retrieved_c->visibility_timeout_sec, 45);
	EXPECT_EQ(retrieved_c->retry.limit, 10);
	EXPECT_EQ(retrieved_c->retry.backoff, "fixed");
	EXPECT_TRUE(retrieved_c->dlq.enabled);
	EXPECT_EQ(retrieved_c->dlq.queue, "dlq-c");

	// Verify save_policy was called 3 times
	auto save_calls = mock_backend_->get_save_policy_calls();
	EXPECT_EQ(save_calls.size(), 3u);

	// Unknown queue still returns nullopt
	EXPECT_FALSE(queue_manager_->get_policy("unknown").has_value());
}

// ---------------------------------------------------------------------------
// Backoff Tests (via sweep workers with mock expired messages)
// ---------------------------------------------------------------------------

TEST_F(QueueManagerTest, BackoffExponential)
{
	queue_manager_ = create_manager();

	// Register queue with exponential backoff: initial=2s, max=60s
	auto policy = make_policy(30, 5, "exponential", 2, 60);
	queue_manager_->register_queue("test-queue", policy);

	// Set expired inflight message at attempt 1
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-1", "test-queue", 1 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// Verify delay_message was called with exponential backoff: 2 * 2^(1-1) = 2s = 2000ms
	auto delay_calls = mock_backend_->get_delay_message_calls();
	ASSERT_GE(delay_calls.size(), 1u);

	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-1")
		{
			EXPECT_EQ(call.delay_ms, 2000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-1";
}

TEST_F(QueueManagerTest, BackoffExponentialHighAttempt)
{
	queue_manager_ = create_manager();

	// Register queue with exponential backoff: initial=1s, max=30s
	auto policy = make_policy(30, 10, "exponential", 1, 30);
	queue_manager_->register_queue("test-queue", policy);

	// Set expired message at attempt 4: 1 * 2^(4-1) = 8s = 8000ms
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-exp", "test-queue", 4 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-exp")
		{
			EXPECT_EQ(call.delay_ms, 8000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-exp";
}

TEST_F(QueueManagerTest, BackoffExponentialCappedAtMax)
{
	queue_manager_ = create_manager();

	// initial=2s, max=10s, attempt=5: 2 * 2^4 = 32s -> capped to 10s
	auto policy = make_policy(30, 10, "exponential", 2, 10);
	queue_manager_->register_queue("test-queue", policy);

	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-cap", "test-queue", 5 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-cap")
		{
			EXPECT_EQ(call.delay_ms, 10000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-cap (capped at max)";
}

TEST_F(QueueManagerTest, BackoffLinear)
{
	queue_manager_ = create_manager();

	// Register queue with linear backoff: initial=3s, max=60s
	auto policy = make_policy(30, 5, "linear", 3, 60);
	queue_manager_->register_queue("test-queue", policy);

	// Set expired message at attempt 2: 3 * 2 = 6s = 6000ms
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-lin", "test-queue", 2 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-lin")
		{
			EXPECT_EQ(call.delay_ms, 6000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-lin";
}

TEST_F(QueueManagerTest, BackoffLinearCappedAtMax)
{
	queue_manager_ = create_manager();

	// initial=5s, max=10s, attempt=4: 5*4=20s -> capped to 10s
	auto policy = make_policy(30, 10, "linear", 5, 10);
	queue_manager_->register_queue("test-queue", policy);

	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-lin-cap", "test-queue", 4 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-lin-cap")
		{
			EXPECT_EQ(call.delay_ms, 10000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-lin-cap (capped at max)";
}

TEST_F(QueueManagerTest, BackoffFixed)
{
	queue_manager_ = create_manager();

	// Register queue with fixed backoff: initial=5s
	auto policy = make_policy(30, 5, "fixed", 5, 60);
	queue_manager_->register_queue("test-queue", policy);

	// Set expired message at attempt 3: fixed = 5s = 5000ms
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-fix", "test-queue", 3 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-fix")
		{
			EXPECT_EQ(call.delay_ms, 5000);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected delay_message call for msg-fix";
}

// ---------------------------------------------------------------------------
// Retry vs DLQ Tests
// ---------------------------------------------------------------------------

TEST_F(QueueManagerTest, RetryVsDlq)
{
	queue_manager_ = create_manager();

	// retry limit = 3, DLQ enabled
	auto policy = make_policy(30, 3, "fixed", 2, 60, true, "test-dlq");
	queue_manager_->register_queue("test-queue", policy);

	// attempt=3 >= limit=3 → should move to DLQ
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-dlq", "test-queue", 3 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// Verify move_to_dlq was called
	auto dlq_calls = mock_backend_->get_move_to_dlq_calls();
	bool found = false;
	for (const auto& call : dlq_calls)
	{
		if (call.message_key == "msg-dlq")
		{
			EXPECT_TRUE(call.reason.find("retry limit exceeded") != std::string::npos);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected move_to_dlq call for msg-dlq";
}

TEST_F(QueueManagerTest, RetryBelowLimit)
{
	queue_manager_ = create_manager();

	// retry limit = 5, attempt = 2 → should delay, not DLQ
	auto policy = make_policy(30, 5, "fixed", 3, 60, true, "test-dlq");
	queue_manager_->register_queue("test-queue", policy);

	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-retry", "test-queue", 2 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// Should call delay_message, not move_to_dlq
	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found_delay = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-retry")
		{
			EXPECT_EQ(call.delay_ms, 3000); // fixed 3s
			found_delay = true;
			break;
		}
	}
	EXPECT_TRUE(found_delay) << "Expected delay_message call for msg-retry";

	auto dlq_calls = mock_backend_->get_move_to_dlq_calls();
	bool found_dlq = false;
	for (const auto& call : dlq_calls)
	{
		if (call.message_key == "msg-retry")
		{
			found_dlq = true;
			break;
		}
	}
	EXPECT_FALSE(found_dlq) << "Should NOT move msg-retry to DLQ";
}

TEST_F(QueueManagerTest, DlqDisabled)
{
	queue_manager_ = create_manager();

	// retry limit = 2, DLQ disabled
	auto policy = make_policy(30, 2, "fixed", 1, 60, false, "");
	queue_manager_->register_queue("test-queue", policy);

	// attempt=5 >= limit=2, but DLQ disabled → message dropped (no move_to_dlq call)
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-nodlq", "test-queue", 5 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// Should NOT call move_to_dlq
	auto dlq_calls = mock_backend_->get_move_to_dlq_calls();
	bool found_dlq = false;
	for (const auto& call : dlq_calls)
	{
		if (call.message_key == "msg-nodlq")
		{
			found_dlq = true;
			break;
		}
	}
	EXPECT_FALSE(found_dlq) << "Should NOT move to DLQ when DLQ is disabled";

	// Should also NOT call delay_message (exceeded limit, no retry)
	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found_delay = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-nodlq")
		{
			found_delay = true;
			break;
		}
	}
	EXPECT_FALSE(found_delay) << "Should NOT delay msg-nodlq when retry limit exceeded and DLQ disabled";
}

// ---------------------------------------------------------------------------
// Sweep Worker Integration Tests
// ---------------------------------------------------------------------------

TEST_F(QueueManagerTest, NoExpiredMessagesNoAction)
{
	queue_manager_ = create_manager();

	// No expired messages set in mock
	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// No delay or DLQ calls expected
	EXPECT_EQ(mock_backend_->get_delay_message_calls().size(), 0u);
	EXPECT_EQ(mock_backend_->get_move_to_dlq_calls().size(), 0u);
}

TEST_F(QueueManagerTest, ExpiredMessageNoPolicyRecoveredImmediately)
{
	queue_manager_ = create_manager();

	// Do NOT register a queue policy — expired message without policy gets delay_message(key, 0)
	std::vector<ExpiredLeaseInfo> expired;
	expired.push_back({ "msg-nopol", "unregistered-queue", 1 });
	mock_backend_->set_expired_inflight_messages(expired);

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	auto delay_calls = mock_backend_->get_delay_message_calls();
	bool found = false;
	for (const auto& call : delay_calls)
	{
		if (call.message_key == "msg-nopol")
		{
			EXPECT_EQ(call.delay_ms, 0);
			found = true;
			break;
		}
	}
	EXPECT_TRUE(found) << "Expected immediate recovery (delay_ms=0) for message without policy";
}

TEST_F(QueueManagerTest, RetrySweepWorkerCallsProcessDelayed)
{
	queue_manager_ = create_manager();

	auto [started, err] = queue_manager_->start();
	ASSERT_TRUE(started);

	wait_for_sweep();

	queue_manager_->stop();

	// Verify process_delayed_messages was called at least once
	EXPECT_GE(mock_backend_->get_process_delayed_call_count(), 1);
}
