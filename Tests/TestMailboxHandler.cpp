#include "TestHelpers.h"
#include "MailboxHandler.h"
#include "QueueManager.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>

using json = nlohmann::json;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// MockBackendAdapter for MailboxHandler tests
// ---------------------------------------------------------------------------
class MailboxMockBackend : public BackendAdapter
{
public:
	MailboxMockBackend(void) = default;
	~MailboxMockBackend(void) override = default;

	// -- Configurable behavior ------------------------------------------------

	struct EnqueueRecord
	{
		MessageEnvelope envelope;
	};

	struct AckRecord
	{
		LeaseToken lease;
	};

	struct NackRecord
	{
		LeaseToken lease;
		std::string reason;
		bool requeue = false;
	};

	struct ExtendLeaseRecord
	{
		LeaseToken lease;
		int32_t visibility_timeout_sec = 0;
	};

	// Configurable return for lease_next
	LeaseResult next_lease_result = { false, std::nullopt, std::nullopt, std::nullopt };

	// Configurable return for enqueue
	bool enqueue_should_succeed = true;

	// Configurable return for ack
	bool ack_should_succeed = true;

	// Configurable return for nack
	bool nack_should_succeed = true;

	// Configurable return for extend_lease
	bool extend_should_succeed = true;

	// Configurable return for reprocess_dlq
	bool reprocess_should_succeed = true;

	// Configurable metrics
	QueueMetrics configured_metrics;

	// Configurable DLQ messages
	std::vector<DlqMessageInfo> configured_dlq_messages;

	// -- Recorded calls -------------------------------------------------------

	auto get_enqueue_calls(void) -> std::vector<EnqueueRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return enqueue_calls_;
	}

	auto get_ack_calls(void) -> std::vector<AckRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return ack_calls_;
	}

	auto get_nack_calls(void) -> std::vector<NackRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return nack_calls_;
	}

	auto get_extend_calls(void) -> std::vector<ExtendLeaseRecord>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return extend_calls_;
	}

	auto get_reprocess_calls(void) -> std::vector<std::string>
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return reprocess_calls_;
	}

	// -- BackendAdapter interface ---------------------------------------------

	auto open(const BackendConfig& /*config*/) -> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto close(void) -> void override {}

	auto enqueue(const MessageEnvelope& message) -> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		enqueue_calls_.push_back({ message });
		if (enqueue_should_succeed)
		{
			return { true, std::nullopt };
		}
		return { false, "mock enqueue failure" };
	}

	auto lease_next(const std::string& /*queue*/, const std::string& /*consumer_id*/, const int32_t& /*visibility_timeout_sec*/)
		-> LeaseResult override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return next_lease_result;
	}

	auto ack(const LeaseToken& lease) -> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		ack_calls_.push_back({ lease });
		if (ack_should_succeed)
		{
			return { true, std::nullopt };
		}
		return { false, "mock ack failure" };
	}

	auto nack(const LeaseToken& lease, const std::string& reason, const bool& requeue)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		nack_calls_.push_back({ lease, reason, requeue });
		if (nack_should_succeed)
		{
			return { true, std::nullopt };
		}
		return { false, "mock nack failure" };
	}

	auto extend_lease(const LeaseToken& lease, const int32_t& visibility_timeout_sec)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		extend_calls_.push_back({ lease, visibility_timeout_sec });
		if (extend_should_succeed)
		{
			return { true, std::nullopt };
		}
		return { false, "mock extend failure" };
	}

	auto load_policy(const std::string& /*queue*/)
		-> std::tuple<std::optional<QueuePolicy>, std::optional<std::string>> override
	{
		return { std::nullopt, std::nullopt };
	}

	auto save_policy(const std::string& /*queue*/, const QueuePolicy& /*policy*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto metrics(const std::string& /*queue*/)
		-> std::tuple<QueueMetrics, std::optional<std::string>> override
	{
		return { configured_metrics, std::nullopt };
	}

	auto recover_expired_leases(void)
		-> std::tuple<int32_t, std::optional<std::string>> override
	{
		return { 0, std::nullopt };
	}

	auto process_delayed_messages(void)
		-> std::tuple<int32_t, std::optional<std::string>> override
	{
		return { 0, std::nullopt };
	}

	auto get_expired_inflight_messages(void)
		-> std::tuple<std::vector<ExpiredLeaseInfo>, std::optional<std::string>> override
	{
		return { std::vector<ExpiredLeaseInfo>{}, std::nullopt };
	}

	auto delay_message(const std::string& /*message_key*/, int64_t /*delay_ms*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto move_to_dlq(const std::string& /*message_key*/, const std::string& /*reason*/)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		return { true, std::nullopt };
	}

	auto list_dlq_messages(const std::string& /*queue*/, int32_t /*limit*/)
		-> std::tuple<std::vector<DlqMessageInfo>, std::optional<std::string>> override
	{
		return { configured_dlq_messages, std::nullopt };
	}

	auto reprocess_dlq_message(const std::string& message_key)
		-> std::tuple<bool, std::optional<std::string>> override
	{
		std::lock_guard<std::mutex> lock(mutex_);
		reprocess_calls_.push_back(message_key);
		if (reprocess_should_succeed)
		{
			return { true, std::nullopt };
		}
		return { false, "mock reprocess failure" };
	}

	auto purge_expired_messages(void)
		-> std::tuple<int32_t, std::optional<std::string>> override
	{
		return { 0, std::nullopt };
	}

private:
	mutable std::mutex mutex_;
	std::vector<EnqueueRecord> enqueue_calls_;
	std::vector<AckRecord> ack_calls_;
	std::vector<NackRecord> nack_calls_;
	std::vector<ExtendLeaseRecord> extend_calls_;
	std::vector<std::string> reprocess_calls_;
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class MailboxHandlerTest : public ::testing::Test
{
protected:
	std::unique_ptr<TempDir> temp_dir_;
	std::shared_ptr<MailboxMockBackend> mock_backend_;
	std::shared_ptr<QueueManager> queue_manager_;
	std::unique_ptr<MailboxHandler> handler_;
	MailboxConfig config_;

	void SetUp(void) override
	{
		init_test_logger();

		temp_dir_ = std::make_unique<TempDir>("mailbox_handler_test_");

		config_.root = temp_dir_->path() + "/ipc";
		config_.requests_dir = "requests";
		config_.processing_dir = "processing";
		config_.responses_dir = "responses";
		config_.dead_dir = "dead";
		config_.stale_timeout_ms = 30000;
		config_.poll_interval_ms = 50;
		config_.use_folder_watcher = false;  // Use polling mode for reliable tests

		mock_backend_ = std::make_shared<MailboxMockBackend>();

		QueueManagerConfig qm_config;
		qm_config.lease_sweep_interval_ms = 50;
		qm_config.retry_sweep_interval_ms = 50;
		queue_manager_ = std::make_shared<QueueManager>(mock_backend_, qm_config);

		handler_ = std::make_unique<MailboxHandler>(mock_backend_, queue_manager_, config_);
	}

	void TearDown(void) override
	{
		if (handler_)
		{
			handler_->stop();
			handler_.reset();
		}
		queue_manager_.reset();
		mock_backend_.reset();

		// Brief delay to let FolderWatcher fully stop
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		temp_dir_.reset();
	}

	// Write a request JSON file to the requests directory
	auto write_request(const json& request_json) -> std::string
	{
		auto request_id = request_json.value("requestId", "unknown");
		auto filename = std::format("{}.json", request_id);
		auto request_dir = config_.root + "/" + config_.requests_dir;

		fs::create_directories(request_dir);

		auto file_path = request_dir + "/" + filename;
		auto temp_path = file_path + ".tmp";

		std::ofstream file(temp_path);
		file << request_json.dump(2);
		file.flush();
		file.close();

		// Atomic rename to trigger FolderWatcher
		fs::rename(temp_path, file_path);

		return file_path;
	}

	// Wait for a response file to appear and return its contents
	auto wait_for_response(const std::string& client_id, const std::string& request_id, int timeout_ms = 5000) -> std::optional<json>
	{
		auto response_path = std::format("{}/{}/{}/{}.json",
			config_.root, config_.responses_dir, client_id, request_id);

		auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

		while (std::chrono::steady_clock::now() < deadline)
		{
			std::error_code ec;
			if (fs::exists(response_path, ec) && fs::file_size(response_path, ec) > 0)
			{
				// Brief delay to ensure file is fully written
				std::this_thread::sleep_for(std::chrono::milliseconds(20));

				std::ifstream file(response_path);
				if (file.is_open())
				{
					std::string content((std::istreambuf_iterator<char>(file)),
						std::istreambuf_iterator<char>());
					file.close();

					try
					{
						return json::parse(content);
					}
					catch (...)
					{
						// File may not be fully written yet
					}
				}
			}

			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}

		return std::nullopt;
	}

	// Helper: build a standard request JSON
	auto make_request_json(
		const std::string& request_id,
		const std::string& client_id,
		const std::string& command,
		const json& payload = json::object()
	) -> json
	{
		json request;
		request["requestId"] = request_id;
		request["clientId"] = client_id;
		request["command"] = command;
		request["timestampMs"] = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
		request["payload"] = payload;
		return request;
	}

	auto now_ms(void) -> int64_t
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
	}
};

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, StartCreatesDirectories)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok) << "start failed: " << err.value_or("unknown");

	EXPECT_TRUE(fs::exists(config_.root + "/requests"));
	EXPECT_TRUE(fs::exists(config_.root + "/processing"));
	EXPECT_TRUE(fs::exists(config_.root + "/responses"));
	EXPECT_TRUE(fs::exists(config_.root + "/dead"));
}

TEST_F(MailboxHandlerTest, StartSetsRunning)
{
	EXPECT_FALSE(handler_->is_running());

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);
	EXPECT_TRUE(handler_->is_running());

	handler_->stop();
	EXPECT_FALSE(handler_->is_running());
}

TEST_F(MailboxHandlerTest, DoubleStartFails)
{
	auto [ok1, err1] = handler_->start();
	ASSERT_TRUE(ok1);

	auto [ok2, err2] = handler_->start();
	EXPECT_FALSE(ok2);
	EXPECT_TRUE(err2.has_value());
	EXPECT_EQ(err2.value(), "already running");
}

TEST_F(MailboxHandlerTest, StopWithoutStartIsSafe)
{
	EXPECT_NO_THROW(handler_->stop());
}

// ---------------------------------------------------------------------------
// Health command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, HealthCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	auto req = make_request_json("req-health-1", "client-1", "health");
	write_request(req);

	auto response = wait_for_response("client-1", "req-health-1");
	ASSERT_TRUE(response.has_value()) << "No response received for health command";

	EXPECT_EQ((*response)["requestId"], "req-health-1");
	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["data"]["status"], "ok");
	EXPECT_TRUE((*response)["data"].contains("timestamp"));
}

// ---------------------------------------------------------------------------
// Publish command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, PublishCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "telemetry";
	payload["message"] = R"({"temp":36.5})";
	payload["priority"] = 5;

	auto req = make_request_json("req-pub-1", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-pub-1");
	ASSERT_TRUE(response.has_value()) << "No response received for publish command";

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_TRUE((*response)["data"].contains("messageId"));
	EXPECT_TRUE((*response)["data"].contains("messageKey"));

	// Verify backend received the enqueue call
	auto calls = mock_backend_->get_enqueue_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0].envelope.queue, "telemetry");
	EXPECT_EQ(calls[0].envelope.priority, 5);
}

TEST_F(MailboxHandlerTest, PublishMissingQueueReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["message"] = R"({"temp":36.5})";
	// No "queue" field

	auto req = make_request_json("req-pub-2", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-pub-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

TEST_F(MailboxHandlerTest, PublishWithTargetConsumerId)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "direct-queue";
	payload["message"] = R"({"data":"targeted"})";
	payload["targetConsumerId"] = "worker-42";

	auto req = make_request_json("req-pub-3", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-pub-3");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());

	auto calls = mock_backend_->get_enqueue_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0].envelope.target_consumer_id, "worker-42");
}

TEST_F(MailboxHandlerTest, PublishEnqueueFailureReturnsError)
{
	mock_backend_->enqueue_should_succeed = false;

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "fail-queue";
	payload["message"] = R"({"data":"test"})";

	auto req = make_request_json("req-pub-4", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-pub-4");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INTERNAL_ERROR");
}

// ---------------------------------------------------------------------------
// Publish with schema validation
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, PublishSchemaValidationSuccess)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	// Register schema requiring "temp" field
	MessageSchema schema;
	schema.name = "telemetry-schema";
	schema.description = "Requires temp field";
	schema.rules = { MessageValidator::required("temp"), MessageValidator::type_number("temp") };
	handler_->register_schema("validated-queue", schema);

	json payload;
	payload["queue"] = "validated-queue";
	payload["message"] = R"({"temp":36.5})";

	auto req = make_request_json("req-valid-1", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-valid-1");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());
}

TEST_F(MailboxHandlerTest, PublishSchemaValidationFailure)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	MessageSchema schema;
	schema.name = "strict-schema";
	schema.description = "Requires temp field";
	schema.rules = { MessageValidator::required("temp") };
	handler_->register_schema("strict-queue", schema);

	json payload;
	payload["queue"] = "strict-queue";
	payload["message"] = R"({"humidity":50})";

	auto req = make_request_json("req-valid-2", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-valid-2");
	ASSERT_TRUE(response.has_value());
	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_VALIDATION_FAILED");
}

TEST_F(MailboxHandlerTest, UnregisterSchemaAllowsAnyMessage)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	MessageSchema schema;
	schema.name = "temp-schema";
	schema.description = "Temporary";
	schema.rules = { MessageValidator::required("temp") };
	handler_->register_schema("temp-queue", schema);
	handler_->unregister_schema("temp-queue");

	json payload;
	payload["queue"] = "temp-queue";
	payload["message"] = R"({"humidity":50})";

	auto req = make_request_json("req-unreg-1", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-unreg-1");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());
}

// ---------------------------------------------------------------------------
// ConsumeNext command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, ConsumeNextWithMessage)
{
	// Configure mock to return a leased message
	MessageEnvelope msg;
	msg.message_id = "msg-001";
	msg.key = "msg:test-q:msg-001";
	msg.queue = "test-q";
	msg.payload_json = R"({"sensor":"temp"})";
	msg.attributes_json = "{}";
	msg.priority = 3;
	msg.attempt = 0;
	msg.created_at_ms = now_ms();

	LeaseToken lease;
	lease.lease_id = "lease-001";
	lease.message_key = "msg:test-q:msg-001";
	lease.consumer_id = "worker-01";
	lease.lease_until_ms = now_ms() + 30000;

	mock_backend_->next_lease_result = { true, msg, lease, std::nullopt };

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "test-q";
	payload["consumerId"] = "worker-01";
	payload["visibilityTimeoutSec"] = 30;

	auto req = make_request_json("req-consume-1", "client-1", "consume_next", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-consume-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["data"]["message"]["messageId"], "msg-001");
	EXPECT_EQ((*response)["data"]["message"]["queue"], "test-q");
	EXPECT_EQ((*response)["data"]["lease"]["leaseId"], "lease-001");
	EXPECT_EQ((*response)["data"]["lease"]["consumerId"], "worker-01");
}

TEST_F(MailboxHandlerTest, ConsumeNextEmptyQueue)
{
	// Default mock returns no lease
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "empty-q";
	payload["consumerId"] = "worker-01";

	auto req = make_request_json("req-consume-2", "client-1", "consume_next", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-consume-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_TRUE((*response)["data"]["message"].is_null());
}

TEST_F(MailboxHandlerTest, ConsumeNextMissingQueueReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["consumerId"] = "worker-01";
	// No "queue" field

	auto req = make_request_json("req-consume-3", "client-1", "consume", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-consume-3");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

// ---------------------------------------------------------------------------
// Ack command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, AckCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-001";
	payload["messageKey"] = "msg:test-q:msg-001";
	payload["consumerId"] = "worker-01";

	auto req = make_request_json("req-ack-1", "client-1", "ack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-ack-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());

	auto calls = mock_backend_->get_ack_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0].lease.lease_id, "lease-001");
	EXPECT_EQ(calls[0].lease.message_key, "msg:test-q:msg-001");
	EXPECT_EQ(calls[0].lease.consumer_id, "worker-01");
}

TEST_F(MailboxHandlerTest, AckMissingMessageKeyReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-001";
	// No "messageKey"

	auto req = make_request_json("req-ack-2", "client-1", "ack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-ack-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

TEST_F(MailboxHandlerTest, AckBackendFailureReturnsError)
{
	mock_backend_->ack_should_succeed = false;

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-001";
	payload["messageKey"] = "msg:test-q:msg-001";

	auto req = make_request_json("req-ack-3", "client-1", "ack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-ack-3");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INTERNAL_ERROR");
}

// ---------------------------------------------------------------------------
// Nack command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, NackWithRequeue)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-002";
	payload["messageKey"] = "msg:test-q:msg-002";
	payload["consumerId"] = "worker-01";
	payload["reason"] = "transient error";
	payload["requeue"] = true;

	auto req = make_request_json("req-nack-1", "client-1", "nack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-nack-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());

	auto calls = mock_backend_->get_nack_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0].lease.message_key, "msg:test-q:msg-002");
	EXPECT_EQ(calls[0].reason, "transient error");
	EXPECT_TRUE(calls[0].requeue);
}

TEST_F(MailboxHandlerTest, NackWithoutRequeue)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-003";
	payload["messageKey"] = "msg:test-q:msg-003";
	payload["reason"] = "permanent failure";
	payload["requeue"] = false;

	auto req = make_request_json("req-nack-2", "client-1", "nack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-nack-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());

	auto calls = mock_backend_->get_nack_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_FALSE(calls[0].requeue);
}

TEST_F(MailboxHandlerTest, NackMissingMessageKeyReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["reason"] = "some error";
	// No "messageKey"

	auto req = make_request_json("req-nack-3", "client-1", "nack", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-nack-3");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

// ---------------------------------------------------------------------------
// ExtendLease command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, ExtendLeaseCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-004";
	payload["messageKey"] = "msg:test-q:msg-004";
	payload["consumerId"] = "worker-01";
	payload["visibilityTimeoutSec"] = 120;

	auto req = make_request_json("req-extend-1", "client-1", "extend_lease", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-extend-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());

	auto calls = mock_backend_->get_extend_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0].lease.lease_id, "lease-004");
	EXPECT_EQ(calls[0].lease.message_key, "msg:test-q:msg-004");
	EXPECT_EQ(calls[0].visibility_timeout_sec, 120);
}

TEST_F(MailboxHandlerTest, ExtendLeaseMissingMessageKeyReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["leaseId"] = "lease-004";
	// No "messageKey"

	auto req = make_request_json("req-extend-2", "client-1", "extendLease", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-extend-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

// ---------------------------------------------------------------------------
// Status command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, StatusCommand)
{
	// Configure mock metrics
	mock_backend_->configured_metrics.ready = 10;
	mock_backend_->configured_metrics.inflight = 3;
	mock_backend_->configured_metrics.delayed = 2;
	mock_backend_->configured_metrics.dlq = 1;

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "status-q";

	auto req = make_request_json("req-status-1", "client-1", "status", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-status-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["data"]["queue"], "status-q");
	EXPECT_EQ((*response)["data"]["metrics"]["ready"], 10);
	EXPECT_EQ((*response)["data"]["metrics"]["inflight"], 3);
	EXPECT_EQ((*response)["data"]["metrics"]["delayed"], 2);
	EXPECT_EQ((*response)["data"]["metrics"]["dlq"], 1);
}

TEST_F(MailboxHandlerTest, StatusMissingQueueReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	// No "queue"

	auto req = make_request_json("req-status-2", "client-1", "status", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-status-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

// ---------------------------------------------------------------------------
// Metrics command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, MetricsCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	auto req = make_request_json("req-metrics-1", "client-1", "metrics");
	write_request(req);

	auto response = wait_for_response("client-1", "req-metrics-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_TRUE((*response)["data"].contains("timestamp"));
	EXPECT_TRUE((*response)["data"].contains("uptimeMs"));
	EXPECT_TRUE((*response)["data"].contains("requests"));
	EXPECT_TRUE((*response)["data"].contains("commands"));
	EXPECT_TRUE((*response)["data"].contains("errors"));
	EXPECT_TRUE((*response)["data"].contains("timing"));
}

// ---------------------------------------------------------------------------
// ListDlq command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, ListDlqCommand)
{
	DlqMessageInfo dlq_msg;
	dlq_msg.message_key = "msg:dlq-q:msg-dead-001";
	dlq_msg.queue = "dlq-q";
	dlq_msg.reason = "max retries exceeded";
	dlq_msg.dlq_at_ms = now_ms();
	dlq_msg.attempt = 5;

	mock_backend_->configured_dlq_messages = { dlq_msg };

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "dlq-q";
	payload["limit"] = 50;

	auto req = make_request_json("req-dlq-1", "client-1", "list_dlq", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-dlq-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["data"]["queue"], "dlq-q");
	EXPECT_EQ((*response)["data"]["count"], 1);
	ASSERT_EQ((*response)["data"]["messages"].size(), 1u);
	EXPECT_EQ((*response)["data"]["messages"][0]["messageKey"], "msg:dlq-q:msg-dead-001");
	EXPECT_EQ((*response)["data"]["messages"][0]["reason"], "max retries exceeded");
}

TEST_F(MailboxHandlerTest, ListDlqMissingQueueReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	// No "queue"

	auto req = make_request_json("req-dlq-2", "client-1", "dlq", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-dlq-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

// ---------------------------------------------------------------------------
// ReprocessDlq command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, ReprocessDlqCommand)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["messageKey"] = "msg:orders:msg-dead-001";

	auto req = make_request_json("req-reprocess-1", "client-1", "reprocess_dlq", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-reprocess-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_TRUE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["data"]["messageKey"], "msg:orders:msg-dead-001");
	EXPECT_TRUE((*response)["data"]["reprocessed"].get<bool>());

	auto calls = mock_backend_->get_reprocess_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_EQ(calls[0], "msg:orders:msg-dead-001");
}

TEST_F(MailboxHandlerTest, ReprocessDlqMissingKeyReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	// No "messageKey"

	auto req = make_request_json("req-reprocess-2", "client-1", "reprocess", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-reprocess-2");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_INVALID_REQUEST");
}

TEST_F(MailboxHandlerTest, ReprocessDlqBackendFailure)
{
	mock_backend_->reprocess_should_succeed = false;

	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["messageKey"] = "msg:orders:msg-not-found";

	auto req = make_request_json("req-reprocess-3", "client-1", "reprocess_dlq", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-reprocess-3");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_DLQ_NOT_FOUND");
}

// ---------------------------------------------------------------------------
// Unknown command
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, UnknownCommandReturnsError)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	auto req = make_request_json("req-unknown-1", "client-1", "invalid_command");
	write_request(req);

	auto response = wait_for_response("client-1", "req-unknown-1");
	ASSERT_TRUE(response.has_value());

	EXPECT_FALSE((*response)["ok"].get<bool>());
	EXPECT_EQ((*response)["error"]["code"], "ERR_UNKNOWN_COMMAND");
}

// ---------------------------------------------------------------------------
// Invalid request (malformed JSON)
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, MalformedJsonGoesToDead)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	// Write malformed JSON directly to requests dir
	auto request_dir = config_.root + "/requests";
	auto file_path = request_dir + "/bad-request.json";
	auto temp_path = file_path + ".tmp";

	std::ofstream file(temp_path);
	file << "{ this is not valid json }}}";
	file.flush();
	file.close();
	fs::rename(temp_path, file_path);

	// Wait for it to be processed
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	// The file should be moved to dead directory
	auto dead_dir = config_.root + "/dead";
	bool found_in_dead = false;
	std::error_code ec;
	if (fs::exists(dead_dir, ec))
	{
		for (const auto& entry : fs::directory_iterator(dead_dir, ec))
		{
			if (entry.path().filename() == "bad-request.json")
			{
				found_in_dead = true;
				break;
			}
		}
	}
	EXPECT_TRUE(found_in_dead) << "Malformed request should be moved to dead directory";
}

TEST_F(MailboxHandlerTest, MissingRequestIdGoesToDead)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	// Valid JSON but missing requestId
	json bad_request;
	bad_request["clientId"] = "client-1";
	bad_request["command"] = "health";

	auto request_dir = config_.root + "/requests";
	auto file_path = request_dir + "/missing-id.json";
	auto temp_path = file_path + ".tmp";

	std::ofstream file(temp_path);
	file << bad_request.dump();
	file.flush();
	file.close();
	fs::rename(temp_path, file_path);

	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	auto dead_dir = config_.root + "/dead";
	bool found_in_dead = false;
	std::error_code ec;
	if (fs::exists(dead_dir, ec))
	{
		for (const auto& entry : fs::directory_iterator(dead_dir, ec))
		{
			if (entry.path().filename() == "missing-id.json")
			{
				found_in_dead = true;
				break;
			}
		}
	}
	EXPECT_TRUE(found_in_dead) << "Request with missing requestId should be moved to dead directory";
}

// ---------------------------------------------------------------------------
// Command alias tests (consume, extend, dlq, reprocess)
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, ConsumeAliasWorks)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "alias-q";
	payload["consumerId"] = "worker-01";

	// "consume" is an alias for "consume_next"
	auto req = make_request_json("req-alias-1", "client-1", "consume", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-alias-1");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());
}

TEST_F(MailboxHandlerTest, ExtendAliasWorks)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["messageKey"] = "msg:q:1";
	payload["visibilityTimeoutSec"] = 60;

	// "extend" is an alias for "extend_lease"
	auto req = make_request_json("req-alias-2", "client-1", "extend", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-alias-2");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());
}

// ---------------------------------------------------------------------------
// Multiple requests sequentially
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, MultipleRequestsProcessedSequentially)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	// Send 3 health requests
	for (int i = 0; i < 3; ++i)
	{
		auto req_id = std::format("req-multi-{}", i);
		auto req = make_request_json(req_id, "client-1", "health");
		write_request(req);

		// Small delay between writes to avoid race conditions
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	// All 3 should get responses
	for (int i = 0; i < 3; ++i)
	{
		auto req_id = std::format("req-multi-{}", i);
		auto response = wait_for_response("client-1", req_id);
		ASSERT_TRUE(response.has_value()) << "Missing response for " << req_id;
		EXPECT_TRUE((*response)["ok"].get<bool>());
	}
}

// ---------------------------------------------------------------------------
// Multiple clients
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, MultipleClientsGetSeparateResponses)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	auto req1 = make_request_json("req-mc-1", "client-alpha", "health");
	auto req2 = make_request_json("req-mc-2", "client-beta", "health");

	write_request(req1);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	write_request(req2);

	auto resp1 = wait_for_response("client-alpha", "req-mc-1");
	auto resp2 = wait_for_response("client-beta", "req-mc-2");

	ASSERT_TRUE(resp1.has_value());
	ASSERT_TRUE(resp2.has_value());

	EXPECT_EQ((*resp1)["requestId"], "req-mc-1");
	EXPECT_EQ((*resp2)["requestId"], "req-mc-2");
}

// ---------------------------------------------------------------------------
// Publish with delay
// ---------------------------------------------------------------------------

TEST_F(MailboxHandlerTest, PublishWithDelay)
{
	auto [ok, err] = handler_->start();
	ASSERT_TRUE(ok);

	json payload;
	payload["queue"] = "delayed-q";
	payload["message"] = R"({"data":"delayed"})";
	payload["delayMs"] = 5000;

	auto req = make_request_json("req-delay-1", "client-1", "publish", payload);
	write_request(req);

	auto response = wait_for_response("client-1", "req-delay-1");
	ASSERT_TRUE(response.has_value());
	EXPECT_TRUE((*response)["ok"].get<bool>());

	// Verify the envelope had delay applied
	auto calls = mock_backend_->get_enqueue_calls();
	ASSERT_GE(calls.size(), 1u);
	EXPECT_GT(calls[0].envelope.available_at_ms, calls[0].envelope.created_at_ms);
}
