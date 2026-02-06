#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// Mailbox IPC configuration
struct MailboxConfig
{
	std::string root = "./ipc";
	std::string requests_dir = "requests";
	std::string processing_dir = "processing";
	std::string responses_dir = "responses";
	std::string dead_dir = "dead";
	int32_t stale_timeout_ms = 30000;
	int32_t poll_interval_ms = 100;
	bool use_folder_watcher = true;
};

// Mailbox command types
enum class MailboxCommand
{
	Unknown,
	Publish,
	ConsumeNext,
	Ack,
	Nack,
	ExtendLease,
	Status,
	Health,
	Metrics,
	ListDlq,
	ReprocessDlq
};

// Mailbox request structure
struct MailboxRequest
{
	std::string request_id;
	std::string client_id;
	MailboxCommand command = MailboxCommand::Unknown;
	int64_t timestamp_ms = 0;
	int64_t deadline_ms = 0;
	std::string payload_json;
	std::string file_path;
};

// Mailbox response structure
struct MailboxResponse
{
	std::string request_id;
	bool ok = false;
	std::string data_json;
	std::string error_code;
	std::string error_message;
};

// Publish command payload
struct PublishPayload
{
	std::string queue;
	std::string message_json;
	int32_t priority = 0;
	int64_t delay_ms = 0;
	std::string attributes_json;
	std::string target_consumer_id;  // empty = any consumer, value = specific consumer only
};

// ConsumeNext command payload
struct ConsumeNextPayload
{
	std::string queue;
	std::string consumer_id;
	int32_t visibility_timeout_sec = 30;
};

// Ack command payload
struct AckPayload
{
	std::string lease_id;
	std::string message_key;
	std::string consumer_id;
};

// Nack command payload
struct NackPayload
{
	std::string lease_id;
	std::string message_key;
	std::string consumer_id;
	std::string reason;
	bool requeue = false;
};

// ExtendLease command payload
struct ExtendLeasePayload
{
	std::string lease_id;
	std::string message_key;
	std::string consumer_id;
	int32_t visibility_timeout_sec = 30;
};

// Status command payload
struct StatusPayload
{
	std::string queue;
};

// ListDlq command payload
struct ListDlqPayload
{
	std::string queue;
	int32_t limit = 100;
};

// ReprocessDlq command payload
struct ReprocessDlqPayload
{
	std::string message_key;
};

// Mailbox handler metrics
struct MailboxMetrics
{
	// Request counters
	uint64_t total_requests = 0;
	uint64_t success_count = 0;
	uint64_t error_count = 0;

	// Per-command counters
	uint64_t publish_count = 0;
	uint64_t consume_count = 0;
	uint64_t ack_count = 0;
	uint64_t nack_count = 0;
	uint64_t status_count = 0;
	uint64_t health_count = 0;
	uint64_t dlq_count = 0;

	// Error type counters
	uint64_t parse_errors = 0;
	uint64_t validation_errors = 0;
	uint64_t timeout_errors = 0;
	uint64_t internal_errors = 0;

	// Timing (cumulative ms)
	uint64_t total_processing_time_ms = 0;
	uint64_t last_request_time_ms = 0;

	// Uptime
	int64_t start_time_ms = 0;
};

// Error codes
namespace MailboxErrorCode
{
	constexpr const char* INVALID_REQUEST = "ERR_INVALID_REQUEST";
	constexpr const char* UNKNOWN_COMMAND = "ERR_UNKNOWN_COMMAND";
	constexpr const char* QUEUE_NOT_FOUND = "ERR_QUEUE_NOT_FOUND";
	constexpr const char* MESSAGE_NOT_FOUND = "ERR_MESSAGE_NOT_FOUND";
	constexpr const char* LEASE_EXPIRED = "ERR_LEASE_EXPIRED";
	constexpr const char* LEASE_MISMATCH = "ERR_LEASE_MISMATCH";
	constexpr const char* INTERNAL_ERROR = "ERR_INTERNAL_ERROR";
	constexpr const char* TIMEOUT = "ERR_TIMEOUT";
	constexpr const char* PARSE_ERROR = "ERR_PARSE_ERROR";
	constexpr const char* VALIDATION_FAILED = "ERR_VALIDATION_FAILED";
	constexpr const char* DLQ_NOT_FOUND = "ERR_DLQ_NOT_FOUND";
}
