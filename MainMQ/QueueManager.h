#pragma once

#include "BackendAdapter.h"
#include "ThreadPool.h"

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct QueueManagerConfig
{
	int32_t lease_sweep_interval_ms = 1000;
	int32_t retry_sweep_interval_ms = 1000;
	int32_t ttl_sweep_interval_ms = 5000;
};

class QueueManager
{
public:
	QueueManager(std::shared_ptr<BackendAdapter> backend, const QueueManagerConfig& config);
	~QueueManager(void);

	auto start(void) -> std::tuple<bool, std::optional<std::string>>;
	auto stop(void) -> void;

	auto register_queue(const std::string& queue_name, const QueuePolicy& policy) -> void;
	auto get_policy(const std::string& queue_name) -> std::optional<QueuePolicy>;

private:
	auto lease_sweep_worker(void) -> void;
	auto retry_sweep_worker(void) -> void;
	auto ttl_sweep_worker(void) -> void;

	auto recover_expired_leases(void) -> void;
	auto process_delayed_messages(void) -> void;
	auto apply_retry_or_dlq(const std::string& message_key, const std::string& queue, int32_t attempt, const QueuePolicy& policy)
		-> std::tuple<bool, std::optional<std::string>>;

	auto calculate_backoff_delay(int32_t attempt, const RetryPolicy& policy) -> int64_t;

private:
	std::atomic<bool> running_;
	QueueManagerConfig config_;
	std::shared_ptr<BackendAdapter> backend_;
	std::shared_ptr<Thread::ThreadPool> thread_pool_;
	std::map<std::string, QueuePolicy> queue_policies_;
	std::mutex policies_mutex_;
};
