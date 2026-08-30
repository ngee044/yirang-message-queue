#pragma once

#include "BackendAdapter.h"
#include "ThreadPool.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>

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

protected:
	// sweep 잡 본문. 루프문을 두지 않는다 — 한 번 호출되면 한 주기만 수행하고, 다음 주기까지
	// 대기한 뒤 자신을 thread_pool_ 에 다시 push 하여 반복한다. running_ 이 내려가면 재등록하지
	// 않으므로 잡이 자연히 소멸한다.
	// 반환 타입이 Job 콜백 계약과 같아 start()·재등록 쪽 람다는 한 줄 위임이면 된다.
	auto queue_manager_lease_job(void) -> std::expected<void, std::string>;
	auto queue_manager_retry_job(void) -> std::expected<void, std::string>;
	auto queue_manager_ttl_job(void) -> std::expected<void, std::string>;

private:
	// 상주 sweep 잡 수 = 전용 ThreadWorker 수. 잡이 주기 대기 중에도 워커를 점유하므로 둘은 같아야 한다.
	static constexpr int32_t sweep_job_count = 3;

	// 각 잡을 Thread::Job 으로 감싸 push 한다. start() 와 각 잡의 마지막 줄이 같은 함수를 쓰므로
	// 잡 제목과 본문의 대응이 한 곳에만 존재한다.
	auto push_lease_job(void) -> void;
	auto push_retry_job(void) -> void;
	auto push_ttl_job(void) -> void;
	auto push_sweep_job(std::shared_ptr<Thread::Job> job) -> void;

	// 세 잡이 공유하는 주기 대기. sleep_for 가 아니라 wait_for 이므로 stop() 의 notify_all 이 즉시 깨운다.
	auto wait_next_sweep(int32_t interval_ms) -> void;

	auto recover_expired_leases(void) -> void;
	auto process_delayed_messages(void) -> void;
	auto purge_expired_messages(void) -> void;
	auto purge_dlq_retentions(void) -> void;

	auto apply_retry_or_dlq(const std::string& message_key, int32_t attempt, const QueuePolicy& policy)
		-> std::tuple<bool, std::optional<std::string>>;

	auto calculate_backoff_delay(int32_t attempt, const RetryPolicy& policy) -> int64_t;

private:
	std::atomic<bool> running_;
	QueueManagerConfig config_;
	QueuePolicy default_policy_;
	std::shared_ptr<BackendAdapter> backend_;
	std::shared_ptr<Thread::ThreadPool> thread_pool_;
	std::map<std::string, QueuePolicy> queue_policies_;
	std::mutex policies_mutex_;
	std::condition_variable sweep_cv_;
	std::mutex sweep_mutex_;
};
