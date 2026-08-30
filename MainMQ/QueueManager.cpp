#include "QueueManager.h"

#include "Job.h"
#include "Logger.h"
#include "ThreadWorker.h"

#include <chrono>
#include <cmath>
#include <expected>
#include <format>
#include <utility>
#include <vector>

using namespace Utilities;

QueueManager::QueueManager(std::shared_ptr<BackendAdapter> backend, const QueueManagerConfig& config)
	: running_(false), config_(config), backend_(backend)
{
	thread_pool_ = std::make_shared<Thread::ThreadPool>("QueueManager");

	default_policy_.visibility_timeout_sec = 30;
	default_policy_.ttl_sec = 0;
	default_policy_.retry.limit = 5;
	default_policy_.retry.backoff = "exponential";
	default_policy_.retry.initial_delay_sec = 1;
	default_policy_.retry.max_delay_sec = 60;
	default_policy_.dlq.enabled = true;
	default_policy_.dlq.queue = "";
	default_policy_.dlq.retention_days = 7;
}

QueueManager::~QueueManager(void) { stop(); }

auto QueueManager::start(void) -> std::tuple<bool, std::optional<std::string>>
{
	if (running_.load())
	{
		return { false, "already running" };
	}

	// 세 잡은 주기 대기 동안에도 워커를 점유하므로 잡 수만큼 전용 ThreadWorker 를 pool 시작 전에 붙인다.
	//
	// 워커에 잡 함수명을 붙이지 않는다. JobPool 은 우선순위로만 잡을 배분하므로 어느 워커가 어느
	// 잡을 집을지는 고정되지 않는다("... on queue_manager_lease_job" 워커가 ttl 잡을 돌 수 있다).
	// 실행 주체는 Job 제목(= 잡 함수명)이 가리키며, 워커 이름은 스레드 구분용 번호만 갖는다.
	for (int32_t index = 1; index <= sweep_job_count; ++index)
	{
		thread_pool_->push(std::make_shared<Thread::ThreadWorker>(
			std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
			std::format("queue_manager_sweep_worker_{}", index)
		));
	}

	auto start_result = thread_pool_->start();
	if (!start_result)
	{
		return { false, start_result.error() };
	}

	running_.store(true);

	push_lease_job();
	push_retry_job();
	push_ttl_job();

	Logger::handle().write(LogTypes::Information, "QueueManager started");
	return { true, std::nullopt };
}

auto QueueManager::stop(void) -> void
{
	if (!running_.load())
	{
		return;
	}

	running_.store(false);
	sweep_cv_.notify_all();
	thread_pool_->stop(true);

	Logger::handle().write(LogTypes::Information, "QueueManager stopped");
}

auto QueueManager::register_queue(const std::string& queue_name, const QueuePolicy& policy) -> void
{
	std::lock_guard<std::mutex> lock(policies_mutex_);
	queue_policies_[queue_name] = policy;

	// Save policy to backend (single point of policy persistence)
	auto [saved, error] = backend_->save_policy(queue_name, policy);
	if (saved)
	{
		Logger::handle().write(
			LogTypes::Information,
			std::format("Registered queue: {}", queue_name)
		);
	}
	else
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to save policy for queue {}: {}", queue_name, error.value_or("unknown"))
		);
	}
}

auto QueueManager::get_policy(const std::string& queue_name) -> std::optional<QueuePolicy>
{
	std::lock_guard<std::mutex> lock(policies_mutex_);
	auto it = queue_policies_.find(queue_name);
	if (it != queue_policies_.end())
	{
		return it->second;
	}
	return std::nullopt;
}

auto QueueManager::queue_manager_lease_job(void) -> std::expected<void, std::string>
{
	if (!running_.load())
	{
		return {};
	}

	recover_expired_leases();

	// 재등록 전에 대기한다. 순서를 뒤집으면 유휴 워커가 곧바로 집어 간격 없이 다시 돌아버린다.
	wait_next_sweep(config_.lease_sweep_interval_ms);
	push_lease_job();

	return {};
}

auto QueueManager::queue_manager_retry_job(void) -> std::expected<void, std::string>
{
	if (!running_.load())
	{
		return {};
	}

	process_delayed_messages();

	wait_next_sweep(config_.retry_sweep_interval_ms);
	push_retry_job();

	return {};
}

auto QueueManager::queue_manager_ttl_job(void) -> std::expected<void, std::string>
{
	if (!running_.load())
	{
		return {};
	}

	purge_expired_messages();
	purge_dlq_retentions();

	wait_next_sweep(config_.ttl_sweep_interval_ms);
	push_ttl_job();

	return {};
}

auto QueueManager::push_lease_job(void) -> void
{
	push_sweep_job(std::make_shared<Thread::Job>(
		Thread::JobPriorities::LongTerm,
		[this]() -> std::expected<void, std::string> { return queue_manager_lease_job(); },
		"queue_manager_lease_job"
	));
}

auto QueueManager::push_retry_job(void) -> void
{
	push_sweep_job(std::make_shared<Thread::Job>(
		Thread::JobPriorities::LongTerm,
		[this]() -> std::expected<void, std::string> { return queue_manager_retry_job(); },
		"queue_manager_retry_job"
	));
}

auto QueueManager::push_ttl_job(void) -> void
{
	push_sweep_job(std::make_shared<Thread::Job>(
		Thread::JobPriorities::LongTerm,
		[this]() -> std::expected<void, std::string> { return queue_manager_ttl_job(); },
		"queue_manager_ttl_job"
	));
}

auto QueueManager::push_sweep_job(std::shared_ptr<Thread::Job> job) -> void
{
	auto push_result = thread_pool_->push(job);
	if (push_result)
	{
		return;
	}

	// stop() 이 JobPool 을 잠그므로 종료 중 재등록 실패는 정상이다. running_ 이 아직 서 있는데
	// 실패했다면 그 sweep 이 영구히 멈춘 것이라 반드시 남긴다.
	if (running_.load())
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to re-arm sweep job {}: {}", job->title(), push_result.error())
		);
	}
}

auto QueueManager::wait_next_sweep(int32_t interval_ms) -> void
{
	std::unique_lock<std::mutex> lock(sweep_mutex_);
	sweep_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms), [this] { return !running_.load(); });
}

auto QueueManager::recover_expired_leases(void) -> void
{
	// First, get list of expired inflight messages with their attempt counts
	auto [expired_list, list_error] = backend_->get_expired_inflight_messages();
	if (list_error.has_value())
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to get expired inflight messages: {}", list_error.value())
		);
		return;
	}

	if (expired_list.empty())
	{
		return;
	}

	int32_t recovered_count = 0;
	int32_t dlq_count = 0;
	int32_t delayed_count = 0;

	for (const auto& info : expired_list)
	{
		QueuePolicy policy = get_policy(info.queue).value_or(default_policy_);

		// Apply retry or DLQ logic based on attempt count
		auto [ok, result_msg] = apply_retry_or_dlq(info.message_key, info.attempt, policy);
		if (ok)
		{
			if (result_msg.has_value() && result_msg.value().find("dlq") != std::string::npos)
			{
				dlq_count++;
			}
			else if (info.attempt >= policy.retry.limit)
			{
				dlq_count++;
			}
			else
			{
				delayed_count++;
			}
		}
		else
		{
			Logger::handle().write(
				LogTypes::Error,
				std::format("Failed to process expired message {}: {}", info.message_key, result_msg.value_or("unknown"))
			);
		}
	}

	if (recovered_count > 0 || dlq_count > 0 || delayed_count > 0)
	{
		Logger::handle().write(
			LogTypes::Information,
			std::format("Processed {} expired leases: {} recovered, {} to DLQ, {} delayed for retry",
				expired_list.size(), recovered_count, dlq_count, delayed_count)
		);
	}
}

auto QueueManager::process_delayed_messages(void) -> void
{
	auto [processed, error] = backend_->process_delayed_messages();
	if (error.has_value())
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to process delayed messages: {}", error.value())
		);
	}
	else if (processed > 0)
	{
		Logger::handle().write(
			LogTypes::Information,
			std::format("Processed {} delayed messages", processed)
		);
	}
}

auto QueueManager::purge_expired_messages(void) -> void
{
	auto [purged, error] = backend_->purge_expired_messages();
	if (error.has_value())
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to purge expired messages: {}", error.value())
		);
	}
	else if (purged > 0)
	{
		Logger::handle().write(
			LogTypes::Information,
			std::format("Purged {} expired messages (TTL)", purged)
		);
	}
}

auto QueueManager::purge_dlq_retentions(void) -> void
{
	// DLQ retention: purge DLQ entries older than each queue's retention window.
	// Snapshot the (queue, days) pairs under the policies lock, then call the backend
	// outside it to avoid holding two locks during I/O. (Defect D-07)
	std::vector<std::pair<std::string, int32_t>> dlq_retentions;
	{
		std::lock_guard<std::mutex> plock(policies_mutex_);
		for (const auto& [queue_name, policy] : queue_policies_)
		{
			if (policy.dlq.enabled && policy.dlq.retention_days > 0)
			{
				dlq_retentions.emplace_back(queue_name, policy.dlq.retention_days);
			}
		}
	}

	auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();

	for (const auto& [queue_name, retention_days] : dlq_retentions)
	{
		auto cutoff = now_ms - static_cast<int64_t>(retention_days) * 24 * 60 * 60 * 1000;
		auto [dlq_purged, dlq_error] = backend_->purge_dlq_messages(queue_name, cutoff);
		if (dlq_error.has_value())
		{
			Logger::handle().write(
				LogTypes::Error,
				std::format("Failed to purge DLQ retention for queue {}: {}", queue_name, dlq_error.value())
			);
		}
		else if (dlq_purged > 0)
		{
			Logger::handle().write(
				LogTypes::Information,
				std::format("Purged {} DLQ messages past retention ({} days) for queue {}", dlq_purged, retention_days, queue_name)
			);
		}
	}
}

auto QueueManager::apply_retry_or_dlq(const std::string& message_key, int32_t attempt, const QueuePolicy& policy)
	-> std::tuple<bool, std::optional<std::string>>
{
	if (attempt >= policy.retry.limit)
	{
		// Move to DLQ
		if (policy.dlq.enabled)
		{
			Logger::handle().write(
				LogTypes::Information,
				std::format("Moving message {} to DLQ after {} attempts", message_key, attempt)
			);

			auto reason = std::format("retry limit exceeded (attempt {})", attempt);
			auto [ok, error] = backend_->move_to_dlq(message_key, reason);
			if (!ok)
			{
				Logger::handle().write(
					LogTypes::Error,
					std::format("Failed to move message {} to DLQ: {}", message_key, error.value_or("unknown"))
				);
				return { false, error };
			}

			return { true, std::nullopt };
		}
		else
		{
			// DLQ disabled, message stays in current state
			Logger::handle().write(
				LogTypes::Information,
				std::format("Message {} exceeded retry limit but DLQ is disabled", message_key)
			);
			return { true, "dlq disabled, message dropped" };
		}
	}

	// Calculate backoff delay
	auto delay_ms = calculate_backoff_delay(attempt, policy.retry);

	Logger::handle().write(
		LogTypes::Information,
		std::format("Retrying message {} after {}ms (attempt {})", message_key, delay_ms, attempt)
	);

	// Set message to delayed state with backoff
	auto [ok, error] = backend_->delay_message(message_key, delay_ms);
	if (!ok)
	{
		Logger::handle().write(
			LogTypes::Error,
			std::format("Failed to delay message {}: {}", message_key, error.value_or("unknown"))
		);
		return { false, error };
	}

	return { true, std::nullopt };
}

auto QueueManager::calculate_backoff_delay(int32_t attempt, const RetryPolicy& policy) -> int64_t
{
	if (policy.backoff == "exponential")
	{
		// Exponential backoff: initial * 2^(attempt-1)
		double delay_sec = policy.initial_delay_sec * std::pow(2.0, attempt - 1);
		delay_sec = std::min(delay_sec, static_cast<double>(policy.max_delay_sec));
		return static_cast<int64_t>(delay_sec * 1000);
	}
	else if (policy.backoff == "linear")
	{
		// Linear backoff: initial * attempt
		int32_t delay_sec = policy.initial_delay_sec * attempt;
		delay_sec = std::min(delay_sec, policy.max_delay_sec);
		return static_cast<int64_t>(delay_sec) * 1000;
	}
	else
	{
		// Fixed backoff
		return static_cast<int64_t>(policy.initial_delay_sec) * 1000;
	}
}
