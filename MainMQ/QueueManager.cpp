#include "QueueManager.h"

#include "Job.h"
#include "Logger.h"
#include "ThreadWorker.h"

#include <chrono>
#include <cmath>
#include <format>
#include <thread>

QueueManager::QueueManager(std::shared_ptr<BackendAdapter> backend, const QueueManagerConfig& config)
			: running_(false), config_(config), backend_(backend)
		{
			thread_pool_ = std::make_shared<Thread::ThreadPool>("QueueManager");
		}

		QueueManager::~QueueManager(void) { stop(); }

		auto QueueManager::start(void) -> std::tuple<bool, std::optional<std::string>>
		{
			if (running_.load())
			{
				return { false, "already running" };
			}

			// Add ThreadWorkers for LongTerm jobs before starting the pool
			auto lease_worker = std::make_shared<Thread::ThreadWorker>(
				std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
				"QueueManagerLeaseWorker"
			);
			thread_pool_->push(lease_worker);

			auto retry_worker = std::make_shared<Thread::ThreadWorker>(
				std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
				"QueueManagerRetryWorker"
			);
			thread_pool_->push(retry_worker);

			auto ttl_worker = std::make_shared<Thread::ThreadWorker>(
				std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm },
				"QueueManagerTTLWorker"
			);
			thread_pool_->push(ttl_worker);

			auto [started, start_error] = thread_pool_->start();
			if (!started)
			{
				return { false, start_error };
			}

			running_.store(true);

			// Launch lease sweep worker (LongTerm priority for background daemon)
			auto lease_sweep_job = std::make_shared<Thread::Job>(
				Thread::JobPriorities::LongTerm,
				[this]() -> std::tuple<bool, std::optional<std::string>>
				{
					lease_sweep_worker();
					return { true, std::nullopt };
				},
				"LeaseSweepWorker"
			);
			thread_pool_->push(lease_sweep_job);

			// Launch retry sweep worker (LongTerm priority for background daemon)
			auto retry_sweep_job = std::make_shared<Thread::Job>(
				Thread::JobPriorities::LongTerm,
				[this]() -> std::tuple<bool, std::optional<std::string>>
				{
					retry_sweep_worker();
					return { true, std::nullopt };
				},
				"RetrySweepWorker"
			);
			thread_pool_->push(retry_sweep_job);

			// Launch TTL sweep worker (LongTerm priority for background daemon)
			auto ttl_sweep_job = std::make_shared<Thread::Job>(
				Thread::JobPriorities::LongTerm,
				[this]() -> std::tuple<bool, std::optional<std::string>>
				{
					ttl_sweep_worker();
					return { true, std::nullopt };
				},
				"TTLSweepWorker"
			);
			thread_pool_->push(ttl_sweep_job);

			Utilities::Logger::handle().write(Utilities::LogTypes::Information, "QueueManager started");
			return { true, std::nullopt };
		}

		auto QueueManager::stop(void) -> void
		{
			if (!running_.load())
			{
				return;
			}

			running_.store(false);
			thread_pool_->stop(true);

			Utilities::Logger::handle().write(Utilities::LogTypes::Information, "QueueManager stopped");
		}

		auto QueueManager::register_queue(const std::string& queue_name, const QueuePolicy& policy) -> void
		{
			std::lock_guard<std::mutex> lock(policies_mutex_);
			queue_policies_[queue_name] = policy;

			// Save policy to backend
			backend_->save_policy(queue_name, policy);
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

		auto QueueManager::lease_sweep_worker(void) -> void
		{
			while (running_.load())
			{
				recover_expired_leases();
				std::this_thread::sleep_for(std::chrono::milliseconds(config_.lease_sweep_interval_ms));
			}
		}

		auto QueueManager::retry_sweep_worker(void) -> void
		{
			while (running_.load())
			{
				process_delayed_messages();
				std::this_thread::sleep_for(std::chrono::milliseconds(config_.retry_sweep_interval_ms));
			}
		}

		auto QueueManager::recover_expired_leases(void) -> void
		{
			// First, get list of expired inflight messages with their attempt counts
			auto [expired_list, list_error] = backend_->get_expired_inflight_messages();
			if (list_error.has_value())
			{
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Error,
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
				// Get policy for this queue
				auto policy_opt = get_policy(info.queue);
				if (!policy_opt.has_value())
				{
					auto [ok, err] = backend_->delay_message(info.message_key, 0);
					if (ok)
					{
						recovered_count++;
					}
					continue;
				}

				auto& policy = policy_opt.value();

				// Apply retry or DLQ logic based on attempt count
				auto [ok, result_msg] = apply_retry_or_dlq(info.message_key, info.queue, info.attempt, policy);
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
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to process expired message {}: {}", info.message_key, result_msg.value_or("unknown"))
					);
				}
			}

			if (recovered_count > 0 || dlq_count > 0 || delayed_count > 0)
			{
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Information,
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
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Error,
					std::format("Failed to process delayed messages: {}", error.value())
				);
			}
			else if (processed > 0)
			{
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Information,
					std::format("Processed {} delayed messages", processed)
				);
			}
		}

		auto QueueManager::apply_retry_or_dlq(const std::string& message_key, const std::string& queue, int32_t attempt, const QueuePolicy& policy)
			-> std::tuple<bool, std::optional<std::string>>
		{
			if (attempt >= policy.retry.limit)
			{
				// Move to DLQ
				if (policy.dlq.enabled)
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Information,
						std::format("Moving message {} to DLQ after {} attempts", message_key, attempt)
					);

					auto reason = std::format("retry limit exceeded (attempt {})", attempt);
					auto [ok, error] = backend_->move_to_dlq(message_key, reason);
					if (!ok)
					{
						Utilities::Logger::handle().write(
							Utilities::LogTypes::Error,
							std::format("Failed to move message {} to DLQ: {}", message_key, error.value_or("unknown"))
						);
						return { false, error };
					}

					return { true, std::nullopt };
				}
				else
				{
					// DLQ disabled, message stays in current state
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Information,
						std::format("Message {} exceeded retry limit but DLQ is disabled", message_key)
					);
					return { true, "dlq disabled, message dropped" };
				}
			}

			// Calculate backoff delay
			auto delay_ms = calculate_backoff_delay(attempt, policy.retry);

			Utilities::Logger::handle().write(
				Utilities::LogTypes::Information,
				std::format("Retrying message {} after {}ms (attempt {})", message_key, delay_ms, attempt)
			);

			// Set message to delayed state with backoff
			auto [ok, error] = backend_->delay_message(message_key, delay_ms);
			if (!ok)
			{
				Utilities::Logger::handle().write(
					Utilities::LogTypes::Error,
					std::format("Failed to delay message {}: {}", message_key, error.value_or("unknown"))
				);
				return { false, error };
			}

			return { true, std::nullopt };
		}

		auto QueueManager::ttl_sweep_worker(void) -> void
		{
			while (running_.load())
			{
				auto [purged, error] = backend_->purge_expired_messages();
				if (error.has_value())
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Error,
						std::format("Failed to purge expired messages: {}", error.value())
					);
				}
				else if (purged > 0)
				{
					Utilities::Logger::handle().write(
						Utilities::LogTypes::Information,
						std::format("Purged {} expired messages (TTL)", purged)
					);
				}

				std::this_thread::sleep_for(std::chrono::milliseconds(config_.ttl_sweep_interval_ms));
			}
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
