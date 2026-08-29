#include "ThreadPool.h"

#include "JobPool.h"
#include "Logger.h"
#include "ThreadWorker.h"

#include <format>

#include <functional>

using namespace Utilities;

namespace Thread
{
	ThreadPool::ThreadPool(const std::string& title)
		: job_pool_(std::make_shared<JobPool>(std::format("JobPool on {}", title))), working_(false), thread_title_(title), pause_(false)
	{
		job_pool_->notify_callback(std::bind(&ThreadPool::notify_callback, this, std::placeholders::_1));
	}

	ThreadPool::~ThreadPool(void)
	{
		stop(true);

		thread_workers_.clear();
		job_pool_.reset();

		Logger::handle().write(LogTypes::Debug, std::format("destroyed {}", thread_title_));
	}

	auto ThreadPool::get_ptr(void) -> std::shared_ptr<ThreadPool> { return shared_from_this(); }

	auto ThreadPool::uncompleted_jobs(const std::string& backup_folder) -> std::vector<std::vector<uint8_t>>
	{
		if (job_pool_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "cannot get uncompleted jobs by null job_pool");

			return {};
		}

		return job_pool_->uncompleted_jobs(backup_folder);
	}

	auto ThreadPool::push(std::shared_ptr<Job> job) -> std::tuple<bool, std::optional<std::string>>
	{
		if (job_pool_ == nullptr)
		{
			return { false, "cannot push a job into null JobPool" };
		}

		return job_pool_->push(job);
	}

	auto ThreadPool::push(std::shared_ptr<ThreadWorker> worker) -> void
	{
		if (worker == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "cannot push a null ThreadWorker");

			return;
		}

		auto priority = priority_string(worker->priorities());

		std::scoped_lock<std::mutex> lock(mutex_);

		thread_workers_.push_back(worker);

		worker->job_pool(job_pool_);
		worker->pause(pause_.load());

		// 워커 제목은 로그에서 실행 주체를 가리는 유일한 이름이다. 호출자가 지은 이름을
		// 덮어쓰면 같은 우선순위의 워커가 모두 한 이름이 되어 어느 워커가 멈췄는지 구분할 수 없다.
		// 이름을 주지 않은 워커만 pool 이 채운다.
		if (worker->worker_title() == default_worker_title)
		{
			worker->worker_title(std::format("{} ThreadWorker on {}", priority, thread_title_));
		}

		Logger::handle().write(LogTypes::Parameter, std::format("pushed {} {} on {}", priority, worker->worker_title(), thread_title_));

		if (working_.load())
		{
			worker->start();
		}
	}

	auto ThreadPool::remove_workers(const JobPriorities& priority) -> std::tuple<size_t, std::optional<std::string>>
	{
		if (job_pool_ == nullptr)
		{
			return { 0, "cannot remove workers due to null JobPool" };
		}

		job_pool_->clear(priority);

		std::scoped_lock<std::mutex> lock(mutex_);

		auto new_end = std::remove_if(thread_workers_.begin(), thread_workers_.end(),
									  [priority](const std::shared_ptr<ThreadWorker>& worker)
									  {
										  if (worker == nullptr)
										  {
											  return false;
										  }

										  auto priorities = worker->priorities();
										  auto new_end = std::remove_if(priorities.begin(), priorities.end(),
																		[priority](const JobPriorities& target) { return target == priority; });
										  priorities.erase(new_end, priorities.end());
										  worker->priorities(priorities);

										  if (!priorities.empty())
										  {
											  return false;
										  }

										  worker->stop();

										  return true;
									  });
		std::vector<std::shared_ptr<ThreadWorker>> removed_items(new_end, thread_workers_.end());
		if (removed_items.size() == 0)
		{
			return { 0, "no worker to remove" };
		}

		thread_workers_.erase(new_end, thread_workers_.end());

		return { removed_items.size(), std::nullopt };
	}

	auto ThreadPool::lock(const bool& lock_condition) -> void
	{
		if (job_pool_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "cannot lock null JobPool");

			return;
		}

		job_pool_->lock(lock_condition);
	}

	auto ThreadPool::lock(void) -> bool
	{
		if (job_pool_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "cannot check null JobPool");

			return false;
		}

		return job_pool_->lock();
	}

	auto ThreadPool::thread_title(const std::string& title) -> void
	{
		std::scoped_lock<std::mutex> lock(mutex_);

		thread_title_ = title;

		if (job_pool_ != nullptr)
		{
			job_pool_->job_pool_title(title);
		}

		for (auto& worker : thread_workers_)
		{
			if (worker == nullptr)
			{
				continue;
			}

			worker->worker_title(title);
		}
	}

	auto ThreadPool::thread_title(void) -> const std::string { return thread_title_; }

	auto ThreadPool::start(void) -> std::tuple<bool, std::optional<std::string>>
	{
		std::scoped_lock<std::mutex> lock(mutex_);

		if (working_.load())
		{
			return { false, "already started" };
		}

		for (auto& worker : thread_workers_)
		{
			if (worker == nullptr)
			{
				continue;
			}

			auto [started, start_error] = worker->start();
			if (!started)
			{
				return { false, start_error };
			}
		}

		working_.store(true);

		return { true, std::nullopt };
	}

	auto ThreadPool::pause(const bool& pause) -> void
	{
		std::scoped_lock<std::mutex> lock(mutex_);

		pause_.store(pause);

		for (auto& worker : thread_workers_)
		{
			if (worker == nullptr)
			{
				continue;
			}

			worker->pause(pause_.load());
		}
	}

	auto ThreadPool::stop(const bool& stop_immediately) -> std::tuple<bool, std::optional<std::string>>
	{
		std::vector<std::shared_ptr<ThreadWorker>> workers;

		{
			std::scoped_lock<std::mutex> lock(mutex_);

			if (!working_.load())
			{
				return { false, "not started" };
			}

			job_pool_->lock(true);

			if (stop_immediately)
			{
				job_pool_->clear();
			}

			workers = thread_workers_;
		}

		// 워커 join 은 mutex_ 를 놓고 한다. 잡이 자기 자신을 다시 push 하는 재등록 패턴에서는
		// 워커 스레드가 JobPool::push -> ThreadPool::notify_callback 안에서 같은 mutex_ 를 기다리는데,
		// join 하는 쪽이 그 mutex_ 를 들고 있으면 서로를 기다려 교착한다.
		// (실측: QueueManager sweep 재등록 시 TestQueueManager 40회 중 12회 교착)
		//
		// 단, join 자체는 stop_mutex_ 로 직렬화한다. mutex_ 를 놓은 탓에 두 스레드가 동시에
		// stop() 에 들어오면 같은 std::thread 를 두 번 join 해 abort 할 수 있다.
		{
			std::scoped_lock<std::mutex> stop_lock(stop_mutex_);

			for (auto& worker : workers)
			{
				if (worker == nullptr)
				{
					continue;
				}

				auto [stopped, stop_error] = worker->stop();
				if (!stopped)
				{
					return { false, stop_error };
				}
			}
		}

		job_pool_->lock(false);

		working_.store(false);

		return { true, std::nullopt };
	}

	auto ThreadPool::job_pool(void) -> std::shared_ptr<JobPool> { return job_pool_; }

	auto ThreadPool::notify_callback(const JobPriorities& priority) -> void
	{
		Logger::handle().write(LogTypes::Sequence, std::format("notify one for {} priority", priority_string(priority)));

		std::scoped_lock<std::mutex> lock(mutex_);

		for (auto& worker : thread_workers_)
		{
			if (worker == nullptr)
			{
				continue;
			}

			worker->notify_one(priority);
		}
	}
} // namespace Thread
