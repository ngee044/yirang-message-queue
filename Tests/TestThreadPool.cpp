#include "ThreadPool.h"

#include "Job.h"
#include "ThreadWorker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// 같은 우선순위의 워커를 로그에서 구분하려면 push() 가 호출자가 지은 이름을 지워서는 안 된다.
// 과거 push() 는 제목을 무조건 "[ LongTerm ] ThreadWorker on <pool>" 로 덮어써서
// 같은 우선순위의 워커가 모두 한 이름이 되었고, 어느 워커가 join 대기 중인지 알 수 없었다.
namespace
{
	auto make_worker(const std::string& title) -> std::shared_ptr<Thread::ThreadWorker>
	{
		return std::make_shared<Thread::ThreadWorker>(std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm }, title);
	}
}

TEST(ThreadPoolTest, PushKeepsCallerGivenWorkerTitle)
{
	auto pool = std::make_shared<Thread::ThreadPool>("QueueManager");

	auto first_worker = make_worker("queue_manager_sweep_worker_1");
	auto second_worker = make_worker("queue_manager_sweep_worker_2");
	pool->push(first_worker);
	pool->push(second_worker);

	EXPECT_EQ(first_worker->worker_title(), "queue_manager_sweep_worker_1");
	EXPECT_EQ(second_worker->worker_title(), "queue_manager_sweep_worker_2");
	EXPECT_NE(first_worker->worker_title(), second_worker->worker_title()) << "같은 우선순위 워커가 한 이름으로 뭉개지면 로그로 구분할 수 없다";
}

TEST(ThreadPoolTest, PushNamesUnnamedWorkerFromPriorityAndPool)
{
	auto pool = std::make_shared<Thread::ThreadPool>("QueueManager");

	auto unnamed = std::make_shared<Thread::ThreadWorker>(std::vector<Thread::JobPriorities>{ Thread::JobPriorities::LongTerm });
	ASSERT_EQ(unnamed->worker_title(), Thread::default_worker_title);

	pool->push(unnamed);

	EXPECT_EQ(unnamed->worker_title(), "[ LongTerm ] ThreadWorker on QueueManager");
}

// 자기 자신을 다시 push 하여 반복하는 잡(재등록 패턴) + stop() 조합의 교착 회귀 테스트.
// ThreadPool::stop() 이 mutex_ 를 들고 워커를 join 하면, 워커 안에서 도는 잡의
// JobPool::push -> ThreadPool::notify_callback 이 같은 mutex_ 를 기다려 교착한다.
// 수정 전 실측 재현률: TestQueueManager 40회 중 12회.
//
// stop_immediately 두 모드를 모두 돈다. QueueManager/MailboxHandler 는 stop(true) 를 쓰므로
// 프로덕션 경로가 반드시 포함되어야 한다.
class ThreadPoolRepushTest : public testing::TestWithParam<bool>
{
};

TEST_P(ThreadPoolRepushTest, StopDoesNotDeadlockWithSelfRepushingJob)
{
	const bool stop_immediately = GetParam();

	for (int32_t attempt = 0; attempt < 30; ++attempt)
	{
		auto pool = std::make_shared<Thread::ThreadPool>("RepushPool");
		pool->push(make_worker("repush_worker_1"));
		pool->push(make_worker("repush_worker_2"));

		ASSERT_TRUE(pool->start().has_value());

		// 잡이 참조하는 상태는 공유 구조체로 묶어 값 캡처한다. 지역변수 참조 캡처는
		// 잡이 반복 경계를 넘어 살아 있을 때 죽은 참조가 된다.
		struct shared_state
		{
			std::atomic<bool> running{ true };
			std::atomic<int32_t> cycles{ 0 };
		};
		auto state = std::make_shared<shared_state>();

		// 잡은 한 주기만 돌고 자신을 다시 push 한다. stop() 이 JobPool 을 잠그면 push 가 거부되어
		// 재등록이 멈추므로, push 실패는 정상 종료 신호다.
		//
		// 클로저가 자신을 담은 shared_ptr 를 강하게 잡으면 순환 참조가 되어 매 반복 누수한다.
		// weak_ptr 로 잡아 순환을 끊는다.
		auto repush = std::make_shared<std::function<std::expected<void, std::string>(void)>>();
		std::weak_ptr<std::function<std::expected<void, std::string>(void)>> weak_repush = repush;

		*repush = [pool, state, weak_repush]() -> std::expected<void, std::string>
		{
			if (!state->running.load())
			{
				return {};
			}

			state->cycles.fetch_add(1);

			auto self = weak_repush.lock();
			if (!self)
			{
				return {};
			}

			pool->push(std::make_shared<Thread::Job>(Thread::JobPriorities::LongTerm, *self, "repush_job"));

			return {};
		};

		pool->push(std::make_shared<Thread::Job>(Thread::JobPriorities::LongTerm, *repush, "repush_job"));

		// 재등록이 실제로 몇 바퀴 돈 뒤 — 즉 push 경합이 열린 상태에서 — stop 을 때린다.
		// 맨 sleep 은 느린 CI 에서 한 바퀴도 못 돌 수 있으므로 실제 진행을 기다린다.
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (state->cycles.load() < 3 && std::chrono::steady_clock::now() < deadline)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		ASSERT_GE(state->cycles.load(), 3) << "재등록 루프가 돌지 않아 경합 구간을 열지 못했다 (시도 " << attempt << ")";

		state->running.store(false);
		auto stop_result = pool->stop(stop_immediately);

		EXPECT_TRUE(stop_result.has_value()) << (stop_result ? "" : stop_result.error());
	}
}

INSTANTIATE_TEST_SUITE_P(StopModes, ThreadPoolRepushTest, testing::Values(false, true),
						 [](const testing::TestParamInfo<bool>& info) { return info.param ? "StopImmediately" : "StopDraining"; });

// notify_one() 은 thread_ / priorities_ 를 워커 mutex_ 아래에서 읽어야 한다.
// 과거에는 락 없이 읽었고, ThreadPool::stop() 이 풀 mutex_ 를 들고 join 하는 덕에
// notify_callback -> notify_one 과 우연히 직렬화되어 가려져 있었다. 교착을 없애려
// join 을 풀 mutex_ 밖으로 뺀 뒤 그 보호가 사라져, 종료 중 push 가 thread_.reset() 과
// 경합했다 — 전체 ctest 11회 중 3회, 서로 다른 테스트가 임의로 행업했다.
//
// 이 테스트는 stop() 이 join 하는 동안 다른 스레드가 계속 push 하게 만들어 notify_one
// 경로를 종료와 겹친다. TSan 빌드에서 돌리면 레이스를 직접 잡는다.
TEST(ThreadPoolTest, ConcurrentPushDuringStopIsSafe)
{
	for (int32_t attempt = 0; attempt < 20; ++attempt)
	{
		auto pool = std::make_shared<Thread::ThreadPool>("NotifyRacePool");
		pool->push(make_worker("notify_worker_1"));
		pool->push(make_worker("notify_worker_2"));
		ASSERT_TRUE(pool->start().has_value());

		auto pushing = std::make_shared<std::atomic<bool>>(true);

		// 외부 스레드가 stop() 과 겹치도록 계속 push 한다. 종료 중 push 거부는 정상이다.
		std::thread pusher(
			[pool, pushing]()
			{
				while (pushing->load())
				{
					pool->push(std::make_shared<Thread::Job>(
						Thread::JobPriorities::LongTerm,
						[]() -> std::expected<void, std::string> { return {}; },
						"noop_job"));
				}
			});

		std::this_thread::sleep_for(std::chrono::milliseconds(5));

		auto stop_result = pool->stop(true);
		EXPECT_TRUE(stop_result.has_value()) << (stop_result ? "" : stop_result.error());

		pushing->store(false);
		pusher.join();
	}
}
