// yirangmq-demo-worker — 설비의 처리 워커 역할.
//
// 기존 yirangmq-cli-consumer 는 한 번 실행에 한 요청만 보내므로, 소비한 뒤 messageKey 와
// leaseId 를 사람이 손으로 옮겨야 ack 을 할 수 있다. 이 프로그램은 consume -> 처리 ->
// ack/nack 을 스스로 이어서 수행하는 상주 프로세스이며, 그래서 lease 만료 재배달과
// 늦은 ack 거부(fencing)를 사람 개입 없이 보여줄 수 있다.

#include "MqCall.h"

#include "ArgumentParser.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <format>
#include <string>

using json = nlohmann::json;

using namespace Utilities;

namespace
{
	std::atomic<bool> running{ true };

	extern "C" void handle_signal(int32_t) { running.store(false); }

	auto print_usage(void) -> void
	{
		std::string usage = R"(yirangmq-demo-worker — 상주 소비 데모 프로세스

Usage: yirangmq-demo-worker [options]

Options:
  --ipc-root <path>      IPC 루트 (기본: ./ipc)
  --queue <name>         큐 이름 (기본: telemetry)
  --consumer-id <id>     consumerId 이자 clientId (기본: demo-worker-01)
  --visibility <sec>     소비 시 가시성 타임아웃 (기본: 30)
  --poll-ms <ms>         큐가 비었을 때 재시도 간격 (기본: 250)
  --idle-exit-ms <ms>    이 시간만큼 계속 비어 있으면 정상 종료 (기본: 0 = 무한 대기)
  --max-messages <n>     이만큼 정산하면 종료 (기본: 0 = 무제한)
  --work-ms <ms>         메시지당 작업 시간 시뮬레이션. lease 가 모자라면 extend_lease 호출 (기본: 0)
  --stall-ms <ms>        lease 를 쥔 채 이만큼 멈춘 뒤 ack 을 시도한다. 가시성보다 크게 주면
                         lease 를 잃고 ack 이 거부되는 장면을 재현한다 (기본: 0)
  --ignore-fault         fault 필드가 있어도 정상 처리한다. 설비를 정비한 뒤 DLQ 메시지를
                         재투입하는 상황에 쓴다 (기본: 꺼짐)
  --timeout <ms>         응답 대기 시간 (기본: 5000)
  --help                 도움말

consumerId 는 lease 소유자다. ack/nack/extend_lease 는 소비할 때와 같은 값이어야 하며,
이 프로그램은 clientId 로도 같은 값을 사용한다.
)";
		Demo::say("usage", usage);
	}
}

auto main(int argc, char* argv[]) -> int
{
	Demo::enable_line_buffered_stdout();

	ArgumentParser args(argc, argv);

	if (args.to_string("--help").has_value())
	{
		print_usage();
		return 0;
	}

	const auto ipc_root = args.to_string("--ipc-root").value_or("./ipc");
	const auto queue = args.to_string("--queue").value_or("telemetry");
	const auto consumer_id = args.to_string("--consumer-id").value_or("demo-worker-01");
	const auto visibility_sec = args.to_int("--visibility").value_or(30);
	const auto poll_ms = args.to_int("--poll-ms").value_or(250);
	const auto idle_exit_ms = args.to_long("--idle-exit-ms").value_or(0);
	const auto max_messages = args.to_int("--max-messages").value_or(0);
	const auto work_ms = args.to_long("--work-ms").value_or(0);
	const auto stall_ms = args.to_long("--stall-ms").value_or(0);
	const auto ignore_fault = args.to_string("--ignore-fault").has_value();
	const auto timeout_ms = args.to_int("--timeout").value_or(5000);

	std::signal(SIGINT, handle_signal);
	std::signal(SIGTERM, handle_signal);

	const Demo::MqCall mq(ipc_root, consumer_id, timeout_ms);

	json ask;
	ask["queue"] = queue;
	ask["consumerId"] = consumer_id;
	ask["visibilityTimeoutSec"] = visibility_sec;

	Demo::say(consumer_id, std::format("기동 — 큐 {} / 가시성 {}초 / 폴링 {}ms", queue, visibility_sec, poll_ms));

	int32_t acked = 0;
	int32_t nacked = 0;
	int32_t fenced = 0;
	int32_t unexpected = 0;
	int32_t settled = 0;

	int64_t idle_since_ms = 0;
	int64_t idle_notices = 0;

	while (running.load())
	{
		if (max_messages > 0 && settled >= max_messages)
		{
			break;
		}

		auto leased = mq.call("consume_next", ask);

		if (!leased.ok)
		{
			Demo::say(consumer_id, std::format("소비 요청 실패 — {}", leased.error.value_or("unknown")));
			Demo::sleep_ms(poll_ms, running);

			continue;
		}

		// 빈 큐는 오류가 아니다. ok 는 true 이고 data.message 가 null 로 온다.
		if (!leased.data.contains("message") || leased.data["message"].is_null())
		{
			const auto now = MailboxIPC::current_time_ms();
			if (idle_since_ms == 0)
			{
				idle_since_ms = now;
			}

			if (idle_exit_ms > 0 && (now - idle_since_ms) >= idle_exit_ms)
			{
				Demo::say(consumer_id, std::format("{}ms 동안 큐가 비어 있어 종료합니다", idle_exit_ms));

				break;
			}

			// 대기 로그가 화면을 덮지 않도록 간격을 두고만 알린다.
			if ((idle_notices % 20) == 0)
			{
				Demo::say(consumer_id, "대기 중 — 큐가 비어 있습니다 (오류가 아닙니다)");
			}
			++idle_notices;

			Demo::sleep_ms(poll_ms, running);

			continue;
		}

		idle_since_ms = 0;
		idle_notices = 0;

		const auto& message = leased.data["message"];
		const auto message_key = message.value("messageKey", std::string{});
		const auto attempt = message.value("attempt", 0);
		const auto payload = message.contains("payload") ? message["payload"] : json::object();

		std::string lease_id;
		int64_t lease_until_ms = 0;
		if (leased.data.contains("lease") && leased.data["lease"].is_object())
		{
			lease_id = leased.data["lease"].value("leaseId", std::string{});
			lease_until_ms = leased.data["lease"].value("leaseUntil", static_cast<int64_t>(0));
		}

		Demo::say(consumer_id, std::format("수신 attempt={} {} {}", attempt, message_key, payload.dump()));

		json settle;
		settle["messageKey"] = message_key;
		settle["leaseId"] = lease_id;
		settle["consumerId"] = consumer_id;

		// 1) lease 를 쥔 채 멈추는 모드. 가시성보다 오래 멈추면 데몬이 lease 를 회수하고
		//    다른 워커가 같은 메시지를 받는다. 이 프로세스의 뒤늦은 ack 은 거부되어야 한다.
		if (stall_ms > 0)
		{
			Demo::say(consumer_id, std::format("일부러 {}ms 멈춥니다 — 처리 중 정지 상황을 재현합니다", stall_ms));
			Demo::sleep_ms(stall_ms, running);

			auto late = mq.call("ack", settle);
			++settled;

			if (!late.ok)
			{
				++fenced;
				Demo::say(consumer_id, std::format("늦은 ack 거부됨 (기대한 결과) — {}", late.error.value_or("unknown")));
				Demo::say(consumer_id, "만료된 lease 로는 확정할 수 없으므로 같은 작업이 두 번 반영되지 않습니다");
			}
			else
			{
				++unexpected;
				Demo::say(consumer_id, "예상과 다릅니다: 만료된 lease 의 ack 이 승인되었습니다");
			}

			continue;
		}

		// 2) 작업 시간 시뮬레이션. lease 가 끝나기 전에 갱신한다.
		bool lease_lost = false;

		if (work_ms > 0)
		{
			int64_t worked_ms = 0;
			while (running.load() && worked_ms < work_ms)
			{
				// extend_lease 응답에는 새 만료 시각이 없으므로 직접 계산해 둔다.
				if (lease_until_ms > 0 && (lease_until_ms - MailboxIPC::current_time_ms()) < 1500)
				{
					json extend = settle;
					extend["visibilityTimeoutSec"] = visibility_sec;

					auto extended = mq.call("extend_lease", extend);
					if (extended.ok)
					{
						lease_until_ms = MailboxIPC::current_time_ms() + (static_cast<int64_t>(visibility_sec) * 1000);
						Demo::say(consumer_id, std::format("lease 연장 — 앞으로 {}초", visibility_sec));
					}
					else
					{
						// 이미 만료된 lease 는 연장할 수 없다. 이 시점에 메시지는 다른 소비자에게
						// 재배달되었을 수 있으므로 확정을 시도하지 않고 결과를 버린다 — 확정을
						// 시도하면 거부될 뿐이고, 성공한다면 오히려 이중 처리가 된다.
						Demo::say(consumer_id, std::format("lease 연장 실패 — {}", extended.error.value_or("unknown")));
						Demo::say(consumer_id, "이미 만료된 lease 는 연장할 수 없습니다. 작업 결과를 버립니다");

						lease_lost = true;

						break;
					}
				}

				Demo::sleep_ms(200, running);
				worked_ms += 200;
			}
		}

		if (lease_lost)
		{
			continue;
		}

		// 3) 업무 판정. 스키마는 통과했지만 처리할 수 없는 프레임은 애플리케이션이 판단한다.
		if (payload.contains("fault") && ignore_fault)
		{
			Demo::say(consumer_id, "fault 프레임이지만 --ignore-fault 로 정상 처리합니다 (설비 정비 후 재투입)");
		}
		else if (payload.contains("fault"))
		{
			json nack = settle;
			nack["reason"] = std::format("sensor fault: {}", payload["fault"].dump());
			nack["requeue"] = true;

			auto result = mq.call("nack", nack);
			++settled;

			if (result.ok)
			{
				++nacked;
				Demo::say(consumer_id, std::format("처리 실패 -> nack requeue=true (attempt={})", attempt));
			}
			else
			{
				++unexpected;
				Demo::say(consumer_id, std::format("nack 실패 — {}", result.error.value_or("unknown")));
			}

			continue;
		}

		// 4) 정상 처리 확정.
		auto result = mq.call("ack", settle);
		++settled;

		if (result.ok)
		{
			++acked;
			Demo::say(consumer_id, std::format("처리 완료 -> ack {}", message_key));
		}
		else
		{
			++unexpected;
			Demo::say(consumer_id, std::format("ack 실패 — {}", result.error.value_or("unknown")));
		}
	}

	Demo::say(consumer_id, std::format("요약 — ack {}건 / nack {}건 / 거부 확인 {}건 / 예상 외 {}건", acked, nacked, fenced, unexpected));

	return unexpected > 0 ? 1 : 0;
}
