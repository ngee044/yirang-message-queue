// yirangmq-demo-sensor — 설비의 센서 수집 프로세스 역할.
//
// 주기적으로 텔레메트리 프레임을 telemetry 큐로 발행한다. 다운스트림 워커가 살아 있는지
// 신경쓰지 않는다는 점이 MQ 를 쓰는 이유이며, 이 프로그램이 그것을 그대로 보여준다.

#include "MqCall.h"

#include "ArgumentParser.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <cstdint>
#include <format>
#include <string>

using json = nlohmann::json;

using namespace Utilities;

namespace
{
	// sleep_ms 는 중단 플래그를 받는다. 센서는 신호를 다루지 않으므로 항상 참인 상수를 넘긴다.
	const std::atomic<bool> always_running{ true };

	auto print_usage(void) -> void
	{
		std::string usage = R"(yirangmq-demo-sensor — 텔레메트리 발행 데모 프로세스

Usage: yirangmq-demo-sensor [options]

Options:
  --ipc-root <path>      IPC 루트 (기본: ./ipc)
  --queue <name>         큐 이름 (기본: telemetry)
  --client-id <id>       클라이언트 ID (기본: demo-sensor)
  --device-id <id>       프레임의 deviceId (기본: sensor-01)
  --count <n>            발행 건수 (기본: 3)
  --interval-ms <ms>     발행 간격 (기본: 250)
  --priority <n>         우선순위, 클수록 먼저 배달 (기본: 0)
  --target <id>          지정 배달 대상 consumerId (기본: 없음 = 아무 워커)
  --bad-frame <index>    이 인덱스의 프레임에서 deviceId 를 빼 스키마 검증 실패를 유도 (기본: -1 = 없음)
  --fault-frame <index>  이 인덱스의 프레임에 fault 필드를 넣어 워커가 실패 판정하게 한다 (기본: -1 = 없음)
  --timeout <ms>         응답 대기 시간 (기본: 5000)
  --help                 도움말

주의: publish 의 message 는 JSON 객체가 아니라 JSON 문자열이다. 이 프로그램은
frame.dump() 를 넣는다. 객체를 그대로 넣으면 데몬이 ERR_PARSE_ERROR 로 거부한다.
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
	const auto client_id = args.to_string("--client-id").value_or("demo-sensor");
	const auto device_id = args.to_string("--device-id").value_or("sensor-01");
	const auto target = args.to_string("--target").value_or("");
	const auto count = args.to_int("--count").value_or(3);
	const auto interval_ms = args.to_int("--interval-ms").value_or(250);
	const auto priority = args.to_int("--priority").value_or(0);
	const auto bad_frame = args.to_int("--bad-frame").value_or(-1);
	const auto fault_frame = args.to_int("--fault-frame").value_or(-1);
	const auto timeout_ms = args.to_int("--timeout").value_or(5000);

	const Demo::MqCall mq(ipc_root, client_id, timeout_ms);

	auto health = mq.call("health", json::object());
	if (!health.ok)
	{
		Demo::say(client_id, std::format("데몬에 연결할 수 없습니다 — {}", health.error.value_or("unknown")));
		Demo::say(client_id, std::format("MainMQ 가 실행 중이고 IPC 루트가 같은지 확인하십시오 (현재: {})", ipc_root));

		return 1;
	}

	int32_t published = 0;
	int32_t rejected = 0;
	int32_t unexpected = 0;

	for (int32_t index = 0; index < count; ++index)
	{
		const bool omit_device_id = (index == bad_frame);
		const bool mark_fault = (index == fault_frame);

		json frame;
		if (!omit_device_id)
		{
			frame["deviceId"] = device_id;
		}
		frame["timestamp"] = MailboxIPC::current_time_ms() / 1000;
		frame["temp"] = 24.0 + (index * 0.7);
		if (mark_fault)
		{
			frame["fault"] = "transient";
		}

		json payload;
		payload["queue"] = queue;
		payload["message"] = frame.dump();
		payload["priority"] = priority;
		if (!target.empty())
		{
			payload["targetConsumerId"] = target;
		}

		auto result = mq.call("publish", payload);

		if (result.ok)
		{
			++published;

			const auto message_key = result.data.value("messageKey", std::string{});
			Demo::say(client_id, std::format("발행 {}{}", frame.dump(), target.empty() ? "" : std::format("  지정 대상 {}", target)));
			Demo::say(client_id, std::format("     -> {}", message_key));

			if (omit_device_id)
			{
				++unexpected;
				Demo::say(client_id, "예상과 다릅니다: deviceId 없는 프레임이 통과했습니다 (스키마가 등록되지 않은 큐입니까?)");
			}
		}
		else
		{
			++rejected;

			if (omit_device_id)
			{
				Demo::say(client_id, std::format("거부 (의도된 실패) {}", result.error.value_or("unknown")));
			}
			else
			{
				++unexpected;
				Demo::say(client_id, std::format("거부 (예상과 다릅니다) {}", result.error.value_or("unknown")));
			}
		}

		if (index + 1 < count)
		{
			Demo::sleep_ms(interval_ms, always_running);
		}
	}

	Demo::say(client_id, std::format("요약 — 발행 {}건 / 거부 {}건", published, rejected));

	return unexpected > 0 ? 1 : 0;
}
