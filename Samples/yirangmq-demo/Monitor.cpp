// yirangmq-demo-monitor — 운영자 콘솔 역할.
//
// 큐 상태를 한 줄로 압축해 보여주고, 러너 스크립트가 sleep 상수 대신 상태 조건으로
// 다음 단계를 진행할 수 있도록 wait 게이트를 제공한다. 상태가 바뀔 때만 출력하므로
// 출력 자체가 곧 상태 전이 타임라인이 된다.

#include "MqCall.h"

#include "ArgumentParser.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <cstdint>
#include <format>
#include <string>
#include <vector>

using json = nlohmann::json;

using namespace Utilities;

namespace
{
	const std::atomic<bool> always_running{ true };

	const std::vector<std::string> state_names = { "ready", "inflight", "delayed", "dlq" };

	struct Snapshot
	{
		int64_t ready = -1;
		int64_t inflight = -1;
		int64_t delayed = -1;
		int64_t dlq = -1;
		bool has_policy = false;

		auto operator==(const Snapshot& other) const -> bool
		{
			return ready == other.ready && inflight == other.inflight && delayed == other.delayed && dlq == other.dlq;
		}

		auto text(void) const -> std::string { return std::format("ready={} inflight={} delayed={} dlq={}", ready, inflight, delayed, dlq); }

		auto of(const std::string& state) const -> int64_t
		{
			if (state == "ready")
			{
				return ready;
			}
			if (state == "inflight")
			{
				return inflight;
			}
			if (state == "delayed")
			{
				return delayed;
			}
			if (state == "dlq")
			{
				return dlq;
			}

			return -1;
		}
	};

	auto read_status(const Demo::MqCall& mq, const std::string& queue, Snapshot& out, std::string& error) -> bool
	{
		json payload;
		payload["queue"] = queue;

		auto result = mq.call("status", payload);
		if (!result.ok)
		{
			error = result.error.value_or("unknown");

			return false;
		}

		// 미등록 큐도 ERR_QUEUE_NOT_FOUND 가 아니라 ok=true 로 오고, metrics 는 전부 0 이며
		// policy 키가 없다. 그래서 policy 유무가 "큐가 등록되었는지"의 진단이 된다.
		const auto metrics = result.data.contains("metrics") ? result.data["metrics"] : json::object();

		out.ready = metrics.value("ready", static_cast<int64_t>(0));
		out.inflight = metrics.value("inflight", static_cast<int64_t>(0));
		out.delayed = metrics.value("delayed", static_cast<int64_t>(0));
		out.dlq = metrics.value("dlq", static_cast<int64_t>(0));
		out.has_policy = result.data.contains("policy");

		return true;
	}

	auto print_usage(void) -> void
	{
		std::string usage = R"(yirangmq-demo-monitor — 큐 상태 관측 데모 프로세스

Usage: yirangmq-demo-monitor <command> [options]

Commands:
  watch      상태가 바뀔 때만 한 줄씩 출력한다 (기본)
  once       현재 상태와 등록된 정책 유무를 한 번 출력한다
  dlq        DLQ 목록을 표로 출력한다
  reprocess  DLQ 메시지를 ready 로 되돌린다
  wait       상태 조건이 충족될 때까지 기다린다 (러너 스크립트용 게이트)
  help       도움말

Options:
  --ipc-root <path>      IPC 루트 (기본: ./ipc)
  --queue <name>         큐 이름 (기본: telemetry)
  --client-id <id>       클라이언트 ID (기본: demo-monitor)
  --interval-ms <ms>     watch/wait 폴링 간격 (기본: 400)
  --iterations <n>       watch 반복 횟수 (기본: 0 = 무한)
  --limit <n>            dlq 조회 개수 (기본: 100)
  --message-key <key>    reprocess 대상 (--first 로 대체 가능)
  --first                reprocess 대상을 DLQ 목록의 첫 항목으로 한다
  --state <name>         wait 대상 상태 — ready | inflight | delayed | dlq
  --count <n>            wait 조건: 해당 상태가 n 이상이 될 때까지
  --max <n>              wait 조건: 해당 상태가 n 이하가 될 때까지
  --timeout-ms <ms>      wait 제한 시간 (기본: 20000)
  --timeout <ms>         응답 대기 시간 (기본: 5000)
)";
		Demo::say("usage", usage);
	}
}

auto main(int argc, char* argv[]) -> int
{
	Demo::enable_line_buffered_stdout();

	ArgumentParser args(argc, argv);

	std::string command = "watch";
	if (argc >= 2)
	{
		std::string first = argv[1];
		if (first.rfind("--", 0) != 0)
		{
			command = first;
		}
	}

	if (command == "help" || args.to_string("--help").has_value())
	{
		print_usage();
		return 0;
	}

	const auto ipc_root = args.to_string("--ipc-root").value_or("./ipc");
	const auto queue = args.to_string("--queue").value_or("telemetry");
	const auto client_id = args.to_string("--client-id").value_or("demo-monitor");
	const auto interval_ms = args.to_int("--interval-ms").value_or(400);
	const auto iterations = args.to_int("--iterations").value_or(0);
	const auto timeout_ms = args.to_int("--timeout").value_or(5000);

	const Demo::MqCall mq(ipc_root, client_id, timeout_ms);

	if (command == "once")
	{
		Snapshot snapshot;
		std::string error;
		if (!read_status(mq, queue, snapshot, error))
		{
			Demo::say(client_id, std::format("상태 조회 실패 — {}", error));

			return 1;
		}

		Demo::say(client_id, std::format("{}  {}", queue, snapshot.text()));
		Demo::say(client_id, snapshot.has_policy ? "정책 등록됨 (queues[] 에 선언된 큐)" : "정책 없음 — 미등록 큐입니다. 스키마 검증과 TTL 이 적용되지 않습니다");

		return 0;
	}

	if (command == "watch")
	{
		Snapshot previous;
		int32_t rounds = 0;

		while (iterations == 0 || rounds < iterations)
		{
			Snapshot snapshot;
			std::string error;
			if (!read_status(mq, queue, snapshot, error))
			{
				Demo::say(client_id, std::format("상태 조회 실패 — {}", error));
			}
			else if (!(snapshot == previous))
			{
				Demo::say(client_id, snapshot.text());
				previous = snapshot;
			}

			++rounds;
			Demo::sleep_ms(interval_ms, always_running);
		}

		return 0;
	}

	if (command == "wait")
	{
		const auto state = args.to_string("--state").value_or("");
		const auto at_least = args.to_long("--count");
		const auto at_most = args.to_long("--max");
		const auto deadline_ms = MailboxIPC::current_time_ms() + args.to_long("--timeout-ms").value_or(20000);

		if (state.empty() || (!at_least.has_value() && !at_most.has_value()))
		{
			Demo::say(client_id, "--state 와 --count 또는 --max 가 필요합니다");

			return 2;
		}

		while (MailboxIPC::current_time_ms() < deadline_ms)
		{
			Snapshot snapshot;
			std::string error;
			if (read_status(mq, queue, snapshot, error))
			{
				const auto value = snapshot.of(state);

				if (at_least.has_value() && value >= at_least.value())
				{
					return 0;
				}
				if (at_most.has_value() && value <= at_most.value())
				{
					return 0;
				}
			}

			Demo::sleep_ms(interval_ms, always_running);
		}

		Demo::say(client_id, std::format("대기 시간 초과 — {} 조건을 만족하지 못했습니다", state));

		return 1;
	}

	if (command == "dlq")
	{
		json payload;
		payload["queue"] = queue;
		payload["limit"] = args.to_int("--limit").value_or(100);

		auto result = mq.call("list_dlq", payload);
		if (!result.ok)
		{
			Demo::say(client_id, std::format("DLQ 조회 실패 — {}", result.error.value_or("unknown")));

			return 1;
		}

		const auto count = result.data.value("count", 0);
		Demo::say(client_id, std::format("DLQ({}) {}건", queue, count));

		if (result.data.contains("messages") && result.data["messages"].is_array())
		{
			const auto now_ms = MailboxIPC::current_time_ms();
			for (const auto& entry : result.data["messages"])
			{
				const auto dlq_at = entry.value("dlqAt", static_cast<int64_t>(0));
				Demo::say(client_id,
					std::format("  {}  attempt={}  {}초 전  사유: {}", entry.value("messageKey", std::string{}), entry.value("attempt", 0),
						dlq_at > 0 ? (now_ms - dlq_at) / 1000 : 0, entry.value("reason", std::string{})));
			}
		}

		if (count > 0)
		{
			Demo::say(client_id, "list_dlq 응답에는 원본 payload 가 없습니다. 내용을 보려면 reprocess 후 소비하십시오");
		}

		return 0;
	}

	if (command == "reprocess")
	{
		auto message_key = args.to_string("--message-key");

		if (!message_key.has_value() && args.to_string("--first").has_value())
		{
			json list;
			list["queue"] = queue;
			list["limit"] = 1;

			auto listed = mq.call("list_dlq", list);
			if (listed.ok && listed.data.contains("messages") && listed.data["messages"].is_array() && !listed.data["messages"].empty())
			{
				message_key = listed.data["messages"][0].value("messageKey", std::string{});
			}
		}

		if (!message_key.has_value() || message_key.value().empty())
		{
			Demo::say(client_id, "--message-key 가 필요합니다 (또는 --first 로 DLQ 첫 항목 선택)");

			return 2;
		}

		json payload;
		payload["messageKey"] = message_key.value();

		auto result = mq.call("reprocess_dlq", payload);
		if (!result.ok)
		{
			Demo::say(client_id, std::format("재처리 실패 — {}", result.error.value_or("unknown")));

			return 1;
		}

		Demo::say(client_id, std::format("재처리 요청 완료 — {} (attempt 가 초기화되어 ready 로 돌아갑니다)", message_key.value()));

		return 0;
	}

	Demo::say(client_id, std::format("알 수 없는 명령: {}", command));
	print_usage();

	return 2;
}
