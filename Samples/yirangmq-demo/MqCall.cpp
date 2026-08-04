#include "MqCall.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdio>
#include <format>
#include <iostream>
#include <thread>

using json = nlohmann::json;

namespace
{
	auto steady_now_ms(void) -> int64_t
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}

	const int64_t process_start_ms = steady_now_ms();
}

namespace Demo
{
	MqCall::MqCall(const std::string& ipc_root, const std::string& client_id, int32_t timeout_ms)
		: client_id_(client_id)
	{
		config_.root = ipc_root;
		config_.timeout_ms = timeout_ms;
	}

	auto MqCall::call(const std::string& command, const json& payload) const -> CallResult
	{
		CallResult result;

		auto [sent, response] = MailboxIPC::send_request(config_, client_id_, command, payload, config_.timeout_ms);

		if (!sent)
		{
			std::string detail = response.dump();
			if (response.contains("error") && response["error"].is_string())
			{
				detail = response["error"].get<std::string>();
			}

			result.error = std::format("TRANSPORT: {}", detail);

			return result;
		}

		if (!response.value("ok", false))
		{
			std::string code = "ERR_UNSPECIFIED";
			std::string message;

			if (response.contains("error") && response["error"].is_object())
			{
				code = response["error"].value("code", code);
				message = response["error"].value("message", std::string{});
			}

			result.error = std::format("{}: {}", code, message);

			return result;
		}

		result.ok = true;

		// ack / nack / extend_lease 의 성공 응답은 data 가 빈 객체다. 호출부가 무조건
		// data 를 만져도 예외가 나지 않도록 빈 객체를 유지한다.
		if (response.contains("data") && response["data"].is_object())
		{
			result.data = response["data"];
		}

		return result;
	}

	auto elapsed_text(void) -> std::string { return std::format("t+{:>6.1f}s", (steady_now_ms() - process_start_ms) / 1000.0); }

	auto say(const std::string& actor, const std::string& message) -> void
	{
		// 데모는 여러 프로세스가 같은 터미널에 동시에 쓴다. 한 줄을 하나의 문자열로 만들어
		// 한 번에 내보내야 줄이 서로 끼어들지 않는다. Utilities::Logger 를 쓰지 않는 이유는
		// 로그 접두사(날짜·마이크로초·레벨)가 시연에서 흐름을 가리기 때문이다.
		std::cout << std::format("{}  {:<18} {}\n", elapsed_text(), actor, message) << std::flush;
	}

	auto enable_line_buffered_stdout(void) -> void
	{
		// 러너가 출력을 파이프로 받으면 stdout 이 블록 버퍼링으로 바뀌어 프로세스 간
		// 출력 순서가 뭉쳐 보인다. std::cout 은 기본적으로 C stdio 와 동기화되므로
		// 여기서 줄 단위 버퍼링으로 고정하면 그대로 적용된다.
		std::setvbuf(stdout, nullptr, _IOLBF, 0);
	}

	auto sleep_ms(int64_t duration_ms, const std::atomic<bool>& keep_going) -> void
	{
		const auto deadline = steady_now_ms() + duration_ms;

		while (keep_going.load() && steady_now_ms() < deadline)
		{
			const auto remain = deadline - steady_now_ms();
			std::this_thread::sleep_for(std::chrono::milliseconds(remain > 50 ? 50 : remain));
		}
	}
}
