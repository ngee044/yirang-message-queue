#pragma once

#include "MailboxClient.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>

namespace Demo
{
	// send_request()는 두 단계로 실패한다. 전송/응답 수신이 실패하면 std::unexpected(오류 문자열)이고,
	// 명령 자체가 실패하면 성공 값 json의 ok가 false이며 json["error"]가
	// {code, message} 객체다. 두 경로를 호출부마다 구분하지 않도록 하나의 결과로 정규화한다.
	struct CallResult
	{
		bool ok = false;
		nlohmann::json data = nlohmann::json::object();
		std::optional<std::string> error;
	};

	class MqCall
	{
	public:
		MqCall(const std::string& ipc_root, const std::string& client_id, int32_t timeout_ms);

		auto call(const std::string& command, const nlohmann::json& payload) const -> CallResult;

	private:
		MailboxIPC::ClientConfig config_;
		std::string client_id_;
	};

	auto elapsed_text(void) -> std::string;

	auto say(const std::string& actor, const std::string& message) -> void;

	auto enable_line_buffered_stdout(void) -> void;

	// keep_going이 false로 바뀌면 즉시 깨어난다. 데모 프로세스가 Ctrl+C에 바로 반응하도록
	// 긴 대기를 짧은 조각으로 나눈다.
	auto sleep_ms(int64_t duration_ms, const std::atomic<bool>& keep_going) -> void;
}
