#pragma once

#include <nlohmann/json.hpp>

#include <cstdint>
#include <string>
#include <tuple>

namespace MailboxIPC
{

struct ClientConfig
{
	std::string root = "./ipc";
	std::string requests_dir = "requests";
	std::string responses_dir = "responses";
	int32_t timeout_ms = 30000;
};

auto send_request(
	const ClientConfig& config,
	const std::string& client_id,
	const std::string& command,
	const nlohmann::json& payload,
	int32_t timeout_ms
) -> std::tuple<bool, nlohmann::json>;

auto atomic_write(const std::string& target_path, const std::string& content) -> bool;

auto current_time_ms(void) -> int64_t;

} // namespace MailboxIPC
