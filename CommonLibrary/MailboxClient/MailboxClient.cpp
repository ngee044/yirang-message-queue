#include "MailboxClient.h"

#include "File.h"
#include "Generator.h"
#include "Logger.h"

#include <chrono>
#include <expected>
#include <filesystem>
#include <format>
#include <fstream>
#include <thread>

using json = nlohmann::json;

using namespace Utilities;

namespace MailboxIPC
{

auto current_time_ms(void) -> int64_t
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
}

auto atomic_write(const std::string& target_path, const std::string& content) -> bool
{
	auto temp_path = target_path + ".tmp";

	// Durable, fail-closed write: on any failure (e.g. ENOSPC) abort before rename so a
	// partial temp is never promoted over a valid target. (Defect D-02, sibling of the
	// FileSystemAdapter fix.)
	auto write_result = write_file_durable(temp_path, content);
	if (!write_result)
	{
		std::error_code rm_ec;
		std::filesystem::remove(temp_path, rm_ec);
		Logger::handle().write(
			LogTypes::Error,
			std::format("Durable write failed: {}", write_result.error())
		);
		return false;
	}

	std::error_code ec;
	std::filesystem::rename(temp_path, target_path, ec);
	if (ec)
	{
		std::filesystem::remove(temp_path, ec);
		Logger::handle().write(
			LogTypes::Error,
			std::format("Rename failed: {}", ec.message())
		);
		return false;
	}

	fsync_parent_directory(target_path);

	return true;
}

auto send_request(
	const ClientConfig& config,
	const std::string& client_id,
	const std::string& command,
	const json& payload,
	int32_t timeout_ms
) -> std::expected<json, std::string>
{
	auto request_id = Generator::guid();
	auto now = current_time_ms();
	auto deadline = now + timeout_ms;

	// Build request JSON
	json request;
	request["requestId"] = request_id;
	request["clientId"] = client_id;
	request["command"] = command;
	request["timestampMs"] = now;
	request["deadlineMs"] = deadline;
	request["payload"] = payload;

	// Ensure directories exist
	std::filesystem::path requests_path = config.root;
	requests_path /= config.requests_dir;

	std::filesystem::path responses_path = config.root;
	responses_path /= config.responses_dir;
	responses_path /= client_id;

	std::error_code ec;
	std::filesystem::create_directories(requests_path, ec);
	std::filesystem::create_directories(responses_path, ec);

	// Write request file
	auto request_file = (requests_path / std::format("{}.json", request_id)).string();
	if (!atomic_write(request_file, request.dump(2)))
	{
		return std::unexpected("failed to write request");
	}

	// Wait for response
	auto response_file = (responses_path / std::format("{}.json", request_id)).string();
	auto start_time = current_time_ms();

	while (current_time_ms() - start_time < timeout_ms)
	{
		if (std::filesystem::exists(response_file, ec))
		{
			std::ifstream file(response_file);
			if (file.is_open())
			{
				std::string content((std::istreambuf_iterator<char>(file)),
					std::istreambuf_iterator<char>());
				file.close();

				// Delete response file after reading
				std::filesystem::remove(response_file, ec);

				try
				{
					return json::parse(content);
				}
				catch (const json::exception& e)
				{
					return std::unexpected(std::format("response parse error: {}", e.what()));
				}
			}
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	// Timed out. Remove our own request file so it does not accumulate in requests/ when the
	// daemon is down or unresponsive — the client-side is the only party that can clean it up
	// in that case. If the daemon already moved it to processing, this is a harmless no-op. (D-15)
	std::filesystem::remove(request_file, ec);

	return std::unexpected("timeout waiting for response");
}

} // namespace MailboxIPC
