#pragma once

#include <expected>
#include <cstdint>
#include <locale>
#include <fstream>
#include <deque>
#include <string>
#include <vector>

namespace Utilities
{
	class File
	{
	public:
		File(void);
		File(const std::string& path, const std::ios_base::openmode& mode);
		File(const std::string& path, const std::ios_base::openmode& mode, const std::locale& locale);
		~File(void);

		auto open(const std::string& path, const std::ios_base::openmode& mode) -> std::expected<void, std::string>;
		auto open(const std::string& path, const std::ios_base::openmode& mode, const std::locale& locale)
			-> std::expected<void, std::string>;

		auto write_bytes(const uint8_t* bytes, const size_t& size) -> std::expected<void, std::string>;
		auto write_bytes(const std::deque<uint8_t>& bytes) -> std::expected<void, std::string>;
		auto write_bytes(const std::vector<uint8_t>& bytes) -> std::expected<void, std::string>;
		auto write_lines(const std::deque<std::string>& lines, const bool& append_newline = false)
			-> std::expected<void, std::string>;
		auto write_lines(const std::vector<std::string>& lines, const bool& append_newline = false)
			-> std::expected<void, std::string>;
		auto read_bytes(void) -> std::expected<std::vector<uint8_t>, std::string>;
		auto read_bytes(const size_t& index, const size_t& size) -> std::expected<std::vector<uint8_t>, std::string>;
		auto read_lines(const bool& include_new_line = true)
			-> std::expected<std::deque<std::string>, std::string>;
		void close(void);

		static auto compression(const std::string& path, const uint16_t& block_bytes = 1024) -> std::expected<void, std::string>;
		static auto decompression(const std::string& path, const uint16_t& block_bytes = 1024) -> std::expected<void, std::string>;

	private:
		std::fstream stream_;
		std::string file_path_;
		std::ios_base::openmode openmode_;
	};

	auto fsync_file(const std::string& path) -> bool;
	auto fsync_parent_directory(const std::string& path) -> bool;

	// Durably write content to path, failing (rather than silently truncating) if any
	// step cannot complete — e.g. ENOSPC on a full disk. On POSIX this writes through a
	// single fd and checks write()/fsync()/close(), so a short write surfaces as an error
	// instead of leaving a partial file behind.
	auto write_file_durable(const std::string& path, const std::string& content) -> std::expected<void, std::string>;
}
