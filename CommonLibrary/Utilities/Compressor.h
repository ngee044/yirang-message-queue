#pragma once

#include <expected>
#include <string>
#include <vector>
#include <cstdint>

namespace Utilities
{
	class Compressor
	{
	public:
		static auto compression(const std::vector<uint8_t>& original_data, const uint16_t& block_bytes = 1024)
			-> std::expected<std::vector<uint8_t>, std::string>;
		static auto decompression(const std::vector<uint8_t>& compressed_data, const uint16_t& block_bytes = 1024)
			-> std::expected<std::vector<uint8_t>, std::string>;
	};
}
