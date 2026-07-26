#include "Converter.h"

#include <codecvt>

namespace Utilities
{
	auto Converter::to_string(const std::u32string& value, std::locale target_locale) -> std::string
	{
		if (value.empty())
		{
			return std::string();
		}

		typedef std::codecvt<char32_t, char, mbstate_t> codecvt_t;
		codecvt_t const& codecvt = std::use_facet<codecvt_t>(target_locale);

		mbstate_t state = mbstate_t();

		std::vector<char> result((value.size() + 1) * codecvt.max_length());
		char32_t const* in_text = value.data();
		char* out_text = &result[0];

		codecvt_t::result condition = codecvt.out(state, in_text, in_text + value.size(), in_text, out_text, out_text + result.size(), out_text);

		return std::string(result.data());
	}

	auto Converter::to_string(const std::u16string& value, std::locale target_locale) -> std::string
	{
		if (value.empty())
		{
			return std::string();
		}

		typedef std::codecvt<char16_t, char, mbstate_t> codecvt_t;
		codecvt_t const& codecvt = std::use_facet<codecvt_t>(target_locale);

		mbstate_t state = mbstate_t();

		std::vector<char> result((value.size() + 1) * codecvt.max_length());
		char16_t const* in_text = value.data();
		char* out_text = &result[0];

		codecvt_t::result condition = codecvt.out(state, in_text, in_text + value.size(), in_text, out_text, out_text + result.size(), out_text);

		return std::string(result.data());
	}

	auto Converter::to_string(const std::wstring& value, std::locale target_locale) -> std::string
	{
#ifdef WSTRING_IS_U16STRING
		return to_string(convert(value));
#else
		if (value.empty())
		{
			return std::string();
		}

		typedef std::codecvt<wchar_t, char, mbstate_t> codecvt_t;
		codecvt_t const& codecvt = std::use_facet<codecvt_t>(target_locale);

		mbstate_t state = mbstate_t();

		std::vector<char> result((value.size() + 1) * codecvt.max_length());
		wchar_t const* in_text = value.data();
		char* out_text = &result[0];

		codecvt_t::result condition = codecvt.out(state, value.data(), value.data() + value.size(), in_text, &result[0], &result[0] + result.size(), out_text);

		return std::string(&result[0], out_text);
#endif
	}

	auto Converter::to_string(const std::vector<uint8_t>& value) -> std::string
	{
		if (value.empty())
		{
			return std::string();
		}

		// UTF-8 BOM is 3 bytes (EF BB BF); skip all three, not two. (Defect D-16)
		if (value.size() >= 3 && value[0] == 0xef && value[1] == 0xbb && value[2] == 0xbf)
		{
			return std::string((char*)value.data() + 3, value.size() - 3);
		}

		// UTF-8 no BOM
		return std::string((char*)value.data(), value.size());
	}

	auto Converter::to_array(const std::string& value) -> std::vector<uint8_t>
	{
		return std::vector<uint8_t>(value.data(), value.data() + value.size());
	}

	auto Converter::from_base64(const std::string& value) -> std::vector<uint8_t>
	{
		if (value.empty())
		{
			return std::vector<uint8_t>();
		}

		static constexpr unsigned char decode_table[] = {
			64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
			64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
			64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
			52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
			64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
			15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
			64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
			41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64
		};

		size_t input_length = value.size();
		size_t padding = 0;
		if (input_length > 0 && value[input_length - 1] == '=') padding++;
		if (input_length > 1 && value[input_length - 2] == '=') padding++;

		size_t output_length = (input_length / 4) * 3 - padding;
		std::vector<uint8_t> result(output_length);

		size_t j = 0;
		uint32_t buffer = 0;
		int bits_collected = 0;

		for (size_t i = 0; i < input_length; ++i)
		{
			unsigned char c = static_cast<unsigned char>(value[i]);
			if (c == '=') break;
			if (c >= 128 || decode_table[c] == 64) continue;

			buffer = (buffer << 6) | decode_table[c];
			bits_collected += 6;

			if (bits_collected >= 8)
			{
				bits_collected -= 8;
				result[j++] = static_cast<uint8_t>((buffer >> bits_collected) & 0xFF);
			}
		}

		result.resize(j);
		return result;
	}

	auto Converter::to_base64(const std::vector<uint8_t>& value) -> std::string
	{
		if (value.empty())
		{
			return std::string();
		}

		static constexpr char encode_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

		size_t input_length = value.size();
		size_t output_length = 4 * ((input_length + 2) / 3);
		std::string result(output_length, '=');

		size_t j = 0;
		for (size_t i = 0; i < input_length;)
		{
			uint32_t octet_a = i < input_length ? value[i++] : 0;
			uint32_t octet_b = i < input_length ? value[i++] : 0;
			uint32_t octet_c = i < input_length ? value[i++] : 0;

			uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

			result[j++] = encode_table[(triple >> 18) & 0x3F];
			result[j++] = encode_table[(triple >> 12) & 0x3F];
			result[j++] = encode_table[(triple >> 6) & 0x3F];
			result[j++] = encode_table[triple & 0x3F];
		}

		size_t mod = input_length % 3;
		if (mod == 1)
		{
			result[output_length - 1] = '=';
			result[output_length - 2] = '=';
		}
		else if (mod == 2)
		{
			result[output_length - 1] = '=';
		}

		return result;
	}

#ifdef WSTRING_IS_U16STRING
	auto Converter::convert(const std::u16string& value) -> std::wstring { return std::wstring(value.begin(), value.end()); }

	auto Converter::convert(const std::wstring& value) -> std::u16string { return std::u16string(value.begin(), value.end()); }
#endif
}
