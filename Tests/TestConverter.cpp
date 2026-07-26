#include "Converter.h"

#include "TestHelpers.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

using Utilities::Converter;

namespace
{
	class ConverterTest : public ::testing::Test
	{
	protected:
		void SetUp(void) override { init_test_logger(); }
	};
}

// Defect D-16: the UTF-8 BOM is 3 bytes (EF BB BF). to_string must strip all three,
// leaving the payload intact. Previously it skipped only two, leaving a stray 0xBF that
// broke json::parse and blocked daemon/CLI startup when a config was saved with a BOM.
TEST_F(ConverterTest, StripsFullUtf8Bom)
{
	std::vector<uint8_t> bytes = { 0xEF, 0xBB, 0xBF };
	const std::string body = R"({"k":1})";
	for (char c : body)
	{
		bytes.push_back(static_cast<uint8_t>(c));
	}

	EXPECT_EQ(Converter::to_string(bytes), body) << "BOM must be fully stripped with no stray byte";
}

TEST_F(ConverterTest, LeavesNonBomContentUnchanged)
{
	const std::string body = "plain text";
	std::vector<uint8_t> bytes(body.begin(), body.end());

	EXPECT_EQ(Converter::to_string(bytes), body);
}

TEST_F(ConverterTest, EmptyInputYieldsEmptyString)
{
	std::vector<uint8_t> bytes;
	EXPECT_TRUE(Converter::to_string(bytes).empty());
}
