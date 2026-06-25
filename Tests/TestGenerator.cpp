#include "Generator.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <regex>
#include <set>
#include <string>

using Utilities::Generator;

// ============================================================
// Generator::guid() — RFC 4122 v4 identifier (LIM-09 / INC-09)
// ============================================================

TEST(GeneratorTest, ProducesRfc4122Version4Format)
{
	const std::regex uuid_v4("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");

	for (int i = 0; i < 1000; ++i)
	{
		const auto id = Generator::guid();
		EXPECT_EQ(id.size(), static_cast<size_t>(36)) << id;
		EXPECT_TRUE(std::regex_match(id, uuid_v4)) << "not a v4 uuid: " << id;
	}
}

TEST(GeneratorTest, ProducesUniqueIdsAtScale)
{
	// Regression for the previous generator: it reseeded mt19937 per call with a single
	// 32-bit draw, so birthday collisions were likely well below this count.
	constexpr int count = 100000;

	std::set<std::string> seen;
	for (int i = 0; i < count; ++i)
	{
		seen.insert(Generator::guid());
	}

	EXPECT_EQ(seen.size(), static_cast<size_t>(count)) << "guid collisions detected";
}

TEST(GeneratorTest, ConsecutiveCallsDiffer)
{
	EXPECT_NE(Generator::guid(), Generator::guid());
}
