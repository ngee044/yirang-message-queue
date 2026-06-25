#include "Job.h"

#include "TestHelpers.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <tuple>
#include <vector>

using Thread::Job;
using Thread::JobPriorities;

namespace
{
	class JobTest : public ::testing::Test
	{
	protected:
		void SetUp(void) override { init_test_logger(); }
	};
}

// ============================================================
// Job int-condition constructor (Job.cpp:62 reserve -> resize)
//
// The previous constructor reserve()d the data buffer but wrote via operator[]
// while size()==0 — out-of-bounds. work() then memcpy'd 4 bytes back out, so the
// int could be corrupted. These tests pin the round-trip.
// ============================================================

TEST_F(JobTest, IntConditionRoundTripsExactValue)
{
	for (const int32_t value : { 0, 1, -1, 42, -42, 0x12345678, static_cast<int32_t>(0x89ABCDEF), INT32_MAX, INT32_MIN })
	{
		int received = 0;
		bool called = false;

		Job job(
			JobPriorities::Normal, value,
			[&](const int& observed) -> std::tuple<bool, std::optional<std::string>>
			{
				received = observed;
				called = true;
				return { true, std::nullopt };
			},
			"int-job", false);

		auto [ok, error] = job.work();
		EXPECT_TRUE(ok) << (error ? *error : "");
		EXPECT_TRUE(called);
		EXPECT_EQ(received, value);
	}
}

TEST_F(JobTest, BoolConditionRoundTrips)
{
	for (const bool value : { true, false })
	{
		bool received = !value;

		Job job(
			JobPriorities::Normal, value,
			[&](const bool& observed) -> std::tuple<bool, std::optional<std::string>>
			{
				received = observed;
				return { true, std::nullopt };
			},
			"bool-job", false);

		auto [ok, error] = job.work();
		EXPECT_TRUE(ok) << (error ? *error : "");
		EXPECT_EQ(received, value);
	}
}

TEST_F(JobTest, VoidCallbackExecutes)
{
	bool ran = false;

	Job job(
		JobPriorities::Normal,
		[&]() -> std::tuple<bool, std::optional<std::string>>
		{
			ran = true;
			return { true, std::nullopt };
		},
		"void-job", false);

	auto [ok, error] = job.work();
	EXPECT_TRUE(ok) << (error ? *error : "");
	EXPECT_TRUE(ran);
}

TEST_F(JobTest, VectorDataRoundTrips)
{
	const std::vector<uint8_t> data{ 1, 2, 3, 4, 5 };
	std::vector<uint8_t> received;

	Job job(
		JobPriorities::Normal, data,
		[&](const std::vector<uint8_t>& observed) -> std::tuple<bool, std::optional<std::string>>
		{
			received = observed;
			return { true, std::nullopt };
		},
		"vec-job", false);

	auto [ok, error] = job.work();
	EXPECT_TRUE(ok) << (error ? *error : "");
	EXPECT_EQ(received, data);
}
