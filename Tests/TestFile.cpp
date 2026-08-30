#include "File.h"

#include "TestHelpers.h"

#include <gtest/gtest.h>

#include <expected>
#include <ios>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

using namespace Utilities;

namespace
{
	class FileTest : public ::testing::Test
	{
	protected:
		void SetUp(void) override
		{
			init_test_logger();
			dir_ = std::make_unique<TempDir>("file_test_");
		}

		auto path_for(const std::string& name) -> std::string { return dir_->path() + "/" + name; }

		std::unique_ptr<TempDir> dir_;
	};
}

TEST_F(FileTest, WriteThenReadRoundTrip)
{
	const std::vector<uint8_t> data{ 10, 20, 30, 40, 50 };
	const auto path = path_for("roundtrip.bin");

	File out;
	auto open_result = out.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
	ASSERT_TRUE(open_result.has_value()) << (open_result ? "" : open_result.error());
	auto write_result = out.write_bytes(data);
	EXPECT_TRUE(write_result.has_value()) << (write_result ? "" : write_result.error());
	out.close();

	File in;
	auto in_result = in.open(path, std::ios::in | std::ios::binary);
	ASSERT_TRUE(in_result.has_value()) << (in_result ? "" : in_result.error());
	auto read_result = in.read_bytes();
	ASSERT_TRUE(read_result.has_value()) << (read_result ? "" : read_result.error());
	EXPECT_EQ(read_result.value(), data);
	in.close();
}

// File::open now assigns openmode_, so the read/write guards are live (INC fix).

TEST_F(FileTest, WriteToReadOnlyHandleFails)
{
	const auto path = path_for("readonly.bin");
	{
		File seed;
		ASSERT_TRUE(seed.open(path, std::ios::out | std::ios::binary | std::ios::trunc).has_value());
		seed.write_bytes(std::vector<uint8_t>{ 1 });
		seed.close();
	}

	File in;
	ASSERT_TRUE(in.open(path, std::ios::in | std::ios::binary).has_value());
	auto write_result = in.write_bytes(std::vector<uint8_t>{ 2, 3 });
	EXPECT_FALSE(write_result.has_value()) << "write to an in-mode handle must be rejected";
	in.close();
}

TEST_F(FileTest, ReadFromWriteOnlyHandleFails)
{
	const auto path = path_for("writeonly.bin");

	File out;
	ASSERT_TRUE(out.open(path, std::ios::out | std::ios::binary | std::ios::trunc).has_value());
	auto read_result = out.read_bytes();
	EXPECT_FALSE(read_result.has_value()) << "read from an out-mode handle must be rejected";
	out.close();
}

// Defect D-02: write_file_durable must write exact content and, crucially, report a
// failure instead of silently leaving a partial file (the class of bug where a full-disk
// write was swallowed as success and a truncated file was promoted over a good one).

TEST_F(FileTest, WriteFileDurableRoundTrip)
{
	const auto path = path_for("durable.json");
	const std::string content = R"({"k":"v","n":42})";

	auto write_result = write_file_durable(path, content);
	ASSERT_TRUE(write_result.has_value()) << (write_result ? "" : write_result.error());

	std::ifstream in(path, std::ios::binary);
	ASSERT_TRUE(in.is_open());
	const std::string read_back((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
	EXPECT_EQ(read_back, content);
}

TEST_F(FileTest, WriteFileDurableReportsFailureInsteadOfSilentSuccess)
{
	// Parent directory does not exist, so the write cannot complete. The contract is that
	// this is REPORTED as a failure rather than swallowed.
	const auto bad_path = path_for("missing_subdir/durable.json");

	auto write_result = write_file_durable(bad_path, "payload");
	EXPECT_FALSE(write_result.has_value()) << "a write that cannot complete must not report success";
}

TEST_F(FileTest, FsyncFileReturnsFalseForMissingFileTrueForReal)
{
	const auto path = path_for("fsync_target.json");
	EXPECT_FALSE(fsync_file(path)) << "fsync of a non-existent file must return false";

	ASSERT_TRUE(write_file_durable(path, "x").has_value());
	EXPECT_TRUE(fsync_file(path)) << "fsync of an existing file must succeed";
}
