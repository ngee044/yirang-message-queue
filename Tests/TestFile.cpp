#include "File.h"

#include "TestHelpers.h"

#include <gtest/gtest.h>

#include <ios>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
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
	auto [open_ok, open_err] = out.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
	ASSERT_TRUE(open_ok) << (open_err ? *open_err : "");
	auto [write_ok, write_err] = out.write_bytes(data);
	EXPECT_TRUE(write_ok) << (write_err ? *write_err : "");
	out.close();

	File in;
	auto [in_ok, in_err] = in.open(path, std::ios::in | std::ios::binary);
	ASSERT_TRUE(in_ok) << (in_err ? *in_err : "");
	auto [bytes, read_err] = in.read_bytes();
	ASSERT_TRUE(bytes.has_value()) << (read_err ? *read_err : "");
	EXPECT_EQ(bytes.value(), data);
	in.close();
}

// File::open now assigns openmode_, so the read/write guards are live (INC fix).

TEST_F(FileTest, WriteToReadOnlyHandleFails)
{
	const auto path = path_for("readonly.bin");
	{
		File seed;
		ASSERT_TRUE(std::get<0>(seed.open(path, std::ios::out | std::ios::binary | std::ios::trunc)));
		seed.write_bytes(std::vector<uint8_t>{ 1 });
		seed.close();
	}

	File in;
	ASSERT_TRUE(std::get<0>(in.open(path, std::ios::in | std::ios::binary)));
	auto [ok, err] = in.write_bytes(std::vector<uint8_t>{ 2, 3 });
	EXPECT_FALSE(ok) << "write to an in-mode handle must be rejected";
	EXPECT_TRUE(err.has_value());
	in.close();
}

TEST_F(FileTest, ReadFromWriteOnlyHandleFails)
{
	const auto path = path_for("writeonly.bin");

	File out;
	ASSERT_TRUE(std::get<0>(out.open(path, std::ios::out | std::ios::binary | std::ios::trunc)));
	auto [bytes, err] = out.read_bytes();
	EXPECT_FALSE(bytes.has_value()) << "read from an out-mode handle must be rejected";
	EXPECT_TRUE(err.has_value());
	out.close();
}

// Defect D-02: write_file_durable must write exact content and, crucially, report a
// failure instead of silently leaving a partial file (the class of bug where a full-disk
// write was swallowed as success and a truncated file was promoted over a good one).

TEST_F(FileTest, WriteFileDurableRoundTrip)
{
	const auto path = path_for("durable.json");
	const std::string content = R"({"k":"v","n":42})";

	auto [ok, err] = write_file_durable(path, content);
	ASSERT_TRUE(ok) << (err ? *err : "");

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

	auto [ok, err] = write_file_durable(bad_path, "payload");
	EXPECT_FALSE(ok) << "a write that cannot complete must not report success";
	EXPECT_TRUE(err.has_value());
}

TEST_F(FileTest, FsyncFileReturnsFalseForMissingFileTrueForReal)
{
	const auto path = path_for("fsync_target.json");
	EXPECT_FALSE(fsync_file(path)) << "fsync of a non-existent file must return false";

	ASSERT_TRUE(std::get<0>(write_file_durable(path, "x")));
	EXPECT_TRUE(fsync_file(path)) << "fsync of an existing file must succeed";
}
