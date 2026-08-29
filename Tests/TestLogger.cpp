#include "Logger.h"

#include <gtest/gtest.h>

#include <atomic>
#include <format>
#include <string>
#include <thread>
#include <vector>

using namespace Utilities;

// ============================================================
// Logger::run() concurrency (Logger.cpp:226)
//
// run() previously read messages_ in the loop condition without holding mutex_
// while write() mutated it under lock — a data race. This test drives many
// concurrent writers while the drain loop runs actively (write_interval > 0) and
// asserts every message reaches the sink (delivered via the database callback,
// which is independent of file path / log level).
// ============================================================

TEST(LoggerTest, ConcurrentWritesAreAllDelivered)
{
	std::atomic<int> delivered{ 0 };

	auto& log = Logger::handle();
	log.console_mode(LogTypes::None);
	log.file_mode(LogTypes::None);
	log.database_mode(true);
	log.write_interval(5); // active periodic draining: run() loops concurrently with writers
	log.set_notification_for_database(
		[&delivered](const std::string&, const std::vector<std::string>& messages) -> bool
		{
			delivered.fetch_add(static_cast<int>(messages.size()));
			return true;
		});

	log.start("concurrency");

	constexpr int thread_count = 8;
	constexpr int per_thread = 200;

	std::vector<std::thread> workers;
	for (int t = 0; t < thread_count; ++t)
	{
		workers.emplace_back(
			[&log, t]()
			{
				for (int i = 0; i < per_thread; ++i)
				{
					log.write(LogTypes::Information, std::format("MARK t{} n{}", t, i));
				}
			});
	}

	for (auto& worker : workers)
	{
		worker.join();
	}

	log.stop(); // drains any remaining queued messages before returning

	// All data messages must be delivered (the [START]/[STOP] markers add a few more).
	EXPECT_GE(delivered.load(), thread_count * per_thread);

	log.set_notification_for_database(nullptr);
	log.database_mode(false);
	Logger::destroy();
}
