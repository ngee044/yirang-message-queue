cmake_minimum_required(VERSION 3.21)

find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# Test executables
set(TEST_SOURCES
	TestMessageValidator.cpp
	TestSQLiteAdapter.cpp
	TestFileSystemAdapter.cpp
	TestHybridAdapter.cpp
	TestQueueManager.cpp
	TestConfigurations.cpp
	TestMailboxHandler.cpp
)

foreach(TEST_SOURCE IN LISTS TEST_SOURCES)
	get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
	add_executable(${TEST_NAME} ${TEST_SOURCE})
	target_link_libraries(${TEST_NAME} PRIVATE
		MainMQLib
		GTest::gtest
		GTest::gtest_main
	)
	target_include_directories(${TEST_NAME} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
	gtest_discover_tests(${TEST_NAME})
endforeach()

# Copy resource files needed by tests
set(RESOURCE_FILES
	${CMAKE_SOURCE_DIR}/MainMQ/BackendAdapter/sqlite_schema.sql
	${CMAKE_SOURCE_DIR}/MainMQ/main_mq_configuration.json
)

foreach(RESOURCE_FILE IN LISTS RESOURCE_FILES)
	get_filename_component(RESOURCE_NAME ${RESOURCE_FILE} NAME)
	configure_file(${RESOURCE_FILE} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${RESOURCE_NAME} COPYONLY)
endforeach()
