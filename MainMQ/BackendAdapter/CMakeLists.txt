cmake_minimum_required(VERSION 3.21)

set(LIBRARY_NAME BackendAdapter)

find_package(nlohmann_json CONFIG REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)

set(HEADER_FILES
	BackendAdapter.h
	SQLiteAdapter.h
	FileSystemAdapter.h
	HybridAdapter.h
)

set(SOURCE_FILES
	SQLiteAdapter.cpp
	FileSystemAdapter.cpp
	HybridAdapter.cpp
)

add_library(${LIBRARY_NAME} ${HEADER_FILES} ${SOURCE_FILES})

target_include_directories(${LIBRARY_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(${LIBRARY_NAME} PUBLIC
	Utilities
	DataBase
	nlohmann_json::nlohmann_json
	unofficial::sqlite3::sqlite3
)

add_custom_command(
	TARGET ${LIBRARY_NAME} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy_if_different
		${CMAKE_SOURCE_DIR}/MainMQ/sqlite_schema.sql
		${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/sqlite_schema.sql
)
