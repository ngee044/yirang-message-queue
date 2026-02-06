cmake_minimum_required(VERSION 3.21)

find_package(nlohmann_json CONFIG REQUIRED)

add_subdirectory(BackendAdapter)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

set(LIB_SOURCE_FILES
	Configurations.cpp
	QueueManager.cpp
	MessageValidator.cpp
	MailboxHandler.cpp
)

set(HEADER_FILES
	Configurations.h
	QueueManager.h
	MessageValidator.h
	MailboxHandler.h
	MailboxTypes.h
)

# MainMQLib static library (everything except main.cpp)
add_library(MainMQLib STATIC ${HEADER_FILES} ${LIB_SOURCE_FILES})
target_link_libraries(MainMQLib PUBLIC
	Utilities
	DataBase
	ThreadPool
	BackendAdapter
	nlohmann_json::nlohmann_json
)
target_include_directories(MainMQLib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

# MainMQ executable
set(PROGRAM_NAME MainMQ)
project(${PROGRAM_NAME} VERSION 1.0.0.0)

add_executable(${PROGRAM_NAME} main.cpp)
target_link_libraries(${PROGRAM_NAME} PRIVATE MainMQLib)

set(RESOURCE_FILES
	main_mq_configuration.json
	sqlite_schema.sql
)

foreach(RESOURCE_FILE IN LISTS RESOURCE_FILES)
	add_custom_command(
		TARGET MainMQ POST_BUILD
		COMMAND ${CMAKE_COMMAND} -E copy_if_different
			${CMAKE_CURRENT_SOURCE_DIR}/${RESOURCE_FILE}
			${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${RESOURCE_FILE}
	)
endforeach()
