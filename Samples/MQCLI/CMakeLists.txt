cmake_minimum_required(VERSION 3.21)

set(PROGRAM_NAME MQCLI)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(nlohmann_json CONFIG REQUIRED)

set(SOURCE_FILES
	main.cpp
)

project(${PROGRAM_NAME} VERSION 1.0.0.0)

add_executable(${PROGRAM_NAME} ${SOURCE_FILES})

target_link_libraries(${PROGRAM_NAME} PUBLIC
	Utilities
	DataBase
	ThreadPool
	BackendAdapter
	nlohmann_json::nlohmann_json
)

target_include_directories(${PROGRAM_NAME} PRIVATE
	"${CMAKE_SOURCE_DIR}/CommonLibrary/Utilities"
	"${CMAKE_SOURCE_DIR}/CommonLibrary/DataBase"
	"${CMAKE_SOURCE_DIR}/CommonLibrary/ThreadPool"
	"${CMAKE_SOURCE_DIR}/MainMQ/BackendAdapter"
	"${CMAKE_SOURCE_DIR}/MainMQ"
)
