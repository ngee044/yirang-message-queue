cmake_minimum_required(VERSION 3.21)

set(PROGRAM_NAME yirangmq-cli-consumer)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(nlohmann_json CONFIG REQUIRED)

set(SOURCE_FILES
	main.cpp
	Configurations.cpp
)

project(${PROGRAM_NAME} VERSION 1.0.0.0)

add_executable(${PROGRAM_NAME} ${SOURCE_FILES})

target_link_libraries(${PROGRAM_NAME} PUBLIC
	Utilities
	nlohmann_json::nlohmann_json
)

target_include_directories(${PROGRAM_NAME} PRIVATE
	"${CMAKE_SOURCE_DIR}/CommonLibrary/Utilities"
	"${CMAKE_CURRENT_SOURCE_DIR}"
)

set(RESOURCE_FILES
	consumer_configuration.json
)

foreach(RESOURCE_FILE IN LISTS RESOURCE_FILES)
	add_custom_command(
		TARGET ${PROGRAM_NAME} POST_BUILD
		COMMAND ${CMAKE_COMMAND} -E copy_if_different
			${CMAKE_CURRENT_SOURCE_DIR}/${RESOURCE_FILE}
			${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${RESOURCE_FILE}
	)
endforeach()
