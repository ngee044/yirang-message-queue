cmake_minimum_required(VERSION 3.21)

set(PROJECT_NAME YiRang-MQ)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_DEBUG")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Set the output directory to the build directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/out")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

if(APPLE AND NOT CMAKE_CROSSCOMPILING)
    list(APPEND CMAKE_PREFIX_PATH /usr/local /opt/homebrew)
endif()

if(DEFINED VCPKG_TARGET_TRIPLET)
    list(APPEND CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}")
endif()

project(${PROJECT_NAME} VERSION 1.0.0.0)

find_package(Threads REQUIRED)

option(BUILD_TESTS "Build unit tests" ON)

add_subdirectory(CommonLibrary)
add_subdirectory(MainMQ)

# Samples
add_subdirectory(Samples)
#add_subdirectory(ExampleProject)

# Tests
if(BUILD_TESTS)
	enable_testing()
	add_subdirectory(Tests)
endif()