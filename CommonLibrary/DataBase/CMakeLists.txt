cmake_minimum_required(VERSION 3.21)

set(LIBRARY_NAME DataBase)

set(HEADER_FILES
	SQLite.h
)
set(SOURCE_FILES
	SQLite.cpp
)

add_library(${LIBRARY_NAME} ${HEADER_FILES} ${SOURCE_FILES})
target_include_directories(${LIBRARY_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(${LIBRARY_NAME} PUBLIC
	unofficial::sqlite3::sqlite3
)
