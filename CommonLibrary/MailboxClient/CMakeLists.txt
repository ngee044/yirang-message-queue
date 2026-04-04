set(PROGRAM_NAME MailboxClient)

set(SOURCE_FILES
	MailboxClient.cpp
)

add_library(${PROGRAM_NAME} STATIC ${SOURCE_FILES})

target_include_directories(${PROGRAM_NAME} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(${PROGRAM_NAME} PUBLIC
	Utilities
	nlohmann_json::nlohmann_json
)
