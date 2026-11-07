cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(nlohmann_json CONFIG REQUIRED)

project(yirangmq-demo VERSION 1.0.0.0)

# 세 데모 프로세스가 MqCall 을 공유한다. 대상이 셋뿐이므로 별도 라이브러리 타깃을 만들지 않고
# 각 실행 파일에 함께 컴파일한다.
set(DEMO_SHARED_SOURCES MqCall.cpp)

configure_file(run-demo.sh ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/run-demo.sh COPYONLY)

function(add_demo_program PROGRAM_NAME PROGRAM_SOURCE)
	add_executable(${PROGRAM_NAME} ${PROGRAM_SOURCE} ${DEMO_SHARED_SOURCES})

	target_link_libraries(${PROGRAM_NAME} PUBLIC
		Utilities
		MailboxClient
		nlohmann_json::nlohmann_json
	)

	target_include_directories(${PROGRAM_NAME} PRIVATE
		"${CMAKE_SOURCE_DIR}/CommonLibrary/Utilities"
		"${CMAKE_SOURCE_DIR}/CommonLibrary/MailboxClient"
		"${CMAKE_CURRENT_SOURCE_DIR}"
	)

endfunction()

add_demo_program(yirangmq-demo-sensor Sensor.cpp)
add_demo_program(yirangmq-demo-worker Worker.cpp)
add_demo_program(yirangmq-demo-monitor Monitor.cpp)
