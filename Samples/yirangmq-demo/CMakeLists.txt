cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(nlohmann_json CONFIG REQUIRED)

project(yirangmq-demo VERSION 1.0.0.0)

# 세 데모 프로세스가 MqCall 을 공유한다. 대상이 셋뿐이므로 별도 라이브러리 타깃을 만들지 않고
# 각 실행 파일에 함께 컴파일한다.
set(DEMO_SHARED_SOURCES MqCall.cpp)

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

	# 러너 스크립트를 실행 파일과 같은 폴더로 복사한다. IPC 루트가 작업 디렉터리 기준 상대
	# 경로이므로 데모는 데몬과 같은 디렉터리에서 실행되어야 한다. 한 타깃에만 걸면
	# 부분 빌드(--target yirangmq-demo-worker) 때 스크립트가 누락되므로 셋 모두에 붙인다.
	add_custom_command(
		TARGET ${PROGRAM_NAME} POST_BUILD
		COMMAND ${CMAKE_COMMAND} -E copy_if_different
			${CMAKE_CURRENT_SOURCE_DIR}/run-demo.sh
			${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/run-demo.sh
	)
endfunction()

add_demo_program(yirangmq-demo-sensor Sensor.cpp)
add_demo_program(yirangmq-demo-worker Worker.cpp)
add_demo_program(yirangmq-demo-monitor Monitor.cpp)
