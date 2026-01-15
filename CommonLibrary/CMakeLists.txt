cmake_minimum_required(VERSION 3.21)

find_package(efsw CONFIG REQUIRED)
find_package(lz4 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)

add_subdirectory(Utilities)
add_subdirectory(DataBase)
add_subdirectory(ThreadPool)
