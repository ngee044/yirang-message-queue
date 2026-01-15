#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

struct sqlite3;
struct sqlite3_stmt;

namespace DataBase
{
	struct QueryResult
	{
		std::vector<std::string> columns;
		std::vector<std::vector<std::string>> rows;
	};

	class SQLiteStatement
	{
	public:
		SQLiteStatement(sqlite3* handle, sqlite3_stmt* statement);
		~SQLiteStatement(void);

		auto step(void) -> int;
		auto reset(void) -> std::tuple<bool, std::optional<std::string>>;
		auto clear_bindings(void) -> std::tuple<bool, std::optional<std::string>>;

		auto bind_int(const int& index, const int& value) -> std::tuple<bool, std::optional<std::string>>;
		auto bind_int64(const int& index, const int64_t& value) -> std::tuple<bool, std::optional<std::string>>;
		auto bind_double(const int& index, const double& value) -> std::tuple<bool, std::optional<std::string>>;
		auto bind_text(const int& index, const std::string& value) -> std::tuple<bool, std::optional<std::string>>;
		auto bind_blob(const int& index, const std::vector<uint8_t>& value) -> std::tuple<bool, std::optional<std::string>>;
		auto bind_null(const int& index) -> std::tuple<bool, std::optional<std::string>>;

		auto column_count(void) const -> int;
		auto column_name(const int& index) const -> std::string;
		auto column_type(const int& index) const -> int;

		auto column_int(const int& index) const -> int;
		auto column_int64(const int& index) const -> int64_t;
		auto column_double(const int& index) const -> double;
		auto column_text(const int& index) const -> std::string;
		auto column_blob(const int& index) const -> std::vector<uint8_t>;

		auto finalize(void) -> void;
		auto is_valid(void) const -> bool;

	private:
		auto error_message(const std::string& context) const -> std::string;

	private:
		sqlite3* handle_;
		sqlite3_stmt* statement_;
	};

	class SQLite
	{
	public:
		SQLite(void);
		~SQLite(void);

		auto open(const std::string& path) -> std::tuple<bool, std::optional<std::string>>;
		auto open(const std::string& path, const int& flags) -> std::tuple<bool, std::optional<std::string>>;
		auto close(void) -> void;

		auto is_open(void) const -> bool;
		auto handle(void) const -> sqlite3*;

		auto set_busy_timeout(const int& timeout_ms) -> std::tuple<bool, std::optional<std::string>>;
		auto execute(const std::string& sql) -> std::tuple<bool, std::optional<std::string>>;
		auto query(const std::string& sql) -> std::tuple<std::optional<QueryResult>, std::optional<std::string>>;
		auto prepare(const std::string& sql) -> std::tuple<std::shared_ptr<SQLiteStatement>, std::optional<std::string>>;

		auto begin_transaction(void) -> std::tuple<bool, std::optional<std::string>>;
		auto commit(void) -> std::tuple<bool, std::optional<std::string>>;
		auto rollback(void) -> std::tuple<bool, std::optional<std::string>>;

	private:
		auto error_message(const std::string& context) const -> std::string;

	private:
		sqlite3* handle_;
	};
}
