#include "SQLite.h"

#include <sqlite3.h>

#include <utility>

namespace DataBase
{
	namespace
	{
		struct QueryContext
		{
			QueryResult result;
			bool columns_initialized = false;
		};

		int query_callback(void* data, int argc, char** argv, char** col_names)
		{
			auto* context = static_cast<QueryContext*>(data);
			if (context == nullptr)
			{
				return 1;
			}

			if (!context->columns_initialized)
			{
				context->result.columns.reserve(argc);
				for (int i = 0; i < argc; ++i)
				{
					context->result.columns.push_back(col_names && col_names[i] ? col_names[i] : "");
				}
				context->columns_initialized = true;
			}

			std::vector<std::string> row;
			row.reserve(argc);
			for (int i = 0; i < argc; ++i)
			{
				row.push_back(argv && argv[i] ? argv[i] : "");
			}

			context->result.rows.push_back(std::move(row));

			return 0;
		}
	} // namespace

	SQLiteStatement::SQLiteStatement(sqlite3* handle, sqlite3_stmt* statement) : handle_(handle), statement_(statement) {}

	SQLiteStatement::~SQLiteStatement(void) { finalize(); }

	auto SQLiteStatement::step(void) -> int
	{
		if (statement_ == nullptr)
		{
			return SQLITE_MISUSE;
		}

		return sqlite3_step(statement_);
	}

	auto SQLiteStatement::reset(void) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_reset(statement_);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot reset statement") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::clear_bindings(void) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_clear_bindings(statement_);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot clear bindings") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_int(const int& index, const int& value) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_int(statement_, index, value);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind int") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_int64(const int& index, const int64_t& value) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_int64(statement_, index, value);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind int64") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_double(const int& index, const double& value) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_double(statement_, index, value);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind double") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_text(const int& index, const std::string& value) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_text(statement_, index, value.c_str(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind text") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_blob(const int& index, const std::vector<uint8_t>& value) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_blob(statement_, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind blob") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::bind_null(const int& index) -> std::tuple<bool, std::optional<std::string>>
	{
		if (statement_ == nullptr)
		{
			return { false, "statement is null" };
		}

		auto result = sqlite3_bind_null(statement_, index);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot bind null") };
		}

		return { true, std::nullopt };
	}

	auto SQLiteStatement::column_count(void) const -> int
	{
		if (statement_ == nullptr)
		{
			return 0;
		}

		return sqlite3_column_count(statement_);
	}

	auto SQLiteStatement::column_name(const int& index) const -> std::string
	{
		if (statement_ == nullptr)
		{
			return "";
		}

		const char* name = sqlite3_column_name(statement_, index);
		return name ? name : "";
	}

	auto SQLiteStatement::column_type(const int& index) const -> int
	{
		if (statement_ == nullptr)
		{
			return SQLITE_NULL;
		}

		return sqlite3_column_type(statement_, index);
	}

	auto SQLiteStatement::column_int(const int& index) const -> int
	{
		if (statement_ == nullptr)
		{
			return 0;
		}

		return sqlite3_column_int(statement_, index);
	}

	auto SQLiteStatement::column_int64(const int& index) const -> int64_t
	{
		if (statement_ == nullptr)
		{
			return 0;
		}

		return sqlite3_column_int64(statement_, index);
	}

	auto SQLiteStatement::column_double(const int& index) const -> double
	{
		if (statement_ == nullptr)
		{
			return 0.0;
		}

		return sqlite3_column_double(statement_, index);
	}

	auto SQLiteStatement::column_text(const int& index) const -> std::string
	{
		if (statement_ == nullptr)
		{
			return "";
		}

		const char* text = reinterpret_cast<const char*>(sqlite3_column_text(statement_, index));
		return text ? text : "";
	}

	auto SQLiteStatement::column_blob(const int& index) const -> std::vector<uint8_t>
	{
		if (statement_ == nullptr)
		{
			return {};
		}

		const auto size = sqlite3_column_bytes(statement_, index);
		if (size <= 0)
		{
			return {};
		}

		const auto* data = static_cast<const uint8_t*>(sqlite3_column_blob(statement_, index));
		if (data == nullptr)
		{
			return {};
		}

		return std::vector<uint8_t>(data, data + size);
	}

	auto SQLiteStatement::finalize(void) -> void
	{
		if (statement_ == nullptr)
		{
			return;
		}

		sqlite3_finalize(statement_);
		statement_ = nullptr;
	}

	auto SQLiteStatement::is_valid(void) const -> bool { return statement_ != nullptr; }

	auto SQLiteStatement::error_message(const std::string& context) const -> std::string
	{
		if (handle_ == nullptr)
		{
			return context;
		}

		return context + " : " + sqlite3_errmsg(handle_);
	}

	SQLite::SQLite(void) : handle_(nullptr) {}

	SQLite::~SQLite(void) { close(); }

	auto SQLite::open(const std::string& path) -> std::tuple<bool, std::optional<std::string>>
	{
		return open(path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
	}

	auto SQLite::open(const std::string& path, const int& flags) -> std::tuple<bool, std::optional<std::string>>
	{
		close();

		if (path.empty())
		{
			return { false, "db path is empty" };
		}

		sqlite3* db_handle = nullptr;
		auto result = sqlite3_open_v2(path.c_str(), &db_handle, flags, nullptr);
		if (result != SQLITE_OK)
		{
			std::string message;
			if (db_handle != nullptr)
			{
				message = sqlite3_errmsg(db_handle);
				sqlite3_close_v2(db_handle);
			}

			return { false, message.empty() ? "cannot open database" : message };
		}

		handle_ = db_handle;
		return { true, std::nullopt };
	}

	auto SQLite::close(void) -> void
	{
		if (handle_ == nullptr)
		{
			return;
		}

		sqlite3_close_v2(handle_);
		handle_ = nullptr;
	}

	auto SQLite::is_open(void) const -> bool { return handle_ != nullptr; }

	auto SQLite::handle(void) const -> sqlite3* { return handle_; }

	auto SQLite::set_busy_timeout(const int& timeout_ms) -> std::tuple<bool, std::optional<std::string>>
	{
		if (handle_ == nullptr)
		{
			return { false, "database is not open" };
		}

		auto result = sqlite3_busy_timeout(handle_, timeout_ms);
		if (result != SQLITE_OK)
		{
			return { false, error_message("cannot set busy timeout") };
		}

		return { true, std::nullopt };
	}

	auto SQLite::execute(const std::string& sql) -> std::tuple<bool, std::optional<std::string>>
	{
		if (handle_ == nullptr)
		{
			return { false, "database is not open" };
		}

		char* error_message_ptr = nullptr;
		auto result = sqlite3_exec(handle_, sql.c_str(), nullptr, nullptr, &error_message_ptr);
		if (result != SQLITE_OK)
		{
			std::string message = error_message("cannot execute sql");
			if (error_message_ptr != nullptr)
			{
				message = error_message_ptr;
				sqlite3_free(error_message_ptr);
			}

			return { false, message };
		}

		return { true, std::nullopt };
	}

	auto SQLite::query(const std::string& sql) -> std::tuple<std::optional<QueryResult>, std::optional<std::string>>
	{
		if (handle_ == nullptr)
		{
			return { std::nullopt, "database is not open" };
		}

		QueryContext context;
		char* error_message_ptr = nullptr;
		auto result = sqlite3_exec(handle_, sql.c_str(), query_callback, &context, &error_message_ptr);
		if (result != SQLITE_OK)
		{
			std::string message = error_message("cannot query sql");
			if (error_message_ptr != nullptr)
			{
				message = error_message_ptr;
				sqlite3_free(error_message_ptr);
			}

			return { std::nullopt, message };
		}

		return { context.result, std::nullopt };
	}

	auto SQLite::prepare(const std::string& sql) -> std::tuple<std::shared_ptr<SQLiteStatement>, std::optional<std::string>>
	{
		if (handle_ == nullptr)
		{
			return { nullptr, "database is not open" };
		}

		sqlite3_stmt* statement = nullptr;
		auto result = sqlite3_prepare_v2(handle_, sql.c_str(), -1, &statement, nullptr);
		if (result != SQLITE_OK)
		{
			return { nullptr, error_message("cannot prepare statement") };
		}

		return { std::make_shared<SQLiteStatement>(handle_, statement), std::nullopt };
	}

	auto SQLite::begin_transaction(void) -> std::tuple<bool, std::optional<std::string>>
	{
		return execute("BEGIN IMMEDIATE;");
	}

	auto SQLite::commit(void) -> std::tuple<bool, std::optional<std::string>>
	{
		return execute("COMMIT;");
	}

	auto SQLite::rollback(void) -> std::tuple<bool, std::optional<std::string>>
	{
		return execute("ROLLBACK;");
	}

	auto SQLite::error_message(const std::string& context) const -> std::string
	{
		if (handle_ == nullptr)
		{
			return context;
		}

		return context + " : " + sqlite3_errmsg(handle_);
	}
}
