#pragma once

#include "ArgumentParser.h"
#include "LogTypes.h"
#include "MailboxClient.h"

#include <string>

using namespace Utilities;

// IPC communication settings (alias for common library type)
using MailboxConfig = MailboxIPC::ClientConfig;

class Configurations
{
public:
	Configurations(ArgumentParser&& arguments);
	virtual ~Configurations(void);

	// Logging
	auto write_file() -> LogTypes;
	auto write_console() -> LogTypes;
	auto log_root() -> std::string;

	// Paths
	auto root_path() -> std::string;

	// IPC (Mailbox)
	auto mailbox_config() -> MailboxConfig;
	auto ipc_root() -> std::string;
	auto timeout_ms() -> int32_t;

	// Publisher defaults
	auto default_queue() -> std::string;
	auto default_target() -> std::string;
	auto default_priority() -> int32_t;

	// Client
	auto client_id() -> std::string;

protected:
	auto load() -> void;
	auto parse(ArgumentParser& arguments) -> void;

private:
	// Logging
	LogTypes write_file_;
	LogTypes write_console_;
	std::string log_root_;

	// Paths
	std::string root_path_;

	// IPC (Mailbox)
	MailboxConfig mailbox_config_;

	// Publisher defaults
	std::string default_queue_;
	std::string default_target_;
	int32_t default_priority_;

	// Client
	std::string client_id_;
};
