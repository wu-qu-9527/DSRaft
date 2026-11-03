#include "server/key_value_state_machine.hpp"

#include <stdexcept>
#include <string_view>

namespace zmq_template::server {
namespace {
struct Command {
    std::string_view operation;
    std::string key;
    std::string value;
};

Command parse(const std::string& command) {
    const auto separator = command.find(' ');
    if (separator == std::string::npos) throw std::invalid_argument("Use SET key value, GET key, or DEL key");
    Command parsed{std::string_view{command}.substr(0, separator), command.substr(separator + 1), {}};
    if (parsed.operation == "SET") {
        const auto value_start = parsed.key.find(' ');
        if (value_start == std::string::npos) throw std::invalid_argument("SET requires a key and value");
        parsed.value = parsed.key.substr(value_start + 1);
        parsed.key.resize(value_start);
    } else if (parsed.operation != "GET" && parsed.operation != "DEL") {
        throw std::invalid_argument("Unknown key-value operation");
    }
    if (parsed.key.empty() || parsed.key.find_first_of(" \t\r\n") != std::string::npos
        || parsed.key.find('\0') != std::string::npos) {
        throw std::invalid_argument("Keys must be nonempty and contain no whitespace or NUL");
    }
    return parsed;
}
} // namespace

void KeyValueStateMachine::validate_command(const std::string& command) { (void)parse(command); }

std::optional<std::string> KeyValueStateMachine::get(const std::string& key) const {
    const auto found = values_.find(key);
    if (found == values_.end()) return std::nullopt;
    return found->second;
}

std::string KeyValueStateMachine::execute(const std::string& command) {
    if (command.empty()) return {}; // Internal no-op.
    try {
        const auto parsed = parse(command);
        if (parsed.operation == "SET") {
            values_[parsed.key] = parsed.value;
            return "OK";
        }
        if (parsed.operation == "DEL") return values_.erase(parsed.key) ? "OK" : "NOT_FOUND";
        const auto value = get(parsed.key);
        return value ? "VALUE " + *value : "NOT_FOUND";
    } catch (const std::invalid_argument&) {
        // Legacy opaque commands produce a deterministic error without changing the map.
        return "ERROR invalid command";
    }
}

std::vector<AppliedResult> KeyValueStateMachine::apply_committed(
    const std::vector<LogEntry>& log, std::uint64_t commit_index) {
    if (commit_index < last_applied_ || commit_index > log.size()) {
        throw std::logic_error("Invalid committed log prefix");
    }
    std::vector<AppliedResult> results;
    while (last_applied_ < commit_index) {
        const auto& entry = log[last_applied_];
        if (entry.index != last_applied_ + 1) throw std::logic_error("Log application must follow index order");
        auto result = execute(entry.command);
        ++last_applied_;
        results.push_back({last_applied_, std::move(result)});
    }
    return results;
}

} // namespace zmq_template::server
