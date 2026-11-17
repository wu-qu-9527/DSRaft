#include "server/state_machine/key_value_state_machine.hpp"
#include "common/client_request.hpp"
#include "server/membership.hpp"
#include "common/binary_codec.hpp"

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

void KeyValueStateMachine::validate_command(const std::string& command) {
    (void)parse(is_client_request(command) ? decode_client_request(command).command : command);
}

std::optional<std::string> KeyValueStateMachine::get(const std::string& key) const {
    const auto found = values_.find(key);
    if (found == values_.end()) return std::nullopt;
    return found->second;
}

std::string KeyValueStateMachine::execute(const std::string& command) {
    if (command.empty() || is_membership(command)) return {}; // Internal no-op.
    try {
        if (!is_client_request(command)) return execute_business(command);
        const auto request = decode_client_request(command);
        (void)parse(request.command);
        const auto found = sessions_.find(request.client_id);
        if (found != sessions_.end()) {
            if (request.sequence < found->second.sequence) return "ERROR stale request sequence";
            if (request.sequence == found->second.sequence) {
                return request.command == found->second.command ? found->second.result
                    : "ERROR request sequence reused with different command";
            }
        }
        auto result = execute_business(request.command);
        sessions_[request.client_id] = {request.sequence, request.command, result};
        return result;
    } catch (const std::invalid_argument&) {
        return "ERROR invalid command";
    }
}

std::string KeyValueStateMachine::execute_business(const std::string& command) {
    const auto parsed = parse(command);
    if (parsed.operation == "SET") {
        values_[parsed.key] = parsed.value;
        return "OK";
    }
    if (parsed.operation == "DEL") return values_.erase(parsed.key) ? "OK" : "NOT_FOUND";
    const auto value = get(parsed.key);
    return value ? "VALUE " + *value : "NOT_FOUND";
}

std::vector<AppliedResult> KeyValueStateMachine::apply_committed(
    const std::vector<LogEntry>& log, std::uint64_t commit_index, std::uint64_t base_index) {
    if (last_applied_ < base_index || commit_index < last_applied_ || commit_index - base_index > log.size()) {
        throw std::logic_error("Invalid committed log prefix");
    }
    std::vector<AppliedResult> results;
    while (last_applied_ < commit_index) {
        const auto& entry = log[last_applied_ - base_index];
        if (entry.index != last_applied_ + 1) throw std::logic_error("Log application must follow index order");
        auto result = execute(entry.command);
        ++last_applied_;
        results.push_back({last_applied_, std::move(result)});
    }
    return results;
}

std::string KeyValueStateMachine::snapshot() const {
    std::string bytes{"KVS1"};
    const auto write = [&](const std::string& value) {
        binary::append_u64(bytes, value.size());
        bytes += value;
    };
    binary::append_u64(bytes, last_applied_);
    binary::append_u64(bytes, values_.size());
    for (const auto& item : values_) { write(item.first); write(item.second); }
    binary::append_u64(bytes, sessions_.size());
    for (const auto& item : sessions_) {
        write(item.first);
        binary::append_u64(bytes, item.second.sequence);
        write(item.second.command);
        write(item.second.result);
    }
    return bytes;
}

void KeyValueStateMachine::restore(const std::string& bytes, std::uint64_t index) {
    if (bytes.substr(0, 4) != "KVS1") throw std::invalid_argument("Invalid state snapshot version");
    std::size_t offset = 4;
    const auto number = [&]() {
        if (bytes.size() - offset < 8) throw std::invalid_argument("Truncated state snapshot");
        const auto value = binary::read_u64(bytes, offset);
        offset += 8;
        return value;
    };
    const auto read = [&]() {
        const auto size = number();
        if (size > bytes.size() - offset) throw std::invalid_argument("Truncated snapshot field");
        auto value = bytes.substr(offset, size);
        offset += size;
        return value;
    };
    KeyValueStateMachine restored;
    restored.last_applied_ = number();
    if (restored.last_applied_ != index) throw std::invalid_argument("Snapshot applied index mismatch");
    const auto values = number();
    for (std::uint64_t i = 0; i < values; ++i) {
        auto key = read();
        auto value = read();
        if (!restored.values_.emplace(std::move(key), std::move(value)).second) throw std::invalid_argument("Duplicate snapshot key");
    }
    const auto sessions = number();
    for (std::uint64_t i = 0; i < sessions; ++i) {
        auto client = read();
        const auto sequence = number();
        auto command = read();
        auto result = read();
        if (client.empty() || client.size() > 128 || sequence == 0) throw std::invalid_argument("Invalid snapshot session");
        (void)parse(command);
        if (!restored.sessions_.emplace(std::move(client), Session{sequence, std::move(command), std::move(result)}).second) {
            throw std::invalid_argument("Duplicate snapshot session");
        }
    }
    if (offset != bytes.size()) throw std::invalid_argument("Trailing state snapshot bytes");
    *this = std::move(restored);
}

} // namespace zmq_template::server
