#pragma once

#include "server/log_entry.hpp"

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace zmq_template::server {

struct AppliedResult {
    std::uint64_t index;
    std::string result;
};

// No external side effects: state can be reconstructed by replaying a committed prefix.
class KeyValueStateMachine {
public:
    // Empty log commands are reserved for internal no-op entries.
    static void validate_command(const std::string& command);
    std::vector<AppliedResult> apply_committed(const std::vector<LogEntry>& log, std::uint64_t commit_index);
    std::uint64_t last_applied() const { return last_applied_; }
    std::optional<std::string> get(const std::string& key) const;

private:
    std::string execute(const std::string& command);
    std::map<std::string, std::string> values_;
    std::uint64_t last_applied_{0};
};

} // namespace zmq_template::server
