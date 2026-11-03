#pragma once

#include "server/rpc/heartbeat_rpc.hpp"

#include <algorithm>
#include <stdexcept>

namespace zmq_template::server {

// Progress for one peer in one leadership term. One request may be outstanding.
class LogReplication {
public:
    void reset(std::uint64_t last_index) {
        next_index_ = last_index + 1;
        match_index_ = 0;
    }

    HeartbeatRequest make_request(const std::vector<LogEntry>& log, std::uint64_t term,
                                  std::uint64_t leader_id, std::uint64_t commit_index) {
        const auto previous = next_index_ - 1;
        HeartbeatRequest request{term, leader_id, previous,
            previous == 0 ? 0 : log.at(previous - 1).term, commit_index, {}};
        std::size_t bytes = 48;
        // Bound both payload size and entry count so each peer gets incremental batches.
        for (std::size_t i = static_cast<std::size_t>(previous); i < log.size() && request.entries.size() < 128; ++i) {
            if (log[i].command.size() > max_payload_size - 48 - 24) {
                throw std::length_error("A log command is too large for AppendEntries");
            }
            const auto size = 24 + log[i].command.size();
            if (size > max_payload_size - bytes) break;
            request.entries.push_back(log[i]);
            bytes += size;
        }
        sent_last_index_ = request.entries.empty() ? previous : request.entries.back().index;
        return request;
    }

    void acknowledge(bool success) {
        if (success) {
            match_index_ = std::max(match_index_, sent_last_index_);
            next_index_ = match_index_ + 1;
        } else if (next_index_ > match_index_ + 1) {
            --next_index_;
        }
    }

    std::uint64_t next_index() const { return next_index_; }
    std::uint64_t match_index() const { return match_index_; }

private:
    std::uint64_t next_index_{1};
    std::uint64_t match_index_{0};
    std::uint64_t sent_last_index_{0};
};

} // namespace zmq_template::server
