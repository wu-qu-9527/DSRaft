#pragma once

#include "server/log_entry.hpp"

#include <algorithm>
#include <functional>
#include <vector>

namespace zmq_template::server {

// Peer matches exclude the local node, whose complete log is already durable.
inline std::uint64_t majority_commit_index(const std::vector<LogEntry>& log, std::uint64_t current_term,
                                         std::uint64_t committed, std::vector<std::uint64_t> peer_matches,
                                         std::uint64_t base_index = 0) {
    peer_matches.push_back(base_index + log.size());
    std::sort(peer_matches.begin(), peer_matches.end(), std::greater<std::uint64_t>{});
    const auto candidate = peer_matches[peer_matches.size() / 2];
    if (candidate <= committed || candidate <= base_index || candidate - base_index > log.size()
        || log[candidate - base_index - 1].term != current_term) {
        return committed;
    }
    return candidate;
}

} // namespace zmq_template::server
