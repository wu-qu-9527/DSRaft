#pragma once

#include <cstdint>
#include <optional>

namespace zmq_template::server {

enum class Role {
    Follower,
    Candidate,
    Leader,
};

// Term and vote are persisted before protocol output; last-log metadata is recovered from stored entries.
struct VoteState {
    std::uint64_t current_term{0};
    std::optional<std::uint64_t> voted_for;
    std::uint64_t last_log_index{0};
    std::uint64_t last_log_term{0};
    std::uint64_t commit_index{0};
    std::optional<std::uint64_t> leader_id;
};

constexpr const char* to_string(Role value) noexcept {
    switch (value) {
    case Role::Follower: return "Follower";
    case Role::Candidate: return "Candidate";
    case Role::Leader: return "Leader";
    }
    return "Unknown";
}

} // namespace zmq_template::server
