#pragma once

#include <cstdint>
#include <optional>

namespace zmq_template::server {

enum class Role {
    Follower,
    Candidate,
    Leader,
};

// Persisted together before any protocol output that depends on the term or vote.
struct TermVoteState {
    std::uint64_t current_term{0};
    std::optional<std::uint64_t> voted_for;
};

// Recovered from the durable log; updated whenever its suffix changes.
struct LogState {
    std::uint64_t last_log_index{0};
    std::uint64_t last_log_term{0};
};

// The committed prefix is checkpointed before state-machine application.
struct CommitState {
    std::uint64_t commit_index{0};
};

// Volatile: a restarted node always begins as a Follower with no known leader.
struct LeadershipState {
    Role role{Role::Follower};
    std::optional<std::uint64_t> leader_id;
};

struct PersistentCommit {
    std::uint64_t index{0};
    std::uint64_t term{0}; // Binds the checkpoint to the corresponding durable log entry.
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
