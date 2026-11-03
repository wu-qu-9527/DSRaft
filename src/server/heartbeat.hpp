#pragma once

#include "server/rpc/heartbeat_rpc.hpp"
#include "server/election_timer.hpp"
#include "server/state.hpp"

#include <algorithm>

namespace zmq_template::server {

struct HeartbeatResult {
    HeartbeatResponse response;
    std::optional<std::uint64_t> changed_from;
};

// Mutates in-memory state only. Persist term/vote and any changed suffix before replying.
// Request indices and terms must already have passed RPC validation.
inline HeartbeatResult handle_heartbeat(
    const HeartbeatRequest& request, VoteState& state, Role& role,
    ElectionTimer& timer, std::vector<LogEntry>& log,
    ElectionTimer::Clock::time_point now = ElectionTimer::Clock::now()) {
    if (request.term < state.current_term) return {{state.current_term, false}, std::nullopt};
    if (request.term > state.current_term) {
        state.current_term = request.term;
        state.voted_for.reset();
    }
    // A current-term leader's contact suppresses elections even if log matching fails.
    role = Role::Follower;
    state.leader_id = request.leader_id;
    timer.reset(now);
    if (request.prev_log_index > log.size()
        || (request.prev_log_index != 0 && log[request.prev_log_index - 1].term != request.prev_log_term)
        || (request.prev_log_index == 0 && request.prev_log_term != 0)) {
        return {{state.current_term, false}, std::nullopt};
    }
    std::size_t first_new = 0;
    while (first_new < request.entries.size()) {
        const auto& entry = request.entries[first_new];
        if (entry.index > log.size()) break;
        const auto& local = log[entry.index - 1];
        if (local.term != entry.term) {
            if (entry.index <= state.commit_index) return {{state.current_term, false}, std::nullopt};
            break;
        }
        // Same index/term must identify the same command; never silently rewrite it.
        if (local.command != entry.command) return {{state.current_term, false}, std::nullopt};
        ++first_new;
    }
    std::optional<std::uint64_t> changed_from;
    if (first_new < request.entries.size()) {
        changed_from = request.entries[first_new].index;
        log.resize(static_cast<std::size_t>(*changed_from - 1));
        log.insert(log.end(), request.entries.begin() + first_new, request.entries.end());
    }
    state.last_log_index = log.empty() ? 0 : log.back().index;
    state.last_log_term = log.empty() ? 0 : log.back().term;
    const auto matched_index = request.entries.empty() ? request.prev_log_index : request.entries.back().index;
    state.commit_index = std::max(state.commit_index, std::min(request.leader_commit, matched_index));
    return {{state.current_term, true}, changed_from};
}

class HeartbeatTimer {
public:
    using Clock = ElectionTimer::Clock;
    explicit HeartbeatTimer(Clock::time_point now = Clock::now())
        : HeartbeatTimer(std::chrono::milliseconds{50}, now) {}
    explicit HeartbeatTimer(std::chrono::milliseconds interval, Clock::time_point now = Clock::now())
        : interval_(interval), deadline_(now) {
        if (interval.count() <= 0) throw std::invalid_argument("Heartbeat interval must be positive");
    }
    void restart(Clock::time_point now = Clock::now()) { deadline_ = now; }
    bool tick(Clock::time_point now = Clock::now()) {
        if (now < deadline_) return false;
        deadline_ = now + interval_;
        return true;
    }
    std::chrono::milliseconds remaining(Clock::time_point now = Clock::now()) const {
        if (now >= deadline_) return std::chrono::milliseconds{0};
        return std::chrono::ceil<std::chrono::milliseconds>(deadline_ - now);
    }
private:
    std::chrono::milliseconds interval_;
    Clock::time_point deadline_;
};

} // namespace zmq_template::server
