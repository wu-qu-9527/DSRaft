#pragma once

#include "server/rpc/heartbeat_rpc.hpp"
#include "server/election_timer.hpp"
#include "server/state.hpp"

#include <algorithm>

namespace zmq_template::server {

inline HeartbeatResponse handle_heartbeat(
    const HeartbeatRequest& request, VoteState& state, Role& role,
    ElectionTimer& timer, ElectionTimer::Clock::time_point now = ElectionTimer::Clock::now()) {
    if (request.term < state.current_term) return {state.current_term, false};
    if (request.term > state.current_term) {
        state.current_term = request.term;
        state.voted_for.reset();
    }
    // A current-term leader's contact suppresses elections even if log matching fails.
    role = Role::Follower;
    state.leader_id = request.leader_id;
    timer.reset(now);
    // Until a log store exists, only the origin and local tail can be checked.
    const bool matches = (request.prev_log_index == 0 && request.prev_log_term == 0)
        || (request.prev_log_index == state.last_log_index && request.prev_log_term == state.last_log_term);
    if (!matches) return {state.current_term, false};
    state.commit_index = std::max(state.commit_index,
                                  std::min(request.leader_commit, request.prev_log_index));
    return {state.current_term, true};
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
