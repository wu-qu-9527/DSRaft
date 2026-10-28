#pragma once

#include "server/rpc/vote_rpc.hpp"
#include "server/election_timer.hpp"

namespace zmq_template::server {

inline RequestVoteResponse voteFor(
    const RequestVoteRequest& request, VoteState& state, Role& current_role,
    ElectionTimer& timer, ElectionTimer::Clock::time_point now = ElectionTimer::Clock::now()) {
    if (request.term < state.current_term) return {state.current_term, false};
    if (request.term > state.current_term) {
        state.current_term = request.term;
        state.voted_for.reset();
        state.leader_id.reset();
        current_role = Role::Follower;
    }
    const bool up_to_date = request.last_log_term > state.last_log_term
        || (request.last_log_term == state.last_log_term
            && request.last_log_index >= state.last_log_index);
    const bool can_vote = !state.voted_for || *state.voted_for == request.candidate_id;
    const bool granted = can_vote && up_to_date;
    if (granted) {
        state.voted_for = request.candidate_id;
        timer.reset(now);
    }
    return {state.current_term, granted};
}

} // namespace zmq_template::server
