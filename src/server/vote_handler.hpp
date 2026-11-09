#pragma once

#include "server/rpc/vote_rpc.hpp"
#include "server/election_timer.hpp"
#include "server/state.hpp"

namespace zmq_template::server {

inline RequestVoteResponse voteFor(
    const RequestVoteRequest& request, TermVoteState& term_vote, const LogState& log_state,
    LeadershipState& leadership, ElectionTimer& timer, ElectionTimer::Clock::time_point now = ElectionTimer::Clock::now()) {
    if (request.term < term_vote.current_term) return {term_vote.current_term, false};
    if (request.term > term_vote.current_term) {
        term_vote.current_term = request.term;
        term_vote.voted_for.reset();
        leadership.leader_id.reset();
        leadership.role = Role::Follower;
    }
    const bool up_to_date = request.last_log_term > log_state.last_log_term
        || (request.last_log_term == log_state.last_log_term
            && request.last_log_index >= log_state.last_log_index);
    const bool can_vote = !term_vote.voted_for || *term_vote.voted_for == request.candidate_id;
    const bool granted = can_vote && up_to_date;
    if (granted) {
        term_vote.voted_for = request.candidate_id;
        timer.reset(now);
    }
    return {term_vote.current_term, granted};
}

} // namespace zmq_template::server
