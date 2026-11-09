#pragma once

#include "server/rpc/vote_rpc.hpp"
#include "server/config/node_config.hpp"
#include "server/state.hpp"

#include <limits>
#include <set>
#include <stdexcept>

namespace zmq_template::server {

class Election {
public:
    Election(const NodeConfig& config, TermVoteState& term_vote, const LogState& log_state,
             LeadershipState& leadership)
        : node_id_(config.id), term_vote_(term_vote), log_state_(log_state), leadership_(leadership) {
        members_.insert(config.id);
        for (const auto& peer : config.peers) members_.insert(peer.id);
    }

    RequestVoteRequest start() {
        if (term_vote_.current_term == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("Raft term exhausted");
        }
        ++term_vote_.current_term;
        leadership_.role = Role::Candidate;
        term_vote_.voted_for = node_id_;
        leadership_.leader_id.reset();
        votes_ = {node_id_};
        promote_if_majority();
        return {term_vote_.current_term, node_id_, log_state_.last_log_index, log_state_.last_log_term};
    }

    void handle_vote_response(std::uint64_t peer_id, std::uint64_t request_term,
                        const RequestVoteResponse& response) {
        if (peer_id == node_id_ || members_.count(peer_id) == 0) return;
        if (become_follower_if_higher_term(response.term)) return;
        if (leadership_.role != Role::Candidate || request_term != term_vote_.current_term
            || response.term != term_vote_.current_term || !response.vote_granted) return;
        votes_.insert(peer_id);
        promote_if_majority();
    }

    std::size_t votes() const { return votes_.size(); }
    std::size_t majority() const { return members_.size() / 2 + 1; }

    bool become_follower_if_higher_term(std::uint64_t term) {
        if (term <= term_vote_.current_term) return false;
        term_vote_.current_term = term;
        term_vote_.voted_for.reset();
        leadership_.leader_id.reset();
        leadership_.role = Role::Follower;
        votes_.clear();
        return true;
    }

private:
    void promote_if_majority() {
        if (votes_.size() >= majority()) {
            leadership_.role = Role::Leader;
            leadership_.leader_id = node_id_;
        }
    }
    std::uint64_t node_id_;
    TermVoteState& term_vote_;
    const LogState& log_state_;
    LeadershipState& leadership_;
    std::set<std::uint64_t> members_;
    std::set<std::uint64_t> votes_;
};

} // namespace zmq_template::server
