#pragma once

#include "server/rpc/vote_rpc.hpp"
#include "server/node_config.hpp"
#include "server/state.hpp"

#include <limits>
#include <set>
#include <stdexcept>

namespace zmq_template::server {

class Election {
public:
    Election(const NodeConfig& config, VoteState& state, Role& role)
        : node_id_(config.id), state_(state), role_(role) {
        members_.insert(config.id);
        for (const auto& peer : config.peers) members_.insert(peer.id);
    }

    RequestVoteRequest start() {
        if (state_.current_term == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("Raft term exhausted");
        }
        ++state_.current_term;
        role_ = Role::Candidate;
        state_.voted_for = node_id_;
        state_.leader_id.reset();
        votes_ = {node_id_};
        promote_if_majority();
        return {state_.current_term, node_id_, state_.last_log_index, state_.last_log_term};
    }

    void handle_vote_response(std::uint64_t peer_id, std::uint64_t request_term,
                        const RequestVoteResponse& response) {
        if (peer_id == node_id_ || members_.count(peer_id) == 0) return;
        if (become_follower_if_higher_term(response.term)) return;
        if (role_ != Role::Candidate || request_term != state_.current_term
            || response.term != state_.current_term || !response.vote_granted) return;
        votes_.insert(peer_id);
        promote_if_majority();
    }

    std::size_t votes() const { return votes_.size(); }
    std::size_t majority() const { return members_.size() / 2 + 1; }

    bool become_follower_if_higher_term(std::uint64_t term) {
        if (term <= state_.current_term) return false;
        state_.current_term = term;
        state_.voted_for.reset();
        state_.leader_id.reset();
        role_ = Role::Follower;
        votes_.clear();
        return true;
    }

private:
    void promote_if_majority() {
        if (votes_.size() >= majority()) {
            role_ = Role::Leader;
            state_.leader_id = node_id_;
        }
    }
    std::uint64_t node_id_;
    VoteState& state_;
    Role& role_;
    std::set<std::uint64_t> members_;
    std::set<std::uint64_t> votes_;
};

} // namespace zmq_template::server
