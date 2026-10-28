#pragma once

#include "common/message.hpp"

namespace zmq_template {

struct RequestVoteRequest {
    std::uint64_t term{0};
    std::uint64_t candidate_id{0};
    std::uint64_t last_log_index{0};
    std::uint64_t last_log_term{0};
};

struct RequestVoteResponse {
    std::uint64_t term{0};
    bool vote_granted{false};
};

Message make_vote_request(std::uint32_t rpc_id, const RequestVoteRequest& request);
Message make_vote_response(std::uint32_t rpc_id, const RequestVoteResponse& response);
RequestVoteRequest decode_vote_request(const Message& message);
RequestVoteResponse decode_vote_response(const Message& message);

} // namespace zmq_template
