#include "server/rpc/vote_rpc.hpp"

#include "common/binary_codec.hpp"

#include <stdexcept>

namespace zmq_template {

Message make_vote_request(std::uint32_t rpc_id, const RequestVoteRequest& request) {
    Message message{MessageType::request_vote, rpc_id, {}};
    binary::append_u64(message.payload, request.term);
    binary::append_u64(message.payload, request.candidate_id);
    binary::append_u64(message.payload, request.last_log_index);
    binary::append_u64(message.payload, request.last_log_term);
    return message;
}

Message make_vote_response(std::uint32_t rpc_id, const RequestVoteResponse& response) {
    Message message{MessageType::request_vote_response, rpc_id, {}};
    binary::append_u64(message.payload, response.term);
    message.payload.push_back(response.vote_granted ? '\1' : '\0');
    return message;
}

RequestVoteRequest decode_vote_request(const Message& message) {
    if (message.type != MessageType::request_vote || message.payload.size() != 32) {
        throw std::invalid_argument("Invalid RequestVote request");
    }
    return {binary::read_u64(message.payload, 0), binary::read_u64(message.payload, 8),
            binary::read_u64(message.payload, 16), binary::read_u64(message.payload, 24)};
}

RequestVoteResponse decode_vote_response(const Message& message) {
    if (message.type != MessageType::request_vote_response || message.payload.size() != 9
        || (message.payload[8] != '\0' && message.payload[8] != '\1')) {
        throw std::invalid_argument("Invalid RequestVote response");
    }
    return {binary::read_u64(message.payload, 0), message.payload[8] == '\1'};
}
} // namespace zmq_template
