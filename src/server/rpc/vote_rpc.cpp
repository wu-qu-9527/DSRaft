#include "server/rpc/vote_rpc.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
void append_u64(std::string& bytes, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::uint64_t read_u64(std::string_view bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<unsigned char>(bytes[offset + i]);
    }
    return value;
}
} // namespace

Message make_vote_request(std::uint32_t rpc_id, const RequestVoteRequest& request) {
    Message message{MessageType::request_vote, rpc_id, {}};
    append_u64(message.payload, request.term);
    append_u64(message.payload, request.candidate_id);
    append_u64(message.payload, request.last_log_index);
    append_u64(message.payload, request.last_log_term);
    return message;
}

Message make_vote_response(std::uint32_t rpc_id, const RequestVoteResponse& response) {
    Message message{MessageType::request_vote_response, rpc_id, {}};
    append_u64(message.payload, response.term);
    message.payload.push_back(response.vote_granted ? '\1' : '\0');
    return message;
}

RequestVoteRequest decode_vote_request(const Message& message) {
    if (message.type != MessageType::request_vote || message.payload.size() != 32) {
        throw std::invalid_argument("Invalid RequestVote request");
    }
    return {read_u64(message.payload, 0), read_u64(message.payload, 8),
            read_u64(message.payload, 16), read_u64(message.payload, 24)};
}

RequestVoteResponse decode_vote_response(const Message& message) {
    if (message.type != MessageType::request_vote_response || message.payload.size() != 9
        || (message.payload[8] != '\0' && message.payload[8] != '\1')) {
        throw std::invalid_argument("Invalid RequestVote response");
    }
    return {read_u64(message.payload, 0), message.payload[8] == '\1'};
}
} // namespace zmq_template
