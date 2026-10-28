#include "server/rpc/heartbeat_rpc.hpp"

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

Message make_heartbeat_request(std::uint32_t rpc_id, const HeartbeatRequest& request) {
    Message message{MessageType::heartbeat, rpc_id, {}};
    append_u64(message.payload, request.term);
    append_u64(message.payload, request.leader_id);
    append_u64(message.payload, request.prev_log_index);
    append_u64(message.payload, request.prev_log_term);
    append_u64(message.payload, request.leader_commit);
    return message;
}

Message make_heartbeat_response(std::uint32_t rpc_id, const HeartbeatResponse& response) {
    Message message{MessageType::heartbeat_response, rpc_id, {}};
    append_u64(message.payload, response.term);
    message.payload.push_back(response.success ? '\1' : '\0');
    return message;
}

HeartbeatRequest decode_heartbeat_request(const Message& message) {
    if (message.type != MessageType::heartbeat || message.payload.size() != 40) {
        throw std::invalid_argument("Invalid heartbeat request");
    }
    return {read_u64(message.payload, 0), read_u64(message.payload, 8),
            read_u64(message.payload, 16), read_u64(message.payload, 24),
            read_u64(message.payload, 32)};
}

HeartbeatResponse decode_heartbeat_response(const Message& message) {
    if (message.type != MessageType::heartbeat_response || message.payload.size() != 9
        || (message.payload[8] != '\0' && message.payload[8] != '\1')) {
        throw std::invalid_argument("Invalid heartbeat response");
    }
    return {read_u64(message.payload, 0), message.payload[8] == '\1'};
}
} // namespace zmq_template
