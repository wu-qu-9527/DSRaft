#include "server/rpc/heartbeat_rpc.hpp"

#include "common/binary_codec.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace zmq_template {
namespace {
void validate_request(const HeartbeatRequest& request) {
    if ((request.prev_log_index == 0) != (request.prev_log_term == 0)
        || request.prev_log_term > request.term) {
        throw std::invalid_argument("Invalid AppendEntries predecessor");
    }
    auto index = request.prev_log_index;
    auto term = request.prev_log_term;
    for (const auto& entry : request.entries) {
        if (index == std::numeric_limits<std::uint64_t>::max() || entry.index != index + 1
            || entry.term == 0 || entry.term < term || entry.term > request.term) {
            throw std::invalid_argument("Invalid AppendEntries log sequence");
        }
        index = entry.index;
        term = entry.term;
    }
}
} // namespace

Message make_heartbeat_request(std::uint32_t rpc_id, const HeartbeatRequest& request) {
    validate_request(request);
    Message message{MessageType::heartbeat, rpc_id, {}};
    binary::append_u64(message.payload, request.term);
    binary::append_u64(message.payload, request.leader_id);
    binary::append_u64(message.payload, request.prev_log_index);
    binary::append_u64(message.payload, request.prev_log_term);
    binary::append_u64(message.payload, request.leader_commit);
    binary::append_u64(message.payload, request.entries.size());
    for (const auto& entry : request.entries) {
        if (entry.command.size() > max_payload_size - 24
            || message.payload.size() > max_payload_size - 24 - entry.command.size()) {
            throw std::invalid_argument("AppendEntries payload exceeds 1 MiB");
        }
        binary::append_u64(message.payload, entry.index);
        binary::append_u64(message.payload, entry.term);
        binary::append_u64(message.payload, entry.command.size());
        message.payload.append(entry.command);
    }
    return message;
}

Message make_heartbeat_response(std::uint32_t rpc_id, const HeartbeatResponse& response) {
    Message message{MessageType::heartbeat_response, rpc_id, {}};
    binary::append_u64(message.payload, response.term);
    message.payload.push_back(response.success ? '\1' : '\0');
    return message;
}

HeartbeatRequest decode_heartbeat_request(const Message& message) {
    if (message.type != MessageType::heartbeat || message.payload.size() < 48
        || message.payload.size() > max_payload_size) {
        throw std::invalid_argument("Invalid AppendEntries request");
    }
    HeartbeatRequest request{binary::read_u64(message.payload, 0), binary::read_u64(message.payload, 8),
        binary::read_u64(message.payload, 16), binary::read_u64(message.payload, 24), binary::read_u64(message.payload, 32), {}};
    const auto count = binary::read_u64(message.payload, 40);
    std::size_t offset = 48;
    if (count > (message.payload.size() - offset) / 24) throw std::invalid_argument("Invalid entry count");
    for (std::uint64_t i = 0; i < count; ++i) {
        if (message.payload.size() - offset < 24) throw std::invalid_argument("Truncated log entry");
        server::LogEntry entry{binary::read_u64(message.payload, offset), binary::read_u64(message.payload, offset + 8), {}};
        const auto length = binary::read_u64(message.payload, offset + 16);
        offset += 24;
        if (length > message.payload.size() - offset) throw std::invalid_argument("Truncated log command");
        entry.command = message.payload.substr(offset, static_cast<std::size_t>(length));
        offset += static_cast<std::size_t>(length);
        request.entries.push_back(std::move(entry));
    }
    if (offset != message.payload.size()) throw std::invalid_argument("Unexpected trailing AppendEntries data");
    validate_request(request);
    return request;
}

HeartbeatResponse decode_heartbeat_response(const Message& message) {
    if (message.type != MessageType::heartbeat_response || message.payload.size() != 9
        || (message.payload[8] != '\0' && message.payload[8] != '\1')) {
        throw std::invalid_argument("Invalid heartbeat response");
    }
    return {binary::read_u64(message.payload, 0), message.payload[8] == '\1'};
}
} // namespace zmq_template
