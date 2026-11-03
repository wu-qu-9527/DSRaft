#include "server/rpc/heartbeat_rpc.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

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
    append_u64(message.payload, request.term);
    append_u64(message.payload, request.leader_id);
    append_u64(message.payload, request.prev_log_index);
    append_u64(message.payload, request.prev_log_term);
    append_u64(message.payload, request.leader_commit);
    append_u64(message.payload, request.entries.size());
    for (const auto& entry : request.entries) {
        if (entry.command.size() > max_payload_size - 24
            || message.payload.size() > max_payload_size - 24 - entry.command.size()) {
            throw std::invalid_argument("AppendEntries payload exceeds 1 MiB");
        }
        append_u64(message.payload, entry.index);
        append_u64(message.payload, entry.term);
        append_u64(message.payload, entry.command.size());
        message.payload.append(entry.command);
    }
    return message;
}

Message make_heartbeat_response(std::uint32_t rpc_id, const HeartbeatResponse& response) {
    Message message{MessageType::heartbeat_response, rpc_id, {}};
    append_u64(message.payload, response.term);
    message.payload.push_back(response.success ? '\1' : '\0');
    return message;
}

HeartbeatRequest decode_heartbeat_request(const Message& message) {
    if (message.type != MessageType::heartbeat || message.payload.size() < 48
        || message.payload.size() > max_payload_size) {
        throw std::invalid_argument("Invalid AppendEntries request");
    }
    HeartbeatRequest request{read_u64(message.payload, 0), read_u64(message.payload, 8),
        read_u64(message.payload, 16), read_u64(message.payload, 24), read_u64(message.payload, 32), {}};
    const auto count = read_u64(message.payload, 40);
    std::size_t offset = 48;
    if (count > (message.payload.size() - offset) / 24) throw std::invalid_argument("Invalid entry count");
    for (std::uint64_t i = 0; i < count; ++i) {
        if (message.payload.size() - offset < 24) throw std::invalid_argument("Truncated log entry");
        server::LogEntry entry{read_u64(message.payload, offset), read_u64(message.payload, offset + 8), {}};
        const auto length = read_u64(message.payload, offset + 16);
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
    return {read_u64(message.payload, 0), message.payload[8] == '\1'};
}
} // namespace zmq_template
