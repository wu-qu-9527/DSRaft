#include "server/rpc/snapshot_rpc.hpp"
#include "common/binary_codec.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
void validate(const SnapshotRequest& request) {
    if (request.term == 0 || request.leader_id == 0 || request.index == 0 || request.snapshot_term == 0
        || request.snapshot_term > request.term || request.total == 0 || request.offset >= request.total
        || request.data.empty() || request.data.size() > snapshot_chunk_size
        || request.data.size() > request.total - request.offset) throw std::invalid_argument("Invalid snapshot chunk");
}
} // namespace
Message make_snapshot_request(std::uint32_t id, const SnapshotRequest& request) {
    validate(request);
    Message message{MessageType::install_snapshot, id, {}};
    for (auto value : {request.term, request.leader_id, request.index, request.snapshot_term, request.offset, request.total}) {
        binary::append_u64(message.payload, value);
    }
    message.payload += request.data;
    return message;
}
SnapshotRequest decode_snapshot_request(const Message& message) {
    if (message.type != MessageType::install_snapshot || message.payload.size() < 49
        || message.payload.size() > 48 + snapshot_chunk_size) throw std::invalid_argument("Invalid InstallSnapshot payload");
    SnapshotRequest request{binary::read_u64(message.payload, 0), binary::read_u64(message.payload, 8),
        binary::read_u64(message.payload, 16), binary::read_u64(message.payload, 24),
        binary::read_u64(message.payload, 32), binary::read_u64(message.payload, 40), message.payload.substr(48)};
    validate(request);
    return request;
}
Message make_snapshot_response(std::uint32_t id, const SnapshotResponse& response) {
    Message message{MessageType::install_snapshot_response, id, {}};
    binary::append_u64(message.payload, response.term);
    message.payload.push_back(response.success ? '\1' : '\0');
    binary::append_u64(message.payload, response.next_offset);
    return message;
}
SnapshotResponse decode_snapshot_response(const Message& message) {
    if (message.type != MessageType::install_snapshot_response || message.payload.size() != 17
        || (message.payload[8] != '\0' && message.payload[8] != '\1')) throw std::invalid_argument("Invalid InstallSnapshot response");
    return {binary::read_u64(message.payload, 0), message.payload[8] == '\1', binary::read_u64(message.payload, 9)};
}
std::string snapshot_payload(const server::Snapshot& snapshot) {
    std::string bytes;
    binary::append_u64(bytes, snapshot.configuration.size());
    return bytes + snapshot.configuration + snapshot.state;
}
server::Snapshot snapshot_from_payload(std::uint64_t index, std::uint64_t term, const std::string& payload) {
    if (payload.size() < 8) throw std::invalid_argument("Truncated snapshot payload");
    const auto size = binary::read_u64(payload, 0);
    if (size > payload.size() - 8) throw std::invalid_argument("Truncated snapshot configuration");
    return {index, term, payload.substr(8 + size), payload.substr(8, size)};
}
} // namespace zmq_template
