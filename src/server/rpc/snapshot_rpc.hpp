#pragma once

#include "common/message.hpp"
#include "server/log_entry.hpp"

namespace zmq_template {
inline constexpr std::size_t snapshot_chunk_size = 64 * 1024;
struct SnapshotRequest {
    std::uint64_t term, leader_id, index, snapshot_term, offset, total;
    std::string data;
};
struct SnapshotResponse {
    std::uint64_t term;
    bool success;
    std::uint64_t next_offset;
};
Message make_snapshot_request(std::uint32_t id, const SnapshotRequest& request);
SnapshotRequest decode_snapshot_request(const Message& message);
Message make_snapshot_response(std::uint32_t id, const SnapshotResponse& response);
SnapshotResponse decode_snapshot_response(const Message& message);
std::string snapshot_payload(const server::Snapshot& snapshot);
server::Snapshot snapshot_from_payload(std::uint64_t index, std::uint64_t term, const std::string& payload);
} // namespace zmq_template
