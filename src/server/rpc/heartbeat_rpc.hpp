#pragma once

#include "common/message.hpp"

namespace zmq_template {

// AppendEntries with an empty entries list. Log replication is not yet supported.
struct HeartbeatRequest {
    std::uint64_t term{0};
    std::uint64_t leader_id{0};
    std::uint64_t prev_log_index{0};
    std::uint64_t prev_log_term{0};
    std::uint64_t leader_commit{0};
};

struct HeartbeatResponse {
    std::uint64_t term{0};
    bool success{false};
};

Message make_heartbeat_request(std::uint32_t rpc_id, const HeartbeatRequest& request);
Message make_heartbeat_response(std::uint32_t rpc_id, const HeartbeatResponse& response);
HeartbeatRequest decode_heartbeat_request(const Message& message);
HeartbeatResponse decode_heartbeat_response(const Message& message);

} // namespace zmq_template
