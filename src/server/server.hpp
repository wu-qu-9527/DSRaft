#pragma once

#include "server/node_config.hpp"
#include "server/state.hpp"

#include <memory>

namespace zmq_template::server {

class PeerRpc;

class server {
public:
    explicit server(NodeConfig config = {});

    int wait_util_stop();

private:
    struct Runtime;
    std::unique_ptr<Runtime> initialize(const NodeConfig& config);
    void handle_request(Runtime& runtime);
    void handle_peer_response(Runtime& runtime, PeerRpc& peer);

    int run1(const NodeConfig& config);
    int run(const NodeConfig& config);

    Role role_{Role::Follower};
    NodeConfig config_;
};

} // namespace zmq_template::server
