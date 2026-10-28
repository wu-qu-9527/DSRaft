#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zmq_template::server {

struct PeerConfig {
    std::uint64_t id;
    std::string endpoint;
};

struct NodeConfig {
    std::uint64_t id{1};
    std::string endpoint{"tcp://127.0.0.1:5555"};
    std::vector<PeerConfig> peers;
};

} // namespace zmq_template::server
