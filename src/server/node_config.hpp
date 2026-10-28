#pragma once

#include <cstdint>
#include <string>
#include <string_view>
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

std::uint64_t parse_node_id(std::string_view text);
NodeConfig parse_node_config(int argc, char* argv[]);

} // namespace zmq_template::server
