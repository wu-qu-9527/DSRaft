#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
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
    std::chrono::milliseconds heartbeat_interval{50};
    std::chrono::milliseconds heartbeat_timeout{100};
    std::chrono::milliseconds election_timeout_min{150};
    std::chrono::milliseconds election_timeout_max{300};
    std::filesystem::path data_dir; // Empty uses data/node-<id>.
};

} // namespace zmq_template::server
