#include "server/config_parser.hpp"

#include <charconv>
#include <set>
#include <stdexcept>

namespace zmq_template::server {
namespace {
std::chrono::milliseconds parse_milliseconds(std::string_view text) {
    int value{};
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || value <= 0) {
        throw std::invalid_argument("Timeouts must be positive integers in milliseconds (maximum 2147483647)");
    }
    return std::chrono::milliseconds{value};
}
} // namespace

std::uint64_t parse_node_id(std::string_view text) {
    std::uint64_t value{};
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || value == 0) {
        throw std::invalid_argument("Node IDs must be positive uint64 integers");
    }
    return value;
}

NodeConfig parse_node_config(int argc, char* argv[]) {
    NodeConfig config;
    int i = 1;
    if (i < argc && std::string_view(argv[i]).substr(0, 2) != "--") config.endpoint = argv[i++];
    std::set<std::string> options;
    while (i < argc) {
        const std::string option = argv[i++];
        if (i == argc) throw std::invalid_argument("Missing option value");
        const std::string value = argv[i++];
        if (option != "--peer" && !options.insert(option).second) {
            throw std::invalid_argument("Repeated option: " + option);
        }
        if (option == "--node-id") {
            config.id = parse_node_id(value);
        } else if (option == "--data-dir") {
            if (value.empty()) throw std::invalid_argument("Data directory must not be empty");
            config.data_dir = value;
        } else if (option == "--heartbeat-interval-ms") {
            config.heartbeat_interval = parse_milliseconds(value);
        } else if (option == "--heartbeat-timeout-ms") {
            config.heartbeat_timeout = parse_milliseconds(value);
        } else if (option == "--election-timeout-min-ms") {
            config.election_timeout_min = parse_milliseconds(value);
        } else if (option == "--election-timeout-max-ms") {
            config.election_timeout_max = parse_milliseconds(value);
        } else if (option == "--peer") {
            const auto separator = value.find('=');
            if (separator == std::string::npos || separator + 1 == value.size()) {
                throw std::invalid_argument("Peer syntax: --peer ID=endpoint");
            }
            config.peers.push_back({parse_node_id(std::string_view(value).substr(0, separator)),
                                    value.substr(separator + 1)});
        } else {
            throw std::invalid_argument("Unknown or repeated option: " + option);
        }
    }
    if (config.election_timeout_min > config.election_timeout_max) {
        throw std::invalid_argument("Election timeout minimum must not exceed maximum");
    }
    if (config.heartbeat_interval >= config.election_timeout_min) {
        throw std::invalid_argument("Heartbeat interval must be less than minimum election timeout");
    }
    std::set<std::uint64_t> ids{config.id};
    std::set<std::string> endpoints{config.endpoint};
    for (const auto& peer : config.peers) {
        if (!ids.insert(peer.id).second || !endpoints.insert(peer.endpoint).second) {
            throw std::invalid_argument("Duplicate/self peer ID or endpoint");
        }
    }
    return config;
}

} // namespace zmq_template::server
