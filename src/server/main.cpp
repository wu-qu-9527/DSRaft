#include "common/logging.hpp"
#include "server/server.hpp"
#include "server/config_parser.hpp"

#include <exception>
#include <iostream>
#include <string>

using zmq_template::server::server;

int main(int argc, char* argv[]) {
    if (argc == 2 && std::string(argv[1]) == "--help") {
        std::cout << "Usage: zmq_server [bind_endpoint] [--node-id ID] [--peer ID=endpoint ...]\n";
        return 0;
    }

    zmq_template::init_logging("server");

    try {
        return server(zmq_template::server::parse_node_config(argc, argv))
            .wait_util_stop();
    } catch (const std::exception& error) {
        spdlog::error("Server error: {}", error.what());
        return 1;
    }
}
