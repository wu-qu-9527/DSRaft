#include "common/logging.hpp"
#include "server/server.hpp"
#include "server/config_parser.hpp"

#include <exception>
#include <iostream>
#include <string>

using zmq_template::server::server;

int main(int argc, char* argv[]) {
    if (argc == 2 && std::string(argv[1]) == "--help") {
        std::cout << "Usage: zmq_server [bind_endpoint] [options]\n"
                  << "  --node-id ID                    Positive node ID (default: 1)\n"
                  << "  --data-dir PATH                 State directory (default: data/node-<id>)\n"
                  << "  --peer ID=endpoint              Peer address; may be repeated\n"
                  << "  --heartbeat-interval-ms MS      Heartbeat send interval (default: 50)\n"
                  << "  --heartbeat-timeout-ms MS       Heartbeat RPC timeout (default: 100)\n"
                  << "  --election-timeout-min-ms MS    Minimum election timeout (default: 150)\n"
                  << "  --election-timeout-max-ms MS    Maximum election timeout (default: 300)\n"
                  << "All times are positive integer milliseconds. Heartbeat interval must be less\n"
                  << "than the minimum election timeout; minimum must not exceed maximum.\n";
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
