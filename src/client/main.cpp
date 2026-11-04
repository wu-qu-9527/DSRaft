#include "common/zmq_transport.hpp"
#include "common/logging.hpp"

#include <exception>
#include <iostream>
#include <stdexcept>
#include <vector>

int main(int argc, char* argv[]) {
    const bool all = argc >= 2 && std::string(argv[1]) == "--all";
    if ((argc >= 2 && std::string(argv[1]).rfind("--", 0) == 0 && !all)
        || (!all && argc > 3) || (all && argc < 3)) {
        std::cerr << "Usage: zmq_client [connect_endpoint] [command]\n"
                  << "       zmq_client --all endpoint [endpoint ...]\n";
        return 2;
    }
    try {
        zmq_template::init_logging("client");
        std::vector<std::string> endpoints;
        if (all) {
            for (int i = 2; i < argc; ++i) endpoints.emplace_back(argv[i]);
        } else {
            endpoints.emplace_back(argc >= 2 ? argv[1] : "tcp://127.0.0.1:5555");
        }
        const std::string message = !all && argc >= 3 ? argv[2] : "SET greeting Hello, ZeroMQ";
        zmq::context_t context{1};
        std::vector<zmq::socket_t> sockets;
        for (const auto& endpoint : endpoints) {
            sockets.emplace_back(context, zmq::socket_type::req);
            zmq_template::configure_socket(sockets.back());
            sockets.back().connect(endpoint);
            spdlog::debug("Connecting to {}", endpoint);
        }
        const zmq_template::Message outgoing{zmq_template::MessageType::text, 1, message};
        // Each server has its own REQ socket; each socket has one pending request.
        for (std::size_t i = 0; i < sockets.size(); ++i) {
            zmq_template::send_message(sockets[i], outgoing);
            spdlog::debug("Queued request id={} endpoint={} payload_bytes={}",
                          outgoing.id, endpoints[i], outgoing.payload.size());
        }
        bool failed = false;
        for (std::size_t i = 0; i < sockets.size(); ++i) {
            const auto reply = zmq_template::receive_message(sockets[i]);
            if (reply.id != outgoing.id || (reply.type != zmq_template::MessageType::acknowledgement
                && reply.type != zmq_template::MessageType::error)) {
                throw std::runtime_error("Unexpected reply type or message id");
            }
            if (all) std::cout << '[' << endpoints[i] << "] ";
            if (reply.type == zmq_template::MessageType::error) {
                std::cout << "Error: ";
                failed = true;
            }
            std::cout << reply.payload << '\n';
            spdlog::info("Received reply id={} endpoint={}", reply.id, endpoints[i]);
        }
        return failed ? 1 : 0;
    } catch (const std::exception& error) {
        spdlog::error("Client error: {}", error.what());
        return 1;
    }
}
