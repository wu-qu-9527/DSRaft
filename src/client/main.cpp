#include "client/request_client.hpp"
#include "common/logging.hpp"

#include <charconv>
#include <exception>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace {
std::uint64_t positive(const std::string& text) {
    std::uint64_t value{};
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || value == 0) {
        throw std::invalid_argument("Expected a positive integer");
    }
    return value;
}
std::string new_client_id() {
    std::random_device random;
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (int i = 0; i < 4; ++i) out << std::setw(8) << random();
    return out.str();
}
void usage() {
    std::cout << "Usage: zmq_client [endpoint] [command] [options]\n"
              << "       zmq_client --cluster endpoint,endpoint,... [command] [options]\n"
              << "       zmq_client --all endpoint [endpoint ...]\n"
              << "Options: --client-id ID --request-seq N --timeout-ms MS --attempt-timeout-ms MS\n"
              << "Reuse ID, sequence and command after an unknown outcome; increment sequence only after completion.\n";
}
} // namespace

int main(int argc, char* argv[]) {
    try {
        zmq_template::init_logging("client");
        if (argc == 2 && std::string{argv[1]} == "--help") { usage(); return 0; }
        const bool all = argc >= 2 && std::string{argv[1]} == "--all";
        std::vector<std::string> endpoints;
        std::vector<std::string> positional;
        std::string client_id = new_client_id();
        std::uint64_t sequence = 1;
        std::chrono::milliseconds timeout{10000}, attempt_timeout{500};
        for (int i = all ? 2 : 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (all) { endpoints.push_back(arg); continue; }
            if (arg.rfind("--", 0) != 0) { positional.push_back(arg); continue; }
            if (++i == argc) throw std::invalid_argument("Missing option value");
            const std::string value = argv[i];
            if (arg == "--cluster") {
                std::istringstream input{value};
                std::string endpoint;
                while (std::getline(input, endpoint, ',')) {
                    if (endpoint.empty()) throw std::invalid_argument("Empty cluster endpoint");
                    endpoints.push_back(endpoint);
                }
                if (endpoints.empty()) throw std::invalid_argument("Empty cluster endpoints");
            } else if (arg == "--client-id") client_id = value;
            else if (arg == "--request-seq") sequence = positive(value);
            else if (arg == "--timeout-ms" || arg == "--attempt-timeout-ms") {
                const auto ms = positive(value);
                if (ms > 2147483647) throw std::invalid_argument("Client timeout is too large");
                (arg == "--timeout-ms" ? timeout : attempt_timeout) = std::chrono::milliseconds{ms};
            } else throw std::invalid_argument("Unknown option: " + arg);
        }
        std::string command = "SET greeting Hello, ZeroMQ";
        if (all) {
            if (endpoints.empty()) throw std::invalid_argument("--all requires endpoints");
        } else if (endpoints.empty()) {
            if (positional.size() > 2) throw std::invalid_argument("Too many arguments");
            endpoints.push_back(positional.empty() ? "tcp://127.0.0.1:5555" : positional[0]);
            if (positional.size() == 2) command = positional[1];
        } else {
            if (positional.size() > 1) throw std::invalid_argument("--cluster accepts one command");
            if (!positional.empty()) command = positional[0];
        }
        zmq::context_t context{1};
        const zmq_template::ClientRequest request{client_id, sequence, command};
        std::cerr << "client_id=" << client_id << " request_seq=" << sequence << '\n';
        if (!all) {
            const auto response = zmq_template::submit_request(context, endpoints, request, timeout, attempt_timeout);
            std::cout << response.payload << '\n';
            return response.type == zmq_template::MessageType::error || response.payload.rfind("ERROR ", 0) == 0 ? 1 : 0;
        }
        // Diagnostic mode: send to each endpoint once; it does not perform leader discovery.
        bool failed = false;
        for (const auto& endpoint : endpoints) {
            zmq::socket_t socket{context, zmq::socket_type::req};
            zmq_template::configure_socket(socket);
            socket.set(zmq::sockopt::linger, 0);
            socket.connect(endpoint);
            zmq_template::send_message(socket, {zmq_template::MessageType::client_request, 1,
                                                zmq_template::encode_client_request(request)});
            const auto response = zmq_template::receive_message(socket);
            if (response.id != 1 || (response.type != zmq_template::MessageType::acknowledgement
                && response.type != zmq_template::MessageType::error)) throw std::runtime_error("Invalid response");
            std::cout << '[' << endpoint << "] " << response.payload << '\n';
            failed |= response.type == zmq_template::MessageType::error;
        }
        return failed ? 1 : 0;
    } catch (const std::exception& error) {
        spdlog::error("Client error: {}", error.what());
        return 1;
    }
}
