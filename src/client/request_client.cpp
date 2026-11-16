#include "client/request_client.hpp"

#include <algorithm>
#include <stdexcept>
#include <thread>

namespace zmq_template {
namespace {
bool retryable(const std::string& error) {
    return error.rfind("Not leader", 0) == 0 || error.rfind("Leadership changed", 0) == 0
        || error.rfind("Commit timed out", 0) == 0 || error.rfind("Too many pending", 0) == 0;
}
} // namespace

Message submit_request(zmq::context_t& context, const std::vector<std::string>& endpoints,
                       const ClientRequest& request, std::chrono::milliseconds timeout,
                       std::chrono::milliseconds attempt_timeout) {
    if (endpoints.empty() || timeout.count() <= 0 || attempt_timeout.count() <= 0) {
        throw std::invalid_argument("Endpoints and positive client timeouts are required");
    }
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + timeout;
    const auto payload = encode_client_request(request);
    std::size_t next = 0;
    std::uint32_t rpc_id = 0;
    while (Clock::now() < deadline) {
        zmq::socket_t socket{context, zmq::socket_type::req};
        configure_socket(socket);
        socket.set(zmq::sockopt::linger, 0);
        socket.set(zmq::sockopt::sndtimeo, 0);
        socket.set(zmq::sockopt::rcvtimeo, 0);
        socket.connect(endpoints[next++ % endpoints.size()]);
        const Message outgoing{MessageType::client_request, ++rpc_id, payload};
        try {
            send_message(socket, outgoing);
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
            zmq::pollitem_t item{socket.handle(), 0, ZMQ_POLLIN, 0};
            zmq::poll(&item, 1, std::max(std::chrono::milliseconds{0}, std::min(remaining, attempt_timeout)));
            if (item.revents & ZMQ_POLLIN) {
                auto response = receive_message(socket);
                if (response.id == outgoing.id) {
                    if (response.type == MessageType::acknowledgement) return response;
                    if (response.type == MessageType::error && !retryable(response.payload)) return response;
                }
            }
        } catch (const std::runtime_error&) {
            // Recreate the REQ socket before retrying; an unanswered REQ cannot send again.
        }
        std::this_thread::sleep_until(std::min(deadline, Clock::now() + std::chrono::milliseconds{10}));
    }
    throw std::runtime_error("Request deadline exceeded; outcome unknown. Retry with the same client ID, sequence and command.");
}

} // namespace zmq_template
