#include "common/messaging.hpp"

#include <stdexcept>

namespace zmq_template {
void send_message(zmq::socket_t& socket, const Message& message) {
    const auto bytes = serialize(message);
    if (!socket.send(zmq::buffer(bytes), zmq::send_flags::none)) {
        throw std::runtime_error("Send timed out");
    }
}

Message receive_message(zmq::socket_t& socket) {
    zmq::message_t message;
    if (!socket.recv(message, zmq::recv_flags::none)) {
        throw std::runtime_error("Receive timed out");
    }
    if (message.more()) {
        // Drain the rest of the invalid multipart message before reporting it.
        do {
            if (!socket.recv(message, zmq::recv_flags::none)) {
                throw std::runtime_error("Receive timed out");
            }
        } while (message.more());
        throw std::invalid_argument("Expected a single-frame message");
    }
    return deserialize(std::string_view(static_cast<const char*>(message.data()), message.size()));
}

Message receive_routed_message(zmq::socket_t& socket, std::string& route) {
    zmq::message_t frame;
    if (!socket.recv(frame)) throw std::runtime_error("Receive route timed out");
    route.assign(static_cast<const char*>(frame.data()), frame.size());
    const auto drain = [&] {
        while (frame.more()) {
            if (!socket.recv(frame)) throw std::runtime_error("Receive envelope timed out");
        }
    };
    if (!frame.more()) throw std::invalid_argument("Missing REQ envelope");
    if (!socket.recv(frame)) throw std::runtime_error("Receive delimiter timed out");
    if (frame.size() != 0 || !frame.more()) {
        drain();
        throw std::invalid_argument("Invalid REQ delimiter");
    }
    if (!socket.recv(frame)) throw std::runtime_error("Receive request timed out");
    if (frame.more()) {
        drain();
        throw std::invalid_argument("Expected a single application frame");
    }
    return deserialize(std::string_view{static_cast<const char*>(frame.data()), frame.size()});
}

void send_routed_message(zmq::socket_t& socket, const std::string& route, const Message& message) {
    const auto bytes = serialize(message);
    if (!socket.send(zmq::buffer(route), zmq::send_flags::sndmore)
        || !socket.send(zmq::const_buffer{nullptr, 0}, zmq::send_flags::sndmore)
        || !socket.send(zmq::buffer(bytes), zmq::send_flags::none)) {
        throw std::runtime_error("Send routed reply timed out");
    }
}

void configure_socket(zmq::socket_t& socket) {
    socket.set(zmq::sockopt::linger, 1000);
    socket.set(zmq::sockopt::sndtimeo, 5000);
    socket.set(zmq::sockopt::rcvtimeo, 5000);
    socket.set(zmq::sockopt::maxmsgsize,
               static_cast<std::int64_t>(message_header_size + max_payload_size));
}
} // namespace zmq_template
