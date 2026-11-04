#include "common/messaging.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
zmq::message_t receive_frame(zmq::socket_t& socket) {
    zmq::message_t frame;
    if (!socket.recv(frame, zmq::recv_flags::none)) throw std::runtime_error("Receive timed out");
    return frame;
}

void send_frame(zmq::socket_t& socket, zmq::const_buffer bytes, zmq::send_flags flags = zmq::send_flags::none) {
    if (!socket.send(bytes, flags)) throw std::runtime_error("Send timed out");
}

std::string_view frame_bytes(const zmq::message_t& frame) {
    return {static_cast<const char*>(frame.data()), frame.size()};
}

void drain_remaining_frames(zmq::socket_t& socket, zmq::message_t& frame) {
    while (frame.more()) frame = receive_frame(socket);
}

Message receive_application_message(zmq::socket_t& socket) {
    auto frame = receive_frame(socket);
    if (frame.more()) {
        // Consume the whole invalid message so the next call starts at a message boundary.
        drain_remaining_frames(socket, frame);
        throw std::invalid_argument("Expected a single application frame");
    }
    return deserialize(frame_bytes(frame));
}

void receive_req_envelope(zmq::socket_t& socket, std::string& route) {
    route.clear();
    auto frame = receive_frame(socket);
    route.assign(frame_bytes(frame));
    if (!frame.more()) throw std::invalid_argument("Missing REQ envelope");
    frame = receive_frame(socket);
    if (frame.size() != 0 || !frame.more()) {
        drain_remaining_frames(socket, frame);
        throw std::invalid_argument("Invalid REQ delimiter");
    }
}
} // namespace

void configure_socket(zmq::socket_t& socket) {
    socket.set(zmq::sockopt::linger, 1000);
    socket.set(zmq::sockopt::sndtimeo, 5000);
    socket.set(zmq::sockopt::rcvtimeo, 5000);
    socket.set(zmq::sockopt::maxmsgsize,
               static_cast<std::int64_t>(message_header_size + max_payload_size));
}

void send_message(zmq::socket_t& socket, const Message& message) {
    const auto bytes = serialize(message);
    send_frame(socket, zmq::buffer(bytes));
}

Message receive_message(zmq::socket_t& socket) {
    return receive_application_message(socket);
}

Message receive_routed_message(zmq::socket_t& socket, std::string& route) {
    receive_req_envelope(socket, route);
    return receive_application_message(socket);
}

void send_routed_message(zmq::socket_t& socket, const std::string& route, const Message& message) {
    // Validate/encode before sending any part of the multipart reply.
    const auto bytes = serialize(message);
    send_frame(socket, zmq::buffer(route), zmq::send_flags::sndmore);
    send_frame(socket, zmq::const_buffer{nullptr, 0}, zmq::send_flags::sndmore);
    send_frame(socket, zmq::buffer(bytes));
}
} // namespace zmq_template
