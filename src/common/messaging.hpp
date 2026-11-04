#pragma once

#include "common/message.hpp"

#include <zmq.hpp>

namespace zmq_template {

// Bound waits and linger allow queued output to flush without waiting forever.
void configure_socket(zmq::socket_t& socket);

// Transport one encoded Message in one application frame.
// Malformed multipart input is drained before std::invalid_argument is thrown.
void send_message(zmq::socket_t& socket, const Message& message);
Message receive_message(zmq::socket_t& socket);

// ROUTER envelopes let the server defer one client's reply while servicing other REQ peers.
// route is cleared before receiving and retained if the received request is malformed.
Message receive_routed_message(zmq::socket_t& socket, std::string& route);
void send_routed_message(zmq::socket_t& socket, const std::string& route, const Message& message);

} // namespace zmq_template
