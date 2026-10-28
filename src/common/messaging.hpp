#pragma once

#include "common/message.hpp"
#include <zmq.hpp>

namespace zmq_template {

// Bound waits and linger allow queued output to flush without waiting forever.
void configure_socket(zmq::socket_t& socket);
void send_message(zmq::socket_t& socket, const Message& message);
Message receive_message(zmq::socket_t& socket);

} // namespace zmq_template
