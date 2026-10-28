#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace zmq_template {

enum class MessageType : std::uint32_t {
    text = 1,
    acknowledgement = 2,
    request_vote = 3,
    request_vote_response = 4,
    error = 5,
    heartbeat = 6,
    heartbeat_response = 7,
};

// Shared application message. Never transmit the raw struct (it contains a string).
struct Message {
    MessageType type{MessageType::text};
    std::uint32_t id{0};
    std::string payload;
};

inline constexpr std::size_t message_header_size = 16;
inline constexpr std::size_t max_payload_size = 1024 * 1024;

// Single frame: "ZMQ1" + type/u32 + id/u32 + payload length/u32 + payload.
// All integer fields use network byte order; payload may contain zero bytes.
std::string serialize(const Message& message);
Message deserialize(std::string_view bytes);

} // namespace zmq_template
