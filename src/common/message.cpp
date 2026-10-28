#include "common/message.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
void append_u32(std::string& bytes, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::uint32_t read_u32(std::string_view bytes, std::size_t offset) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<unsigned char>(bytes[offset + i]);
    }
    return value;
}

bool valid_type(MessageType type) {
    return type == MessageType::text || type == MessageType::acknowledgement
        || type == MessageType::request_vote || type == MessageType::request_vote_response
        || type == MessageType::error || type == MessageType::heartbeat
        || type == MessageType::heartbeat_response;
}
} // namespace

std::string serialize(const Message& message) {
    if (!valid_type(message.type)) throw std::invalid_argument("Unknown message type");
    if (message.payload.size() > max_payload_size) {
        throw std::invalid_argument("Payload exceeds 1 MiB");
    }
    std::string bytes{"ZMQ1"};
    bytes.reserve(message_header_size + message.payload.size());
    append_u32(bytes, static_cast<std::uint32_t>(message.type));
    append_u32(bytes, message.id);
    append_u32(bytes, static_cast<std::uint32_t>(message.payload.size()));
    bytes.append(message.payload);
    return bytes;
}

Message deserialize(std::string_view bytes) {
    if (bytes.size() < message_header_size || bytes.substr(0, 4) != "ZMQ1") {
        throw std::invalid_argument("Invalid message header");
    }
    const auto type = static_cast<MessageType>(read_u32(bytes, 4));
    const auto size = read_u32(bytes, 12);
    if (!valid_type(type)) throw std::invalid_argument("Unknown message type");
    if (size > max_payload_size || bytes.size() - message_header_size != size) {
        throw std::invalid_argument("Invalid payload length");
    }
    return {type, read_u32(bytes, 8), std::string(bytes.substr(message_header_size))};
}
} // namespace zmq_template
