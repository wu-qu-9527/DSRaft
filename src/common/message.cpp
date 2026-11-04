#include "common/message.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
struct MessageHeader {
    MessageType type;
    std::uint32_t id;
    std::uint32_t payload_size;
};

void validate_type(MessageType type) {
    switch (type) {
    case MessageType::text:
    case MessageType::acknowledgement:
    case MessageType::request_vote:
    case MessageType::request_vote_response:
    case MessageType::error:
    case MessageType::heartbeat:
    case MessageType::heartbeat_response:
        return;
    }
    throw std::invalid_argument("Unknown message type");
}

void append_u32(std::string& bytes, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

// read_header checks that the complete header exists before consuming its fields.
std::uint32_t consume_u32(std::string_view& bytes) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        value = (value << 8) | static_cast<unsigned char>(bytes[i]);
    }
    bytes.remove_prefix(sizeof(value));
    return value;
}

void append_header(std::string& bytes, const MessageHeader& header) {
    bytes.append(message_magic);
    append_u32(bytes, static_cast<std::uint32_t>(header.type));
    append_u32(bytes, header.id);
    append_u32(bytes, header.payload_size);
}

MessageHeader read_header(std::string_view& bytes) {
    if (bytes.size() < message_header_size || bytes.substr(0, message_magic.size()) != message_magic) {
        throw std::invalid_argument("Invalid message header");
    }
    bytes.remove_prefix(message_magic.size());
    const auto type = static_cast<MessageType>(consume_u32(bytes));
    const auto id = consume_u32(bytes);
    const auto payload_size = consume_u32(bytes);
    validate_type(type);
    return {type, id, payload_size};
}
} // namespace

std::string serialize(const Message& message) {
    validate_type(message.type);
    if (message.payload.size() > max_payload_size) {
        throw std::invalid_argument("Payload exceeds 1 MiB");
    }
    std::string bytes;
    bytes.reserve(message_header_size + message.payload.size());
    append_header(bytes, {message.type, message.id, static_cast<std::uint32_t>(message.payload.size())});
    bytes.append(message.payload);
    return bytes;
}

Message deserialize(std::string_view bytes) {
    const auto header = read_header(bytes);
    if (header.payload_size > max_payload_size || bytes.size() != header.payload_size) {
        throw std::invalid_argument("Invalid payload length");
    }
    return {header.type, header.id, std::string(bytes)};
}
} // namespace zmq_template
