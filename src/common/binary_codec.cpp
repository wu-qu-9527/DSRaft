#include "common/binary_codec.hpp"

namespace zmq_template::binary {

void append_u64(std::string& bytes, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::uint64_t read_u64(std::string_view bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        value = (value << 8) | static_cast<unsigned char>(bytes[offset + i]);
    }
    return value;
}

std::uint64_t fnv1a_checksum(std::string_view bytes) {
    std::uint64_t value = 14695981039346656037ULL;
    for (unsigned char byte : bytes) {
        value ^= byte;
        value *= 1099511628211ULL;
    }
    return value;
}

} // namespace zmq_template::binary
