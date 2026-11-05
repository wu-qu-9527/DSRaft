#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace zmq_template::binary {

void append_u64(std::string& bytes, std::uint64_t value);

// The caller must ensure that eight bytes are available at offset.
std::uint64_t read_u64(std::string_view bytes, std::size_t offset);

std::uint64_t fnv1a_checksum(std::string_view bytes);

} // namespace zmq_template::binary
