#pragma once

#include <cstdint>
#include <string>

namespace zmq_template::server {

struct LogEntry {
    // Real entries start at index 1; index 0 and term 0 denote the empty-log boundary.
    std::uint64_t index{0};
    std::uint64_t term{0};
    // Opaque state-machine command bytes; embedded NUL bytes are allowed.
    std::string command;
};

} // namespace zmq_template::server
