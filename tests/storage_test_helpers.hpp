#pragma once

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <cstdint>

namespace zmq_template::server::test {

inline nlohmann::json read_json(const std::filesystem::path& path) {
    std::ifstream input{path};
    return nlohmann::json::parse(input);
}

// Re-sign modified fixtures to exercise schema validation independently of checksums.
inline void write_json(const std::filesystem::path& path, nlohmann::json record) {
    record.erase("checksum");
    std::uint64_t checksum = 14695981039346656037ULL;
    for (unsigned char byte : record.dump()) {
        checksum = (checksum ^ byte) * 1099511628211ULL;
    }
    record["checksum"] = checksum;
    std::ofstream{path} << record.dump(2) << '\n';
}

} // namespace zmq_template::server::test
