#pragma once

#include "server/storage/storage_directory.hpp"

#include <nlohmann/json.hpp>

#include <cstdint>
#include <optional>
#include <string_view>

namespace zmq_template::server {

// Versioned JSON records share a checksum over the compact, sorted-key document
// with its checksum field removed. Formatting whitespace is not significant.
std::optional<nlohmann::json> read_json_record(
    const StorageDirectory& directory, const char* filename, std::string_view format);
void write_json_record(StorageDirectory& directory, const char* filename,
    const char* temporary, std::string_view format, nlohmann::json data);

// Reject floating-point values, negative numbers, booleans and numeric strings.
std::uint64_t json_u64(const nlohmann::json& object, const char* field);

} // namespace zmq_template::server
