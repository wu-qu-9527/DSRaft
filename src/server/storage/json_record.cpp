#include "server/storage/json_record.hpp"

#include "common/binary_codec.hpp"

#include <set>
#include <string>
#include <utility>
#include <vector>

namespace zmq_template::server {

std::uint64_t json_u64(const nlohmann::json& object, const char* field) {
    const auto found = object.find(field);
    if (found == object.end() || !found->is_number_unsigned()) {
        throw StorageError(std::string{"Expected uint64 JSON field: "} + field);
    }
    return found->get<std::uint64_t>();
}

std::optional<nlohmann::json> read_json_record(
    const StorageDirectory& directory, const char* filename, std::string_view format) {
    const auto bytes = directory.read(filename);
    if (!bytes) return std::nullopt;
    try {
        std::vector<std::set<std::string>> object_keys;
        const auto check_keys = [&](int depth, nlohmann::json::parse_event_t event, nlohmann::json& value) {
            if (depth > 64) throw StorageError("JSON storage nesting exceeds 64 levels");
            if (event == nlohmann::json::parse_event_t::object_start) {
                object_keys.emplace_back();
            } else if (event == nlohmann::json::parse_event_t::object_end) {
                object_keys.pop_back();
            } else if (event == nlohmann::json::parse_event_t::key) {
                if (!object_keys.back().insert(value.get<std::string>()).second) {
                    throw StorageError("Duplicate JSON storage field");
                }
            }
            return true;
        };
        auto record = nlohmann::json::parse(*bytes, check_keys);
        if (!record.is_object() || record.size() != 4 || !record.contains("format")
            || record["format"] != format || json_u64(record, "version") != 1
            || !record.contains("data") || !record["data"].is_object()) {
            throw StorageError("Invalid JSON storage format or version");
        }
        const auto checksum = json_u64(record, "checksum");
        record.erase("checksum");
        if (checksum != binary::fnv1a_checksum(record.dump())) {
            throw StorageError("JSON storage checksum mismatch");
        }
        return std::move(record["data"]);
    } catch (const nlohmann::json::exception& error) {
        throw StorageError(std::string{"Invalid JSON storage record: "} + error.what());
    }
}

void write_json_record(StorageDirectory& directory, const char* filename,
    const char* temporary, std::string_view format, nlohmann::json data) {
    nlohmann::json record{{"format", format}, {"version", 1u}, {"data", std::move(data)}};
    record["checksum"] = binary::fnv1a_checksum(record.dump());
    directory.replace(filename, temporary, record.dump(2) + '\n');
}

} // namespace zmq_template::server
