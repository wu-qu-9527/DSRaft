#include "server/storage/log_storage.hpp"

#include "server/storage/json_record.hpp"

#include <limits>
#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
void validate_log_entry(const LogEntry& entry, std::uint64_t previous_index, std::uint64_t previous_term) {
    if (previous_index == std::numeric_limits<std::uint64_t>::max() || entry.index != previous_index + 1) {
        throw StorageError("Log indices must be contiguous and start at 1");
    }
    if (entry.term == 0 || entry.term < previous_term) {
        throw StorageError("Log terms must be positive and nondecreasing");
    }
}

std::string encode_hex(std::string_view bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(bytes.size() * 2);
    for (unsigned char byte : bytes) {
        hex.push_back(digits[byte >> 4]);
        hex.push_back(digits[byte & 0xf]);
    }
    return hex;
}

unsigned char hex_digit(char digit) {
    if (digit >= '0' && digit <= '9') return static_cast<unsigned char>(digit - '0');
    if (digit >= 'a' && digit <= 'f') return static_cast<unsigned char>(digit - 'a' + 10);
    if (digit >= 'A' && digit <= 'F') return static_cast<unsigned char>(digit - 'A' + 10);
    throw StorageError("Invalid hexadecimal log command");
}

std::string decode_hex(std::string_view hex) {
    if (hex.size() % 2 != 0) throw StorageError("Invalid hexadecimal log command length");
    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<char>((hex_digit(hex[i]) << 4) | hex_digit(hex[i + 1])));
    }
    return bytes;
}

} // namespace

LogStorage::LogStorage(std::filesystem::path data_dir)
    : LogStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

LogStorage::LogStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

std::vector<LogEntry> LogStorage::load() const {
    const auto data = read_json_record(*directory_, "log.json", "raft-log");
    if (!data) return {};
    if ((data->size() != 1 && !(data->size() == 2 && data->contains("snapshot")))
        || !data->contains("entries") || !data->at("entries").is_array()) {
        throw StorageError("Invalid persistent log fields");
    }
    std::vector<LogEntry> entries;
    const auto snapshot = load_snapshot();
    std::uint64_t previous_term = snapshot.term;
    std::uint64_t previous_index = snapshot.index;
    for (const auto& item : data->at("entries")) {
        if (!item.is_object() || item.size() != 3
            || item.contains("command") == item.contains("command_hex")) {
            throw StorageError("Invalid persistent log entry fields");
        }
        LogEntry entry{json_u64(item, "index"), json_u64(item, "term"), {}};
        const auto& command = item.at(item.contains("command") ? "command" : "command_hex");
        if (!command.is_string()) throw StorageError("Invalid persistent log command");
        entry.command = command.get<std::string>();
        if (item.contains("command_hex")) entry.command = decode_hex(entry.command);
        validate_log_entry(entry, previous_index, previous_term);
        previous_index = entry.index;
        previous_term = entry.term;
        entries.push_back(std::move(entry));
    }
    return entries;
}

Snapshot LogStorage::load_snapshot() const {
    const auto data = read_json_record(*directory_, "log.json", "raft-log");
    if (!data || !data->contains("snapshot")) return {};
    const auto& value = data->at("snapshot");
    if (!value.is_object() || value.size() != 4 || !value.contains("state_hex") || !value["state_hex"].is_string()
        || !value.contains("configuration") || !value["configuration"].is_string()) throw StorageError("Invalid snapshot");
    Snapshot snapshot{json_u64(value, "index"), json_u64(value, "term"),
        decode_hex(value["state_hex"].get<std::string>()), value["configuration"].get<std::string>()};
    if (snapshot.index == 0 || snapshot.term == 0) throw StorageError("Invalid snapshot boundary");
    return snapshot;
}

void LogStorage::save(const std::vector<LogEntry>& entries, const Snapshot& snapshot) {
    auto items = nlohmann::json::array();
    for (const auto& entry : entries) {
        nlohmann::json item{{"index", entry.index}, {"term", entry.term}};
        // JSON strings require UTF-8. Preserve arbitrary command bytes with hex.
        try {
            (void)nlohmann::json(entry.command).dump();
            item["command"] = entry.command;
        } catch (const nlohmann::json::type_error&) {
            item["command_hex"] = encode_hex(entry.command);
        }
        items.push_back(std::move(item));
    }
    nlohmann::json data{{"entries", std::move(items)}};
    if (snapshot.index != 0) data["snapshot"] = {{"index", snapshot.index}, {"term", snapshot.term},
        {"state_hex", encode_hex(snapshot.state)}, {"configuration", snapshot.configuration}};
    write_json_record(*directory_, "log.json", "log.tmp", "raft-log", std::move(data));
}

void LogStorage::install_snapshot(const Snapshot& snapshot) {
    const auto old = load_snapshot();
    if (snapshot.index == 0 || snapshot.term == 0 || snapshot.index < old.index || snapshot.term < old.term
        || (snapshot.index == old.index && (snapshot.term != old.term || snapshot.state != old.state
            || snapshot.configuration != old.configuration))) throw StorageError("Snapshot must not move backwards or change identity");
    const auto log = load();
    std::vector<LogEntry> suffix;
    if (snapshot.index == old.index && snapshot.term == old.term) suffix = log;
    else if (snapshot.index > old.index && snapshot.index - old.index <= log.size()
             && log[snapshot.index - old.index - 1].term == snapshot.term) {
        suffix.assign(log.begin() + static_cast<std::size_t>(snapshot.index - old.index), log.end());
    }
    save(suffix, snapshot);
}

void LogStorage::append(const std::vector<LogEntry>& entries) {
    auto log = load();
    if (entries.empty()) return;
    const auto snapshot = load_snapshot();
    auto previous_index = log.empty() ? snapshot.index : log.back().index;
    auto previous_term = log.empty() ? snapshot.term : log.back().term;
    for (const auto& entry : entries) {
        validate_log_entry(entry, previous_index, previous_term);
        previous_index = entry.index;
        previous_term = entry.term;
    }
    log.insert(log.end(), entries.begin(), entries.end());
    save(log, snapshot);
}

void LogStorage::truncate_suffix(std::uint64_t first_index) {
    replace_suffix(first_index, {});
}

void LogStorage::replace_suffix(std::uint64_t first_index, const std::vector<LogEntry>& entries) {
    auto log = load();
    const auto snapshot = load_snapshot();
    if (first_index <= snapshot.index || first_index - snapshot.index - 1 > log.size()) throw StorageError("Invalid log truncation index");
    if (first_index - snapshot.index - 1 == log.size() && entries.empty()) return;
    log.resize(static_cast<std::size_t>(first_index - snapshot.index - 1));
    auto previous_index = log.empty() ? snapshot.index : log.back().index;
    auto previous_term = log.empty() ? snapshot.term : log.back().term;
    for (const auto& entry : entries) {
        validate_log_entry(entry, previous_index, previous_term);
        previous_index = entry.index;
        previous_term = entry.term;
    }
    log.insert(log.end(), entries.begin(), entries.end());
    save(log, snapshot);
}

} // namespace zmq_template::server
