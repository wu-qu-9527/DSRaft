#include "server/storage/log_storage.hpp"

#include "common/binary_codec.hpp"

#include <limits>
#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
constexpr std::string_view log_magic{"RAFTLG01"};

std::uint64_t take_log_u64(std::string_view& bytes) {
    if (bytes.size() < 8) throw StorageError("Truncated log record");
    const auto value = binary::read_u64(bytes, 0);
    bytes.remove_prefix(8);
    return value;
}

void validate_log_entry(const LogEntry& entry, std::uint64_t previous_index, std::uint64_t previous_term) {
    if (previous_index == std::numeric_limits<std::uint64_t>::max() || entry.index != previous_index + 1) {
        throw StorageError("Log indices must be contiguous and start at 1");
    }
    if (entry.term == 0 || entry.term < previous_term) {
        throw StorageError("Log terms must be positive and nondecreasing");
    }
}
} // namespace

LogStorage::LogStorage(std::filesystem::path data_dir)
    : LogStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

LogStorage::LogStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

std::vector<LogEntry> LogStorage::load() const {
    const auto bytes = directory_->read("log.bin");
    if (!bytes) return {};
    if (bytes->size() < 24) throw StorageError("Invalid persistent log size");
    std::string_view payload{bytes->data(), bytes->size() - 8};
    std::string_view trailer{bytes->data() + bytes->size() - 8, 8};
    if (payload.substr(0, 8) != log_magic) throw StorageError("Invalid persistent log format or version");
    if (binary::read_u64(trailer, 0) != binary::fnv1a_checksum(payload)) {
        throw StorageError("Persistent log checksum mismatch");
    }
    payload.remove_prefix(8);
    const auto count = take_log_u64(payload);
    if (count > payload.size() / 24) throw StorageError("Invalid persistent log entry count");
    std::vector<LogEntry> entries;
    std::uint64_t previous_term = 0;
    for (std::uint64_t i = 0; i < count; ++i) {
        LogEntry entry;
        entry.index = take_log_u64(payload);
        entry.term = take_log_u64(payload);
        const auto length = take_log_u64(payload);
        if (length > payload.size()) throw StorageError("Truncated persistent log command");
        validate_log_entry(entry, i, previous_term);
        entry.command.assign(payload.data(), static_cast<std::size_t>(length));
        payload.remove_prefix(static_cast<std::size_t>(length));
        previous_term = entry.term;
        entries.push_back(std::move(entry));
    }
    if (!payload.empty()) throw StorageError("Unexpected trailing persistent log data");
    return entries;
}

void LogStorage::save(const std::vector<LogEntry>& entries) {
    std::string bytes{log_magic};
    binary::append_u64(bytes, entries.size());
    for (const auto& entry : entries) {
        binary::append_u64(bytes, entry.index);
        binary::append_u64(bytes, entry.term);
        binary::append_u64(bytes, entry.command.size());
        bytes.append(entry.command);
    }
    binary::append_u64(bytes, binary::fnv1a_checksum(bytes));
    directory_->replace("log.bin", "log.tmp", bytes);
}

void LogStorage::append(const std::vector<LogEntry>& entries) {
    auto log = load();
    if (entries.empty()) return;
    auto previous_index = log.empty() ? 0 : log.back().index;
    auto previous_term = log.empty() ? 0 : log.back().term;
    for (const auto& entry : entries) {
        validate_log_entry(entry, previous_index, previous_term);
        previous_index = entry.index;
        previous_term = entry.term;
    }
    log.insert(log.end(), entries.begin(), entries.end());
    save(log);
}

void LogStorage::truncate_suffix(std::uint64_t first_index) {
    replace_suffix(first_index, {});
}

void LogStorage::replace_suffix(std::uint64_t first_index, const std::vector<LogEntry>& entries) {
    auto log = load();
    if (first_index == 0 || first_index - 1 > log.size()) throw StorageError("Invalid log truncation index");
    if (first_index - 1 == log.size() && entries.empty()) return;
    log.resize(static_cast<std::size_t>(first_index - 1));
    auto previous_index = log.empty() ? 0 : log.back().index;
    auto previous_term = log.empty() ? 0 : log.back().term;
    for (const auto& entry : entries) {
        validate_log_entry(entry, previous_index, previous_term);
        previous_index = entry.index;
        previous_term = entry.term;
    }
    log.insert(log.end(), entries.begin(), entries.end());
    save(log);
}

} // namespace zmq_template::server
