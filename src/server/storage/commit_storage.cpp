#include "server/storage/commit_storage.hpp"

#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
void append_u64(std::string& bytes, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<char>(value >> shift));
}

std::uint64_t read_u64(std::string_view bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(bytes[offset + i]);
    return value;
}

std::uint64_t checksum(std::string_view bytes) {
    std::uint64_t value = 14695981039346656037ULL;
    for (unsigned char byte : bytes) {
        value ^= byte;
        value *= 1099511628211ULL;
    }
    return value;
}

void validate(const PersistentCommit& commit) {
    if ((commit.index == 0) != (commit.term == 0)) throw StorageError("Invalid committed log boundary");
}
} // namespace

CommitStorage::CommitStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

PersistentCommit CommitStorage::load() const {
    const auto bytes = directory_->read("commit.bin");
    if (!bytes) return {};
    if (bytes->size() != 32 || bytes->substr(0, 8) != "RAFTCT01"
        || read_u64(*bytes, 24) != checksum(std::string_view{*bytes}.substr(0, 24))) {
        throw StorageError("Invalid committed log checkpoint");
    }
    const PersistentCommit commit{read_u64(*bytes, 8), read_u64(*bytes, 16)};
    validate(commit);
    return commit;
}

void CommitStorage::save(const PersistentCommit& commit) {
    validate(commit);
    const auto previous = load();
    if (commit.index < previous.index || commit.term < previous.term
        || (commit.index == previous.index && commit.term != previous.term)) {
        throw StorageError("Committed log checkpoint must not move backwards or change identity");
    }
    std::string bytes{"RAFTCT01"};
    append_u64(bytes, commit.index);
    append_u64(bytes, commit.term);
    append_u64(bytes, checksum(bytes));
    directory_->replace("commit.bin", "commit.tmp", bytes);
}

} // namespace zmq_template::server
