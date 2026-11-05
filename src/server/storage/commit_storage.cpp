#include "server/storage/commit_storage.hpp"

#include "common/binary_codec.hpp"

#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
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
        || binary::read_u64(*bytes, 24) != binary::fnv1a_checksum(std::string_view{*bytes}.substr(0, 24))) {
        throw StorageError("Invalid committed log checkpoint");
    }
    const PersistentCommit commit{binary::read_u64(*bytes, 8), binary::read_u64(*bytes, 16)};
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
    binary::append_u64(bytes, commit.index);
    binary::append_u64(bytes, commit.term);
    binary::append_u64(bytes, binary::fnv1a_checksum(bytes));
    directory_->replace("commit.bin", "commit.tmp", bytes);
}

} // namespace zmq_template::server
