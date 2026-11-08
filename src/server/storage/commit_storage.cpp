#include "server/storage/commit_storage.hpp"

#include "server/storage/json_record.hpp"

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
    const auto data = read_json_record(*directory_, "commit.json", "raft-commit");
    if (!data) return {};
    if (data->size() != 2) throw StorageError("Invalid committed log checkpoint fields");
    const PersistentCommit commit{json_u64(*data, "index"), json_u64(*data, "term")};
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
    write_json_record(*directory_, "commit.json", "commit.tmp", "raft-commit",
        {{"index", commit.index}, {"term", commit.term}});
}

} // namespace zmq_template::server
