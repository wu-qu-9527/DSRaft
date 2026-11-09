#pragma once

#include "server/state.hpp"
#include "server/storage/storage_directory.hpp"

#include <memory>

namespace zmq_template::server {

// Persists term/vote and commit checkpoints as separate records in one locked directory.
// Each save is durable independently; the caller coordinates ordering with log storage.
class RaftStateStorage {
public:
    explicit RaftStateStorage(std::filesystem::path data_dir);
    explicit RaftStateStorage(std::shared_ptr<StorageDirectory> directory);
    RaftStateStorage(const RaftStateStorage&) = delete;
    RaftStateStorage& operator=(const RaftStateStorage&) = delete;

    // A missing state file means a new node; malformed existing state is fatal.
    TermVoteState load_term_vote() const;
    void save_term_vote(const TermVoteState& state);

    // A missing checkpoint means no committed entries; corruption is fatal.
    PersistentCommit load_commit() const;
    // Checkpoints cannot move backwards or change the term at the same index.
    void save_commit(const PersistentCommit& commit);

private:
    std::shared_ptr<StorageDirectory> directory_;
};

} // namespace zmq_template::server
