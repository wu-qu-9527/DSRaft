#pragma once

#include "server/storage/storage_directory.hpp"

#include <cstdint>
#include <memory>

namespace zmq_template::server {

struct PersistentCommit {
    std::uint64_t index{0};
    std::uint64_t term{0}; // Binds the checkpoint to the corresponding durable log entry.
};

class CommitStorage {
public:
    explicit CommitStorage(std::shared_ptr<StorageDirectory> directory);
    PersistentCommit load() const;
    // Checkpoints cannot move backwards or change the term at the same index.
    void save(const PersistentCommit& commit);

private:
    std::shared_ptr<StorageDirectory> directory_;
};

} // namespace zmq_template::server
