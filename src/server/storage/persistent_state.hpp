#pragma once

#include "server/storage/storage_directory.hpp"

#include <cstdint>
#include <memory>
#include <optional>

namespace zmq_template::server {

struct PersistentState {
    std::uint64_t current_term{0};
    std::optional<std::uint64_t> voted_for;
};

// Persists only current term and vote, using a shared locked directory.
class StateStorage {
public:
    explicit StateStorage(std::filesystem::path data_dir);
    explicit StateStorage(std::shared_ptr<StorageDirectory> directory);
    StateStorage(const StateStorage&) = delete;
    StateStorage& operator=(const StateStorage&) = delete;

    // A missing state file means a new node; malformed existing state is fatal.
    PersistentState load() const;
    void save(const PersistentState& state);

private:
    std::shared_ptr<StorageDirectory> directory_;
};

} // namespace zmq_template::server
