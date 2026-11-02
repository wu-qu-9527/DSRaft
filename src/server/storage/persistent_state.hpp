#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>

namespace zmq_template::server {

struct PersistentState {
    std::uint64_t current_term{0};
    std::optional<std::uint64_t> voted_for;
};

class StorageError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Owns an exclusively locked data directory. Linux/POSIX durable file storage.
class StateStorage {
public:
    explicit StateStorage(std::filesystem::path data_dir);
    ~StateStorage();

    StateStorage(const StateStorage&) = delete;
    StateStorage& operator=(const StateStorage&) = delete;

    // A missing state file means a new node; malformed existing state is fatal.
    PersistentState load() const;
    // Returns only after the replacement file and directory have been synced.
    void save(const PersistentState& state);

private:
    std::filesystem::path data_dir_;
    int directory_fd_{-1};
    int lock_fd_{-1};
};

} // namespace zmq_template::server
