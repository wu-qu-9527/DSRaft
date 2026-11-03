#pragma once

#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace zmq_template::server {

class StorageError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Shared by state/log stores; owns the node directory and its exclusive state.lock.
class StorageDirectory {
public:
    explicit StorageDirectory(std::filesystem::path data_dir);
    ~StorageDirectory();
    StorageDirectory(const StorageDirectory&) = delete;
    StorageDirectory& operator=(const StorageDirectory&) = delete;

    // Missing files return nullopt; all other failures throw StorageError.
    std::optional<std::string> read(const char* filename) const;
    // Sync temporary contents, rename, then sync the directory before returning.
    void replace(const char* filename, const char* temporary, std::string_view bytes);

private:
    std::filesystem::path data_dir_;
    int directory_fd_{-1};
    int lock_fd_{-1};
};

} // namespace zmq_template::server
