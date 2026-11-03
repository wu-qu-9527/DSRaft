#pragma once

#include "server/log_entry.hpp"
#include "server/storage/storage_directory.hpp"

#include <memory>
#include <vector>

namespace zmq_template::server {

class LogStorage {
public:
    explicit LogStorage(std::filesystem::path data_dir);
    explicit LogStorage(std::shared_ptr<StorageDirectory> directory);
    LogStorage(const LogStorage&) = delete;
    LogStorage& operator=(const LogStorage&) = delete;

    // Missing log means empty; malformed logs are fatal. Indices start at 1.
    std::vector<LogEntry> load() const;
    // Append contiguous entries with positive, nondecreasing terms, then sync.
    void append(const std::vector<LogEntry>& entries);
    // Remove first_index and all later entries; last_index + 1 is a no-op.
    // The caller must ensure that no committed entries are removed.
    void truncate_suffix(std::uint64_t first_index);

private:
    void save(const std::vector<LogEntry>& entries);
    std::shared_ptr<StorageDirectory> directory_;
};

} // namespace zmq_template::server
