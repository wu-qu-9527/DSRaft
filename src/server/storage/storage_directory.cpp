#include "server/storage/storage_directory.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdint>
#include <limits>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace zmq_template::server {
namespace {
class FileDescriptor {
public:
    explicit FileDescriptor(int fd) : fd_(fd) {}
    ~FileDescriptor() { if (fd_ >= 0) ::close(fd_); }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    int get() const { return fd_; }
    int release() { const int fd = fd_; fd_ = -1; return fd; }
private:
    int fd_;
};

[[noreturn]] void fail(const std::string& operation) {
    const int error = errno;
    throw StorageError(operation + ": " + std::error_code(error, std::generic_category()).message());
}

void sync_file(int fd) {
    int result;
    do { result = ::fsync(fd); } while (result == -1 && errno == EINTR);
    if (result == -1) fail("Sync storage file");
}

} // namespace

StorageDirectory::StorageDirectory(std::filesystem::path data_dir) : data_dir_(std::move(data_dir)) {
    if (data_dir_.empty()) throw StorageError("Data directory must not be empty");
    std::error_code error;
    std::filesystem::create_directories(data_dir_, error);
    if (error) throw StorageError("Create data directory: " + error.message());
    try {
        directory_fd_ = ::open(data_dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd_ == -1) fail("Open data directory");
        lock_fd_ = ::openat(directory_fd_, "state.lock", O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (lock_fd_ == -1) fail("Open state directory lock");
        if (::flock(lock_fd_, LOCK_EX | LOCK_NB) == -1) fail("Lock data directory (already in use or unavailable)");
        // Persist newly created directory entries, including any new ancestors.
        auto path = std::filesystem::absolute(data_dir_).lexically_normal();
        for (;;) {
            FileDescriptor fd{::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
            if (fd.get() == -1) fail("Open data directory ancestor");
            sync_file(fd.get());
            const auto parent = path.parent_path();
            if (parent == path) break;
            path = parent;
        }
    } catch (...) {
        if (lock_fd_ >= 0) ::close(lock_fd_);
        if (directory_fd_ >= 0) ::close(directory_fd_);
        throw;
    }
}

StorageDirectory::~StorageDirectory() {
    if (lock_fd_ >= 0) ::close(lock_fd_);
    if (directory_fd_ >= 0) ::close(directory_fd_);
}

std::optional<std::string> StorageDirectory::read(const char* filename) const {
    FileDescriptor fd{::openat(directory_fd_, filename, O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
    if (fd.get() == -1) {
        if (errno == ENOENT) return std::nullopt;
        fail(std::string{"Open storage file "} + filename);
    }
    struct stat info{};
    if (::fstat(fd.get(), &info) == -1) fail("Read storage file size");
    if (!S_ISREG(info.st_mode) || info.st_size < 0
        || static_cast<std::uintmax_t>(info.st_size) > std::string{}.max_size()) {
        throw StorageError("Invalid storage file size or type");
    }
    std::string bytes(static_cast<std::size_t>(info.st_size), '\0');
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = ::read(fd.get(), bytes.data() + offset,
            std::min(bytes.size() - offset, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())));
        if (count == -1) {
            if (errno == EINTR) continue;
            fail("Read storage file");
        }
        if (count == 0) throw StorageError("Truncated storage file");
        offset += static_cast<std::size_t>(count);
    }
    return bytes;
}

void StorageDirectory::replace(const char* filename, const char* temporary, std::string_view bytes) {
    try {
        FileDescriptor fd{::openat(directory_fd_, temporary,
                                  O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600)};
        if (fd.get() == -1) fail("Create temporary storage file");
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto count = ::write(fd.get(), bytes.data() + offset,
                std::min(bytes.size() - offset, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())));
            if (count == -1) {
                if (errno == EINTR) continue;
                fail("Write storage file");
            }
            if (count == 0) throw StorageError("Incomplete storage file write");
            offset += static_cast<std::size_t>(count);
        }
        sync_file(fd.get());
        if (::close(fd.release()) == -1) fail("Close storage file");
        if (::renameat(directory_fd_, temporary, directory_fd_, filename) == -1) fail("Replace storage file");
        sync_file(directory_fd_);
    } catch (...) {
        ::unlinkat(directory_fd_, temporary, 0);
        throw;
    }
}

} // namespace zmq_template::server
