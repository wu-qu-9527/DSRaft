#include "server/storage/persistent_state.hpp"

#include <array>
#include <cerrno>
#include <cstdio>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace zmq_template::server {
namespace {
constexpr char state_file[] = "state.bin";
constexpr char temporary_file[] = "state.tmp";
constexpr std::array<unsigned char, 8> magic{'R', 'A', 'F', 'T', 'S', 'T', '0', '1'};
using Record = std::array<unsigned char, 32>;

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
    if (result == -1) fail("Sync persistent state");
}

void write_u64(Record& record, std::size_t offset, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        record[offset + i] = static_cast<unsigned char>(value >> (56 - 8 * i));
    }
}

std::uint64_t read_u64(const Record& record, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) value = (value << 8) | record[offset + i];
    return value;
}

// FNV-1a detects accidental corruption; it is not an authentication mechanism.
std::uint64_t checksum(const Record& record) {
    std::uint64_t value = 14695981039346656037ULL;
    for (std::size_t i = 0; i < 24; ++i) {
        value ^= record[i];
        value *= 1099511628211ULL;
    }
    return value;
}
} // namespace

StateStorage::StateStorage(std::filesystem::path data_dir) : data_dir_(std::move(data_dir)) {
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

StateStorage::~StateStorage() {
    if (lock_fd_ >= 0) ::close(lock_fd_);
    if (directory_fd_ >= 0) ::close(directory_fd_);
}

PersistentState StateStorage::load() const {
    FileDescriptor fd{::openat(directory_fd_, state_file, O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
    if (fd.get() == -1) {
        if (errno == ENOENT) return {};
        fail("Open persistent state");
    }
    struct stat info{};
    if (::fstat(fd.get(), &info) == -1) fail("Read persistent state size");
    Record record{};
    if (!S_ISREG(info.st_mode) || info.st_size != static_cast<off_t>(record.size())) {
        throw StorageError("Invalid persistent state size or file type");
    }
    std::size_t offset = 0;
    while (offset < record.size()) {
        const auto count = ::read(fd.get(), record.data() + offset, record.size() - offset);
        if (count == -1) {
            if (errno == EINTR) continue;
            fail("Read persistent state");
        }
        if (count == 0) throw StorageError("Truncated persistent state");
        offset += static_cast<std::size_t>(count);
    }
    for (std::size_t i = 0; i < magic.size(); ++i) {
        if (record[i] != magic[i]) throw StorageError("Invalid persistent state format or version");
    }
    if (read_u64(record, 24) != checksum(record)) throw StorageError("Persistent state checksum mismatch");
    PersistentState state{read_u64(record, 8), std::nullopt};
    const auto vote = read_u64(record, 16);
    if (vote != 0) state.voted_for = vote;
    return state;
}

void StateStorage::save(const PersistentState& state) {
    if (state.voted_for && *state.voted_for == 0) throw StorageError("Persisted node ID must be positive");
    Record record{};
    for (std::size_t i = 0; i < magic.size(); ++i) record[i] = magic[i];
    write_u64(record, 8, state.current_term);
    write_u64(record, 16, state.voted_for.value_or(0));
    write_u64(record, 24, checksum(record));
    try {
        FileDescriptor fd{::openat(directory_fd_, temporary_file,
                                  O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600)};
        if (fd.get() == -1) fail("Create temporary persistent state");
        std::size_t offset = 0;
        while (offset < record.size()) {
            const auto count = ::write(fd.get(), record.data() + offset, record.size() - offset);
            if (count == -1) {
                if (errno == EINTR) continue;
                fail("Write persistent state");
            }
            if (count == 0) throw StorageError("Incomplete persistent state write");
            offset += static_cast<std::size_t>(count);
        }
        sync_file(fd.get());
        if (::close(fd.release()) == -1) fail("Close persistent state file");
        if (::renameat(directory_fd_, temporary_file, directory_fd_, state_file) == -1) {
            fail("Replace persistent state");
        }
        sync_file(directory_fd_);
    } catch (...) {
        ::unlinkat(directory_fd_, temporary_file, 0);
        throw;
    }
}

} // namespace zmq_template::server
