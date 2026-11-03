#include "server/storage/persistent_state.hpp"

#include <algorithm>
#include <array>
#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
constexpr std::array<unsigned char, 8> magic{'R', 'A', 'F', 'T', 'S', 'T', '0', '1'};
using Record = std::array<unsigned char, 32>;

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

StateStorage::StateStorage(std::filesystem::path data_dir)
    : StateStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

StateStorage::StateStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

PersistentState StateStorage::load() const {
    const auto bytes = directory_->read("state.bin");
    if (!bytes) return {};
    Record record{};
    if (bytes->size() != record.size()) throw StorageError("Invalid persistent state size");
    std::copy(bytes->begin(), bytes->end(), record.begin());
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
    directory_->replace("state.bin", "state.tmp",
        std::string_view{reinterpret_cast<const char*>(record.data()), record.size()});
}

} // namespace zmq_template::server
