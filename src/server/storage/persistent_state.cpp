#include "server/storage/persistent_state.hpp"

#include "common/binary_codec.hpp"

#include <string_view>
#include <utility>

namespace zmq_template::server {
namespace {
constexpr std::string_view magic{"RAFTST01"};
constexpr std::size_t record_size = 32;
} // namespace

StateStorage::StateStorage(std::filesystem::path data_dir)
    : StateStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

StateStorage::StateStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

PersistentState StateStorage::load() const {
    const auto bytes = directory_->read("state.bin");
    if (!bytes) return {};
    const std::string_view record{*bytes};
    if (record.size() != record_size) throw StorageError("Invalid persistent state size");
    if (record.substr(0, magic.size()) != magic) throw StorageError("Invalid persistent state format or version");
    if (binary::read_u64(record, 24) != binary::fnv1a_checksum(record.substr(0, 24))) {
        throw StorageError("Persistent state checksum mismatch");
    }
    PersistentState state{binary::read_u64(record, 8), std::nullopt};
    const auto vote = binary::read_u64(record, 16);
    if (vote != 0) state.voted_for = vote;
    return state;
}

void StateStorage::save(const PersistentState& state) {
    if (state.voted_for && *state.voted_for == 0) throw StorageError("Persisted node ID must be positive");
    std::string record;
    record.reserve(record_size);
    record.append(magic);
    binary::append_u64(record, state.current_term);
    binary::append_u64(record, state.voted_for.value_or(0));
    binary::append_u64(record, binary::fnv1a_checksum(record));
    directory_->replace("state.bin", "state.tmp", record);
}

} // namespace zmq_template::server
