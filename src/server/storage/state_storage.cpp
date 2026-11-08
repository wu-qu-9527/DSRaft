#include "server/storage/state_storage.hpp"

#include "server/storage/json_record.hpp"

#include <utility>

namespace zmq_template::server {

StateStorage::StateStorage(std::filesystem::path data_dir)
    : StateStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

StateStorage::StateStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

PersistentState StateStorage::load() const {
    const auto data = read_json_record(*directory_, "state.json", "raft-state");
    if (!data) return {};
    if (data->size() != 2 || !data->contains("voted_for")) {
        throw StorageError("Invalid persistent state fields");
    }
    PersistentState state{json_u64(*data, "current_term"), std::nullopt};
    if (!data->at("voted_for").is_null()) {
        const auto vote = json_u64(*data, "voted_for");
        if (vote == 0) throw StorageError("Persisted node ID must be positive");
        state.voted_for = vote;
    }
    return state;
}

void StateStorage::save(const PersistentState& state) {
    if (state.voted_for && *state.voted_for == 0) throw StorageError("Persisted node ID must be positive");
    nlohmann::json data{{"current_term", state.current_term}, {"voted_for", nullptr}};
    if (state.voted_for) data["voted_for"] = *state.voted_for;
    write_json_record(*directory_, "state.json", "state.tmp", "raft-state", std::move(data));
}

} // namespace zmq_template::server
