#include "server/storage/raft_state_storage.hpp"

#include "server/storage/json_record.hpp"

#include <utility>

namespace zmq_template::server {
namespace {
void validate(const PersistentCommit& commit) {
    if ((commit.index == 0) != (commit.term == 0)) throw StorageError("Invalid committed log boundary");
}
} // namespace

RaftStateStorage::RaftStateStorage(std::filesystem::path data_dir)
    : RaftStateStorage(std::make_shared<StorageDirectory>(std::move(data_dir))) {}

RaftStateStorage::RaftStateStorage(std::shared_ptr<StorageDirectory> directory) : directory_(std::move(directory)) {
    if (!directory_) throw StorageError("Storage directory must not be null");
}

TermVoteState RaftStateStorage::load_term_vote() const {
    const auto data = read_json_record(*directory_, "state.json", "raft-state");
    if (!data) return {};
    if (data->size() != 2 || !data->contains("voted_for")) {
        throw StorageError("Invalid persistent state fields");
    }
    TermVoteState state{json_u64(*data, "current_term"), std::nullopt};
    if (!data->at("voted_for").is_null()) {
        const auto vote = json_u64(*data, "voted_for");
        if (vote == 0) throw StorageError("Persisted node ID must be positive");
        state.voted_for = vote;
    }
    return state;
}

void RaftStateStorage::save_term_vote(const TermVoteState& state) {
    if (state.voted_for && *state.voted_for == 0) throw StorageError("Persisted node ID must be positive");
    nlohmann::json data{{"current_term", state.current_term}, {"voted_for", nullptr}};
    if (state.voted_for) data["voted_for"] = *state.voted_for;
    write_json_record(*directory_, "state.json", "state.tmp", "raft-state", std::move(data));
}

PersistentCommit RaftStateStorage::load_commit() const {
    const auto data = read_json_record(*directory_, "commit.json", "raft-commit");
    if (!data) return {};
    if (data->size() != 2) throw StorageError("Invalid committed log checkpoint fields");
    const PersistentCommit commit{json_u64(*data, "index"), json_u64(*data, "term")};
    validate(commit);
    return commit;
}

void RaftStateStorage::save_commit(const PersistentCommit& commit) {
    validate(commit);
    const auto previous = load_commit();
    if (commit.index < previous.index || commit.term < previous.term
        || (commit.index == previous.index && commit.term != previous.term)) {
        throw StorageError("Committed log checkpoint must not move backwards or change identity");
    }
    write_json_record(*directory_, "commit.json", "commit.tmp", "raft-commit",
        {{"index", commit.index}, {"term", commit.term}});
}

} // namespace zmq_template::server
