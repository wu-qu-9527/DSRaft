#pragma once

#include "server/node_config.hpp"
#include "server/state.hpp"
#include "server/storage/persistent_state.hpp"
#include "server/storage/log_storage.hpp"

#include <memory>

namespace zmq_template::server {

class PeerRpc;

class server {
public:
    explicit server(NodeConfig config = {});

    int wait_util_stop();

private:
    struct Runtime;
    std::unique_ptr<Runtime> initialize(const NodeConfig& config);
    void handle_request(Runtime& runtime);
    void handle_peer_response(Runtime& runtime, PeerRpc& peer);
    void process_peer_timeouts(Runtime& runtime);
    void tick_follower(Runtime& runtime);
    void tick_candidate(Runtime& runtime);
    void tick_leader(Runtime& runtime);
    void start_election(Runtime& runtime);
    void persist_state();

    int run(const NodeConfig& config);

    Role role_{Role::Follower};
    NodeConfig config_;
    VoteState vote_state_;
    std::shared_ptr<StorageDirectory> storage_directory_;
    StateStorage state_storage_;
    LogStorage log_storage_;
    PersistentState persisted_state_;
};

} // namespace zmq_template::server
