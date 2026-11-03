#include "server/server.hpp"
#include "common/messaging.hpp"
#include "common/logging.hpp"
#include "server/state.hpp"
#include "server/election_timer.hpp"
#include "server/vote_handler.hpp"
#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"
#include "server/heartbeat.hpp"
#include "server/log_replication.hpp"
#include "server/key_value_state_machine.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <exception>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace {
volatile std::sig_atomic_t stop_requested = 0;
void request_stop(int) { stop_requested = 1; }
} // namespace

namespace zmq_template::server {

struct server::Runtime {
    explicit Runtime(const NodeConfig& config, VoteState& state, Role& role)
        : election_timer(config.election_timeout_min, config.election_timeout_max),
          election(config, state, role), heartbeat_timer(config.heartbeat_interval) {}

    // Keep context before sockets so it is destroyed after all connections.
    zmq::context_t context{1};
    zmq::socket_t socket{context, zmq::socket_type::rep};
    ElectionTimer election_timer;
    Election election;
    HeartbeatTimer heartbeat_timer;
    bool leader_active{false};
    std::uint64_t heartbeat_term{0};
    std::vector<PeerRpc> peers;
    std::vector<LogEntry> log;
    KeyValueStateMachine machine;
    std::unordered_map<std::uint64_t, LogReplication> replication;
    std::uint32_t next_rpc_id{0};
};

server::server(NodeConfig config)
    : config_(std::move(config)),
      storage_directory_(std::make_shared<StorageDirectory>(config_.data_dir.empty()
          ? std::filesystem::read_symlink("/proc/self/exe").parent_path()
              / "data" / ("node-" + std::to_string(config_.id))
          : config_.data_dir)),
      state_storage_(storage_directory_), log_storage_(storage_directory_), commit_storage_(storage_directory_) {}

void server::persist_state() {
    const PersistentState next{vote_state_.current_term, vote_state_.voted_for};
    if (next.current_term == persisted_state_.current_term
        && next.voted_for == persisted_state_.voted_for) return;
    state_storage_.save(next);
    persisted_state_ = next;
}

void server::persist_commit_and_apply(Runtime& runtime) {
    if (vote_state_.commit_index > persisted_commit_.index) {
        const PersistentCommit next{vote_state_.commit_index, runtime.log.at(vote_state_.commit_index - 1).term};
        commit_storage_.save(next);
        persisted_commit_ = next;
    }
    runtime.machine.apply_committed(runtime.log, vote_state_.commit_index);
}

int server::wait_util_stop() {
    try {
        return run(config_);
    } catch (const std::exception& error) {
        spdlog::error("Server error: {}", error.what());
        return 1;
    }
}

std::unique_ptr<server::Runtime> server::initialize(const NodeConfig& config) {
    stop_requested = 0;
    persisted_state_ = state_storage_.load();
    vote_state_ = {};
    vote_state_.current_term = persisted_state_.current_term;
    vote_state_.voted_for = persisted_state_.voted_for;
    auto log = log_storage_.load();
    if (!log.empty()) {
        vote_state_.last_log_index = log.back().index;
        vote_state_.last_log_term = log.back().term;
        if (vote_state_.last_log_term > vote_state_.current_term) {
            throw StorageError("Persistent log term exceeds current term");
        }
    }
    role_ = Role::Follower;
    spdlog::info("Recovered persistent state term={} voted_for={}",
                 vote_state_.current_term, vote_state_.voted_for.value_or(0));
    auto runtime = std::make_unique<Runtime>(config, vote_state_, role_);
    runtime->log = std::move(log);
    persisted_commit_ = commit_storage_.load();
    if (persisted_commit_.index > runtime->log.size()
        || (persisted_commit_.index != 0 && runtime->log[persisted_commit_.index - 1].term != persisted_commit_.term)) {
        throw StorageError("Committed checkpoint does not match the persistent log");
    }
    vote_state_.commit_index = persisted_commit_.index;
    runtime->machine.apply_committed(runtime->log, vote_state_.commit_index);
    zmq_template::configure_socket(runtime->socket);
    // Poll owns the wait; a slow/disconnected peer must not block elections.
    runtime->socket.set(zmq::sockopt::rcvtimeo, 0);
    runtime->socket.set(zmq::sockopt::sndtimeo, 0);
    runtime->socket.bind(config.endpoint);
    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    for (const auto& peer : config.peers) runtime->peers.emplace_back(runtime->context, peer, config.heartbeat_timeout);
    spdlog::info("Listening on {} node={} role={} election_timeout={}ms election_range=[{},{}]ms "
                 "heartbeat_interval={}ms heartbeat_timeout={}ms",
                 runtime->socket.get(zmq::sockopt::last_endpoint), config.id,
                 to_string(role_), runtime->election_timer.timeout().count(),
                 config.election_timeout_min.count(), config.election_timeout_max.count(),
                 config.heartbeat_interval.count(), config.heartbeat_timeout.count());
    return runtime;
}

void server::handle_request(Runtime& runtime) {
    zmq_template::Message response;
    try {
        const auto message = zmq_template::receive_message(runtime.socket);
        response.id = message.id;
        if (message.type == zmq_template::MessageType::request_vote) {
            const auto request = zmq_template::decode_vote_request(message);
            const auto result = voteFor(request, vote_state_, role_, runtime.election_timer);
            persist_state();
            response = zmq_template::make_vote_response(message.id, result);
            spdlog::info("RequestVote candidate={} request_term={} current_term={} granted={} role={}",
                         request.candidate_id, request.term, result.term, result.vote_granted, to_string(role_));
        } else if (message.type == zmq_template::MessageType::heartbeat) {
            const auto request = zmq_template::decode_heartbeat_request(message);
            const auto result = handle_heartbeat(request, vote_state_, role_, runtime.election_timer, runtime.log);
            persist_state(); // A higher term must be durable before logs from that term.
            if (result.changed_from) {
                const auto first = runtime.log.begin() + static_cast<std::size_t>(*result.changed_from - 1);
                log_storage_.replace_suffix(*result.changed_from, {first, runtime.log.end()});
            }
            persist_commit_and_apply(runtime);
            response = zmq_template::make_heartbeat_response(message.id, result.response);
            spdlog::debug("Heartbeat received leader={} term={} success={}",
                          request.leader_id, request.term, result.response.success);
        } else if (message.type == zmq_template::MessageType::text) {
            response = {zmq_template::MessageType::acknowledgement, message.id, "Hello, " + message.payload};
        } else {
            throw std::invalid_argument("Unsupported RPC request type");
        }
    } catch (const std::invalid_argument& error) {
        response.type = zmq_template::MessageType::error;
        response.payload = error.what();
        spdlog::warn("Rejected invalid request: {}", error.what());
    }
    zmq_template::send_message(runtime.socket, response);
    spdlog::debug("Reply sent for request id={}", response.id);
}

void server::handle_peer_response(Runtime& runtime, PeerRpc& peer) {
    try {
        const auto request_term = peer.request_term();
        if (peer.request_type() == zmq_template::MessageType::heartbeat) {
            const auto response = peer.receiveHeartbeat();
            if (runtime.election.become_follower_if_higher_term(response.term)) {
                persist_state();
                runtime.election_timer.reset();
                spdlog::info("Higher heartbeat response term={}; stepping down to Follower", response.term);
            }
            if (role_ == Role::Leader && request_term == vote_state_.current_term
                && response.term == vote_state_.current_term && runtime.leader_active) {
                runtime.replication.at(peer.id()).acknowledge(response.success);
            }
            spdlog::debug("Heartbeat response peer={} term={} success={}",
                          peer.id(), response.term, response.success);
        } else if (peer.request_type() == zmq_template::MessageType::request_vote) {
            const auto response = peer.receive_vote_response();
            const auto previous_term = vote_state_.current_term;
            const auto previous_role = role_;
            runtime.election.handle_vote_response(peer.id(), request_term, response);
            persist_state();
            if (vote_state_.current_term > previous_term) runtime.election_timer.reset();
            spdlog::info("voteFor peer={} term={} granted={} votes={}/{} role={}",
                         peer.id(), response.term, response.vote_granted,
                         runtime.election.votes(), runtime.election.majority(), to_string(role_));
            if (previous_role != Role::Leader && role_ == Role::Leader) {
                spdlog::info("Node {} elected Leader term={}", config_.id, vote_state_.current_term);
            }
        } else {
            throw std::logic_error("Unsupported pending RPC type");
        }
    } catch (const StorageError&) {
        throw; // A durability failure is fatal, not a recoverable peer error.
    } catch (const std::exception& error) {
        peer.cancel();
        spdlog::warn("Peer RPC peer={} failed: {}", peer.id(), error.what());
    }
}

void server::process_peer_timeouts(Runtime& runtime) {
    for (auto& peer : runtime.peers) {
        if (peer.expired()) {
            spdlog::warn("Peer RPC timeout peer={} type={}", peer.id(), static_cast<int>(peer.request_type()));
            peer.cancel();
        }
    }
}

void server::tick_follower(Runtime& runtime) {
    runtime.leader_active = false;
    if (runtime.election_timer.expired()) start_election(runtime);
}

void server::tick_candidate(Runtime& runtime) {
    runtime.leader_active = false;
    if (runtime.election_timer.expired()) start_election(runtime);
}

void server::start_election(Runtime& runtime) {
    const auto request = runtime.election.start();
    persist_state(); // Durable term and self-vote before any election RPC or heartbeat.
    runtime.election_timer.reset();
    spdlog::info("Node {} starts election term={} self_vote={} next_timeout={}ms",
                 config_.id, request.term, config_.id, runtime.election_timer.timeout().count());
    for (auto& peer : runtime.peers) {
        try {
            peer.requestVoteFor(request, ++runtime.next_rpc_id);
            spdlog::debug("requestVoteFor peer={} term={}", peer.id(), request.term);
        } catch (const std::exception& error) {
            peer.cancel();
            spdlog::warn("RequestVote send peer={} failed: {}", peer.id(), error.what());
        }
    }
    if (role_ == Role::Leader) {
        spdlog::info("Node {} elected Leader term={}", config_.id, vote_state_.current_term);
        tick_leader(runtime);
    }
}

void server::tick_leader(Runtime& runtime) {
    if (!runtime.leader_active || runtime.heartbeat_term != vote_state_.current_term) {
        // Replace any outstanding election RPCs with immediate leader contact.
        for (auto& peer : runtime.peers) {
            peer.cancel();
            runtime.replication[peer.id()].reset(vote_state_.last_log_index);
        }
        runtime.heartbeat_timer.restart();
        runtime.leader_active = true;
        runtime.heartbeat_term = vote_state_.current_term;
    }
    if (runtime.heartbeat_timer.tick()) {
        for (auto& peer : runtime.peers) {
            if (peer.pending()) continue;
            const auto request = runtime.replication.at(peer.id()).make_request(runtime.log,
                vote_state_.current_term, config_.id, vote_state_.commit_index);
            try {
                peer.requestHeartbeat(request, ++runtime.next_rpc_id);
                spdlog::debug("Heartbeat sent peer={} term={}", peer.id(), request.term);
            } catch (const std::exception& error) {
                peer.cancel();
                spdlog::warn("Heartbeat send peer={} failed: {}", peer.id(), error.what());
            }
        }
    }
}

int server::run(const NodeConfig& config) {
    auto runtime = initialize(config);
    while (!stop_requested) {
        auto wait = role_ == Role::Leader ? std::chrono::milliseconds{100}
            : std::min(runtime->election_timer.remaining(), std::chrono::milliseconds{100});
        if (role_ == Role::Leader) {
            wait = runtime->leader_active && runtime->heartbeat_term == vote_state_.current_term
                ? std::min(wait, runtime->heartbeat_timer.remaining()) : std::chrono::milliseconds{0};
        }
        std::vector<zmq::pollitem_t> items{{runtime->socket.handle(), 0, ZMQ_POLLIN, 0}};
        std::vector<std::size_t> polled_peers;
        for (std::size_t i = 0; i < runtime->peers.size(); ++i) {
            if (!runtime->peers[i].pending()) continue;
            wait = std::min(wait, runtime->peers[i].remaining());
            items.push_back({runtime->peers[i].handle(), 0, ZMQ_POLLIN, 0});
            polled_peers.push_back(i);
        }
        try {
            zmq::poll(items, wait);
        } catch (const zmq::error_t& error) {
            if (error.num() == EINTR) continue;
            throw;
        }
        if (stop_requested) break;
        if (items[0].revents & ZMQ_POLLIN) {
            handle_request(*runtime);
        }
        for (std::size_t i = 0; i < polled_peers.size(); ++i) {
            if (!(items[i + 1].revents & ZMQ_POLLIN)) continue;
            handle_peer_response(*runtime, runtime->peers[polled_peers[i]]);
        }
        process_peer_timeouts(*runtime);
        switch (role_) {
        case Role::Follower:
            tick_follower(*runtime);
            break;
        case Role::Candidate:
            tick_candidate(*runtime);
            break;
        case Role::Leader:
            tick_leader(*runtime);
            break;
        }
    }
    spdlog::info("Server stopped");
    return 0;
}

} // namespace zmq_template::server
