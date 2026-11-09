#include "server/server.hpp"
#include "common/zmq_transport.hpp"
#include "common/logging.hpp"
#include "server/state.hpp"
#include "server/election_timer.hpp"
#include "server/vote_handler.hpp"
#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"
#include "server/heartbeat.hpp"
#include "server/log_replication.hpp"
#include "server/state_machine/key_value_state_machine.hpp"
#include "server/commit_index.hpp"

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
    struct PendingCommand {
        std::string route;
        std::uint32_t rpc_id;
        std::uint64_t index;
        std::uint64_t term;
        ElectionTimer::Clock::time_point deadline;
        std::optional<std::string> result;
    };
    explicit Runtime(const NodeConfig& config, TermVoteState& state, const LogState& log_state,
                     LeadershipState& leadership)
        : election_timer(config.election_timeout_min, config.election_timeout_max),
          election(config, state, log_state, leadership), heartbeat_timer(config.heartbeat_interval) {}

    // Keep context before sockets so it is destroyed after all connections.
    zmq::context_t context{1};
    zmq::socket_t socket{context, zmq::socket_type::router};
    ElectionTimer election_timer;
    Election election;
    HeartbeatTimer heartbeat_timer;
    bool leader_active{false};
    std::uint64_t heartbeat_term{0};
    std::vector<PeerRpc> peers;
    std::vector<LogEntry> log;
    KeyValueStateMachine machine;
    std::vector<PendingCommand> pending;
    std::unordered_map<std::uint64_t, LogReplication> replication;
    std::uint32_t next_rpc_id{0};
};

server::server(NodeConfig config)
    : config_(std::move(config)),
      storage_directory_(std::make_shared<StorageDirectory>(config_.data_dir.empty()
          ? std::filesystem::read_symlink("/proc/self/exe").parent_path()
              / "data" / ("node-" + std::to_string(config_.id))
          : config_.data_dir)),
      state_storage_(storage_directory_), log_storage_(storage_directory_) {}

void server::persist_state() {
    const auto& next = term_vote_state_;
    if (next.current_term == persisted_state_.current_term
        && next.voted_for == persisted_state_.voted_for) return;
    state_storage_.save_term_vote(next);
    persisted_state_ = next;
}

void server::persist_commit_and_apply(Runtime& runtime) {
    if (commit_state_.commit_index > persisted_commit_.index) {
        const PersistentCommit next{commit_state_.commit_index, runtime.log.at(commit_state_.commit_index - 1).term};
        state_storage_.save_commit(next);
        persisted_commit_ = next;
    }
    const auto applied = runtime.machine.apply_committed(runtime.log, commit_state_.commit_index);
    for (const auto& result : applied) {
        for (auto& pending : runtime.pending) {
            if (pending.index == result.index && pending.term == runtime.log[result.index - 1].term) {
                pending.result = result.result;
            }
        }
    }
}

void server::advance_commit(Runtime& runtime) {
    if (leadership_.role != Role::Leader || !runtime.leader_active) return;
    std::vector<std::uint64_t> matches;
    for (const auto& peer : runtime.peers) matches.push_back(runtime.replication.at(peer.id()).match_index());
    commit_state_.commit_index = majority_commit_index(runtime.log, term_vote_state_.current_term,
        commit_state_.commit_index, std::move(matches));
    persist_commit_and_apply(runtime);
}

void server::complete_pending(Runtime& runtime) {
    const auto now = ElectionTimer::Clock::now();
    auto pending = runtime.pending.begin();
    while (pending != runtime.pending.end()) {
        zmq_template::Message response{MessageType::error, pending->rpc_id, {}};
        if (pending->result) {
            response.type = MessageType::acknowledgement;
            response.payload = *pending->result;
        } else if (leadership_.role != Role::Leader || pending->term != term_vote_state_.current_term) {
            response.payload = "Leadership changed; outcome unknown. Retrying may execute the command again.";
        } else if (now >= pending->deadline) {
            response.payload = "Commit timed out; outcome unknown. Retrying may execute the command again.";
        } else {
            ++pending;
            continue;
        }
        reply(runtime, pending->route, response);
        pending = runtime.pending.erase(pending);
    }
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
    persisted_state_ = state_storage_.load_term_vote();
    term_vote_state_ = persisted_state_;
    log_state_ = {};
    commit_state_ = {};
    leadership_ = {};
    auto log = log_storage_.load();
    if (!log.empty()) {
        log_state_.last_log_index = log.back().index;
        log_state_.last_log_term = log.back().term;
        if (log_state_.last_log_term > term_vote_state_.current_term) {
            throw StorageError("Persistent log term exceeds current term");
        }
    }
    spdlog::info("Recovered persistent state term={} voted_for={}",
                 term_vote_state_.current_term, term_vote_state_.voted_for.value_or(0));
    auto runtime = std::make_unique<Runtime>(config, term_vote_state_, log_state_, leadership_);
    runtime->log = std::move(log);
    persisted_commit_ = state_storage_.load_commit();
    if (persisted_commit_.index > runtime->log.size()
        || (persisted_commit_.index != 0 && runtime->log[persisted_commit_.index - 1].term != persisted_commit_.term)) {
        throw StorageError("Committed checkpoint does not match the persistent log");
    }
    commit_state_.commit_index = persisted_commit_.index;
    runtime->machine.apply_committed(runtime->log, commit_state_.commit_index);
    zmq_template::configure_socket(runtime->socket);
    // Poll owns the wait; a slow/disconnected peer must not block elections.
    runtime->socket.set(zmq::sockopt::rcvtimeo, 0);
    runtime->socket.set(zmq::sockopt::sndtimeo, 0);
    runtime->socket.set(zmq::sockopt::router_mandatory, 1);
    runtime->socket.bind(config.endpoint);
    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    for (const auto& peer : config.peers) runtime->peers.emplace_back(runtime->context, peer, config.heartbeat_timeout);
    spdlog::info("Listening on {} node={} role={} election_timeout={}ms election_range=[{},{}]ms "
                 "heartbeat_interval={}ms heartbeat_timeout={}ms",
                 runtime->socket.get(zmq::sockopt::last_endpoint), config.id,
                 to_string(leadership_.role), runtime->election_timer.timeout().count(),
                 config.election_timeout_min.count(), config.election_timeout_max.count(),
                 config.heartbeat_interval.count(), config.heartbeat_timeout.count());
    return runtime;
}

void server::reply(Runtime& runtime, const std::string& route, const zmq_template::Message& message) {
    try {
        zmq_template::send_routed_message(runtime.socket, route, message);
    } catch (const std::exception& error) {
        // The durable result survives a disconnected client; a failed delivery must not stop Raft.
        spdlog::warn("Reply delivery failed id={}: {}", message.id, error.what());
    }
}

void server::handle_request(Runtime& runtime) {
    std::string route;
    zmq_template::Message response;
    try {
        const auto message = zmq_template::receive_routed_message(runtime.socket, route);
        response.id = message.id;
        if (message.type == zmq_template::MessageType::request_vote) {
            const auto request = zmq_template::decode_vote_request(message);
            const auto result = voteFor(request, term_vote_state_, log_state_, leadership_, runtime.election_timer);
            persist_state();
            response = zmq_template::make_vote_response(message.id, result);
            spdlog::info("RequestVote candidate={} request_term={} current_term={} granted={} role={}",
                         request.candidate_id, request.term, result.term, result.vote_granted, to_string(leadership_.role));
        } else if (message.type == zmq_template::MessageType::heartbeat) {
            const auto request = zmq_template::decode_heartbeat_request(message);
            const auto result = handle_heartbeat(request, term_vote_state_, log_state_, commit_state_,
                leadership_, runtime.election_timer, runtime.log);
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
            if (leadership_.role != Role::Leader || !runtime.leader_active
                || runtime.heartbeat_term != term_vote_state_.current_term) {
                throw std::invalid_argument(leadership_.leader_id
                    ? "Not leader; known leader ID=" + std::to_string(*leadership_.leader_id)
                    : "Not leader; leader unknown");
            }
            KeyValueStateMachine::validate_command(message.payload);
            if (message.payload.size() > max_payload_size - 72) {
                throw std::invalid_argument("Command is too large for AppendEntries");
            }
            if (runtime.pending.size() >= 1024) throw std::invalid_argument("Too many pending commands");
            const LogEntry entry{log_state_.last_log_index + 1, term_vote_state_.current_term, message.payload};
            log_storage_.append({entry});
            runtime.log.push_back(entry);
            log_state_.last_log_index = entry.index;
            log_state_.last_log_term = entry.term;
            runtime.pending.push_back({route, message.id, entry.index, entry.term,
                ElectionTimer::Clock::now() + std::chrono::seconds{2}, std::nullopt});
            advance_commit(runtime);
            runtime.heartbeat_timer.restart();
            return; // The event loop replies after commit/application, timeout, or leadership loss.
        } else {
            throw std::invalid_argument("Unsupported RPC request type");
        }
    } catch (const std::invalid_argument& error) {
        response.type = zmq_template::MessageType::error;
        response.payload = error.what();
        spdlog::warn("Rejected invalid request: {}", error.what());
    }
    reply(runtime, route, response);
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
            if (leadership_.role == Role::Leader && request_term == term_vote_state_.current_term
                && response.term == term_vote_state_.current_term && runtime.leader_active) {
                runtime.replication.at(peer.id()).acknowledge(response.success);
                advance_commit(runtime);
            }
            spdlog::debug("Heartbeat response peer={} term={} success={}",
                          peer.id(), response.term, response.success);
        } else if (peer.request_type() == zmq_template::MessageType::request_vote) {
            const auto response = peer.receive_vote_response();
            const auto previous_term = term_vote_state_.current_term;
            const auto previous_role = leadership_.role;
            runtime.election.handle_vote_response(peer.id(), request_term, response);
            persist_state();
            if (term_vote_state_.current_term > previous_term) runtime.election_timer.reset();
            spdlog::info("voteFor peer={} term={} granted={} votes={}/{} role={}",
                         peer.id(), response.term, response.vote_granted,
                         runtime.election.votes(), runtime.election.majority(), to_string(leadership_.role));
            if (previous_role != Role::Leader && leadership_.role == Role::Leader) {
                spdlog::info("Node {} elected Leader term={}", config_.id, term_vote_state_.current_term);
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
    if (leadership_.role == Role::Leader) {
        spdlog::info("Node {} elected Leader term={}", config_.id, term_vote_state_.current_term);
        tick_leader(runtime);
    }
}

void server::tick_leader(Runtime& runtime) {
    if (!runtime.leader_active || runtime.heartbeat_term != term_vote_state_.current_term) {
        // Replace any outstanding election RPCs with immediate leader contact.
        for (auto& peer : runtime.peers) {
            peer.cancel();
            runtime.replication[peer.id()].reset(log_state_.last_log_index);
        }
        // Persist a current-term no-op before replication; never commit an old term by counting it alone.
        const LogEntry noop{log_state_.last_log_index + 1, term_vote_state_.current_term, {}};
        log_storage_.append({noop});
        runtime.log.push_back(noop);
        log_state_.last_log_index = noop.index;
        log_state_.last_log_term = noop.term;
        runtime.heartbeat_timer.restart();
        runtime.leader_active = true;
        runtime.heartbeat_term = term_vote_state_.current_term;
        advance_commit(runtime);
    }
    if (runtime.heartbeat_timer.tick()) {
        for (auto& peer : runtime.peers) {
            if (peer.pending()) continue;
            const auto request = runtime.replication.at(peer.id()).make_request(runtime.log,
                term_vote_state_.current_term, config_.id, commit_state_.commit_index);
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
        auto wait = leadership_.role == Role::Leader ? std::chrono::milliseconds{100}
            : std::min(runtime->election_timer.remaining(), std::chrono::milliseconds{100});
        if (leadership_.role == Role::Leader) {
            wait = runtime->leader_active && runtime->heartbeat_term == term_vote_state_.current_term
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
        switch (leadership_.role) {
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
        complete_pending(*runtime);
    }
    spdlog::info("Server stopped");
    return 0;
}

} // namespace zmq_template::server
