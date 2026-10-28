#include "common/messaging.hpp"
#include "common/logging.hpp"
#include "server/state.hpp"
#include "server/election_timer.hpp"
#include "server/vote_handler.hpp"
#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"
#include "server/heartbeat.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <exception>
#include <iostream>

namespace {
volatile std::sig_atomic_t stop_requested = 0;
void request_stop(int) { stop_requested = 1; }
} // namespace

int main(int argc, char* argv[]) {
    if (argc == 2 && std::string(argv[1]) == "--help") {
        std::cout << "Usage: zmq_server [bind_endpoint] [--node-id ID] [--peer ID=endpoint ...]\n";
        return 0;
    }

    zmq_template::init_logging("server");

    // try {
    //
    //     using namespace zmq_template::server;
    //     const auto config = parse_node_config(argc, argv);
    //     zmq::context_t context{1};
    //     zmq::socket_t socket{context, zmq::socket_type::rep};
    //     zmq_template::configure_socket(socket);
    //     // Poll owns the wait; a slow/disconnected peer must not block elections.
    //     socket.set(zmq::sockopt::rcvtimeo, 0);
    //     socket.set(zmq::sockopt::sndtimeo, 0);
    //     socket.bind(config.endpoint);
    //     std::signal(SIGINT, request_stop);
    //     std::signal(SIGTERM, request_stop);
    //     ElectionTimer election_timer;
    //     Election election{config, vote_state, role};
    //     HeartbeatTimer heartbeat_timer;
    //     bool leader_active = false;
    //     std::uint64_t heartbeat_term = 0;
    //     std::vector<PeerRpc> peers;
    //     for (const auto& peer : config.peers) peers.emplace_back(context, peer);
    //     std::uint32_t next_rpc_id = 0;
    //     spdlog::info("Listening on {} node={} role={} election_timeout={}ms",
    //                  socket.get(zmq::sockopt::last_endpoint), config.id,
    //                  zmq_template::server::to_string(zmq_template::server::role),
    //                  election_timer.timeout().count());
    //     while (!stop_requested) {
    //         auto wait = role == Role::Leader ? std::chrono::milliseconds{100}
    //             : std::min(election_timer.remaining(), std::chrono::milliseconds{100});
    //         if (role == Role::Leader) {
    //             wait = leader_active && heartbeat_term == vote_state.current_term
    //                 ? std::min(wait, heartbeat_timer.remaining()) : std::chrono::milliseconds{0};
    //         }
    //         std::vector<zmq::pollitem_t> items{{socket.handle(), 0, ZMQ_POLLIN, 0}};
    //         std::vector<std::size_t> polled_peers;
    //         for (std::size_t i = 0; i < peers.size(); ++i) {
    //             if (!peers[i].pending()) continue;
    //             wait = std::min(wait, peers[i].remaining());
    //             items.push_back({peers[i].handle(), 0, ZMQ_POLLIN, 0});
    //             polled_peers.push_back(i);
    //         }
    //         try {
    //             zmq::poll(items, wait);
    //         } catch (const zmq::error_t& error) {
    //             if (error.num() == EINTR) continue;
    //             throw;
    //         }
    //         if (stop_requested) break;
    //         if (items[0].revents & ZMQ_POLLIN) {
    //             zmq_template::Message response;
    //             try {
    //                 const auto message = zmq_template::receive_message(socket);
    //                 response.id = message.id;
    //                 if (message.type == zmq_template::MessageType::request_vote) {
    //                     const auto request = zmq_template::decode_vote_request(message);
    //                     const auto result = voteFor(request, vote_state, role, election_timer);
    //                     response = zmq_template::make_vote_response(message.id, result);
    //                     spdlog::info("RequestVote candidate={} request_term={} current_term={} granted={} role={}",
    //                                  request.candidate_id, request.term, result.term, result.vote_granted, to_string(role));
    //                 } else if (message.type == zmq_template::MessageType::heartbeat) {
    //                     const auto request = zmq_template::decode_heartbeat_request(message);
    //                     const auto result = handle_heartbeat(request, vote_state, role, election_timer);
    //                     response = zmq_template::make_heartbeat_response(message.id, result);
    //                     spdlog::debug("Heartbeat received leader={} term={} success={}",
    //                                   request.leader_id, request.term, result.success);
    //                 } else if (message.type == zmq_template::MessageType::text) {
    //                     response = {zmq_template::MessageType::acknowledgement, message.id, "Hello, " + message.payload};
    //                 } else {
    //                     throw std::invalid_argument("Unsupported RPC request type");
    //                 }
    //             } catch (const std::invalid_argument& error) {
    //                 response.type = zmq_template::MessageType::error;
    //                 response.payload = error.what();
    //                 spdlog::warn("Rejected invalid request: {}", error.what());
    //             }
    //             zmq_template::send_message(socket, response);
    //             spdlog::debug("Reply sent for request id={}", response.id);
    //         }
    //         for (std::size_t i = 0; i < polled_peers.size(); ++i) {
    //             auto& peer = peers[polled_peers[i]];
    //             if (!(items[i + 1].revents & ZMQ_POLLIN)) continue;
    //             try {
    //                 const auto request_term = peer.request_term();
    //                 if (peer.request_type() == zmq_template::MessageType::heartbeat) {
    //                     const auto response = peer.receiveHeartbeat();
    //                     if (election.observe_term(response.term)) {
    //                         election_timer.reset();
    //                         spdlog::info("Higher heartbeat response term={}; stepping down to Follower", response.term);
    //                     }
    //                     spdlog::debug("Heartbeat response peer={} term={} success={}",
    //                                   peer.id(), response.term, response.success);
    //                 } else {
    //                     const auto response = peer.receiveVoteFor();
    //                     const auto previous_term = vote_state.current_term;
    //                     const auto previous_role = role;
    //                     election.receiveVoteFor(peer.id(), request_term, response);
    //                     if (vote_state.current_term > previous_term) election_timer.reset();
    //                     spdlog::info("voteFor peer={} term={} granted={} votes={}/{} role={}",
    //                                  peer.id(), response.term, response.vote_granted,
    //                                  election.votes(), election.majority(), to_string(role));
    //                     if (previous_role != Role::Leader && role == Role::Leader) {
    //                         spdlog::info("Node {} elected Leader term={}", config.id, vote_state.current_term);
    //                     }
    //                 }
    //             } catch (const std::exception& error) {
    //                 peer.cancel();
    //                 spdlog::warn("Peer RPC peer={} failed: {}", peer.id(), error.what());
    //             }
    //         }
    //         for (auto& peer : peers) {
    //             if (peer.expired()) {
    //                 spdlog::warn("Peer RPC timeout peer={} type={}", peer.id(), static_cast<int>(peer.request_type()));
    //                 peer.cancel();
    //             }
    //         }
    //         if (election_timer.tick(role)) {
    //             const auto request = election.start();
    //             spdlog::info("Node {} starts election term={} self_vote={} next_timeout={}ms",
    //                          config.id, request.term, config.id, election_timer.timeout().count());
    //             for (auto& peer : peers) {
    //                 try {
    //                     peer.requestVoteFor(request, ++next_rpc_id);
    //                     spdlog::debug("requestVoteFor peer={} term={}", peer.id(), request.term);
    //                 } catch (const std::exception& error) {
    //                     peer.cancel();
    //                     spdlog::warn("RequestVote send peer={} failed: {}", peer.id(), error.what());
    //                 }
    //             }
    //             if (role == Role::Leader) {
    //                 spdlog::info("Node {} elected Leader term={}", config.id, vote_state.current_term);
    //             }
    //         }
    //         if (role == Role::Leader) {
    //             if (!leader_active || heartbeat_term != vote_state.current_term) {
    //                 // Replace any outstanding election RPCs with immediate leader contact.
    //                 for (auto& peer : peers) peer.cancel();
    //                 heartbeat_timer.restart();
    //                 leader_active = true;
    //                 heartbeat_term = vote_state.current_term;
    //             }
    //             if (heartbeat_timer.tick()) {
    //                 const zmq_template::HeartbeatRequest request{vote_state.current_term, config.id,
    //                     vote_state.last_log_index, vote_state.last_log_term, vote_state.commit_index};
    //                 for (auto& peer : peers) {
    //                     if (peer.pending()) continue;
    //                     try {
    //                         peer.requestHeartbeat(request, ++next_rpc_id);
    //                         spdlog::debug("Heartbeat sent peer={} term={}", peer.id(), request.term);
    //                     } catch (const std::exception& error) {
    //                         peer.cancel();
    //                         spdlog::warn("Heartbeat send peer={} failed: {}", peer.id(), error.what());
    //                     }
    //                 }
    //             }
    //         } else {
    //             leader_active = false;
    //         }
    //     }
    //     spdlog::info("Server stopped");
    // } catch (const std::exception& error) {
    //     spdlog::error("Server error: {}", error.what());
    //     return 1;
    // }
}
