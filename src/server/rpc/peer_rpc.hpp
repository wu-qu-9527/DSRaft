#pragma once

#include "common/zmq_transport.hpp"
#include "server/rpc/vote_rpc.hpp"
#include "server/rpc/heartbeat_rpc.hpp"
#include "server/node_config.hpp"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>

namespace zmq_template::server {

// One nonblocking REQ connection per peer. Recreate after each call so a timed
// out request cannot leave the next election stuck in REQ's receive state.
class PeerRpc {
public:
    using Clock = std::chrono::steady_clock;
    PeerRpc(zmq::context_t& context, PeerConfig peer,
            std::chrono::milliseconds heartbeat_timeout = std::chrono::milliseconds{100})
        : context_(context), peer_(std::move(peer)), heartbeat_timeout_(heartbeat_timeout) {
        if (heartbeat_timeout.count() <= 0) throw std::invalid_argument("Heartbeat timeout must be positive");
    }

    void requestVoteFor(const RequestVoteRequest& request, std::uint32_t rpc_id,
                        Clock::time_point now = Clock::now()) {
        start_call(make_vote_request(rpc_id, request), request.term, now, std::chrono::milliseconds{100});
    }

    void requestHeartbeat(const HeartbeatRequest& request, std::uint32_t rpc_id,
                          Clock::time_point now = Clock::now()) {
        start_call(make_heartbeat_request(rpc_id, request), request.term, now, heartbeat_timeout_);
    }

    HeartbeatResponse receiveHeartbeat() {
        const auto response = decode_heartbeat_response(receive_response(MessageType::heartbeat));
        cancel();
        return response;
    }

    RequestVoteResponse receive_vote_response() {
        const auto response = decode_vote_response(receive_response(MessageType::request_vote));
        cancel();
        return response;
    }

    bool expired(Clock::time_point now = Clock::now()) const {
        return socket_ && now >= deadline_;
    }
    std::chrono::milliseconds remaining(Clock::time_point now = Clock::now()) const {
        if (!socket_ || now >= deadline_) return std::chrono::milliseconds{0};
        return std::chrono::ceil<std::chrono::milliseconds>(deadline_ - now);
    }
    void cancel() { socket_.reset(); }
    bool pending() const { return static_cast<bool>(socket_); }
    void* handle() const { return socket_->handle(); }
    std::uint64_t id() const { return peer_.id; }
    std::uint64_t request_term() const { return term_; }
    MessageType request_type() const { return request_type_; }

private:
    void start_call(const Message& message, std::uint64_t term, Clock::time_point now,
                    std::chrono::milliseconds timeout) {
        cancel();
        socket_ = std::make_unique<zmq::socket_t>(context_, zmq::socket_type::req);
        configure_socket(*socket_);
        socket_->set(zmq::sockopt::linger, 0);
        socket_->set(zmq::sockopt::sndtimeo, 0);
        socket_->set(zmq::sockopt::rcvtimeo, 0);
        socket_->connect(peer_.endpoint);
        rpc_id_ = message.id;
        request_type_ = message.type;
        term_ = term;
        deadline_ = now + timeout;
        send_message(*socket_, message);
    }

    Message receive_response(MessageType expected_request) {
        if (!socket_ || request_type_ != expected_request) throw std::logic_error("No matching pending RPC");
        auto message = receive_message(*socket_);
        if (message.id != rpc_id_) throw std::invalid_argument("Unexpected RPC id");
        return message;
    }

    zmq::context_t& context_;
    PeerConfig peer_;
    std::chrono::milliseconds heartbeat_timeout_;
    std::unique_ptr<zmq::socket_t> socket_;
    MessageType request_type_{MessageType::request_vote};
    std::uint32_t rpc_id_{};
    std::uint64_t term_{};
    Clock::time_point deadline_{};
};

} // namespace zmq_template::server
