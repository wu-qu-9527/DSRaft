#include "server/rpc/heartbeat_rpc.hpp"
#include "server/heartbeat.hpp"
#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"

#include <gtest/gtest.h>

namespace zmq_template::server {
namespace {
using namespace std::chrono_literals;

TEST(HeartbeatRpc, RoundTripsRequestAndResponse) {
    const HeartbeatRequest request{0x0102030405060708ULL, 2, 10, 3, 8};
    const auto wire = deserialize(serialize(make_heartbeat_request(42, request)));
    EXPECT_EQ(wire.id, 42u);
    EXPECT_EQ(wire.payload.size(), 40u);
    EXPECT_EQ(static_cast<unsigned char>(wire.payload[0]), 1);
    const auto decoded = decode_heartbeat_request(wire);
    EXPECT_EQ(decoded.term, request.term);
    EXPECT_EQ(decoded.leader_id, 2u);
    EXPECT_EQ(decoded.prev_log_index, 10u);
    EXPECT_EQ(decoded.prev_log_term, 3u);
    EXPECT_EQ(decoded.leader_commit, 8u);
    for (bool success : {true, false}) {
        const auto reply = deserialize(serialize(make_heartbeat_response(42, {request.term, success})));
        EXPECT_EQ(reply.id, 42u);
        EXPECT_EQ(decode_heartbeat_response(reply).term, request.term);
        EXPECT_EQ(decode_heartbeat_response(reply).success, success);
    }
}

TEST(HeartbeatRpc, RejectsInvalidSizeTypeAndBoolean) {
    auto message = make_heartbeat_request(1, {1, 2, 0, 0, 0});
    EXPECT_THROW(decode_heartbeat_response(message), std::invalid_argument);
    message.payload.pop_back();
    EXPECT_THROW(decode_heartbeat_request(message), std::invalid_argument);
    message.payload += "xx";
    EXPECT_THROW(decode_heartbeat_request(message), std::invalid_argument);
    message = make_heartbeat_response(1, {1, true});
    EXPECT_THROW(decode_heartbeat_request(message), std::invalid_argument);
    message.payload[8] = 2;
    EXPECT_THROW(decode_heartbeat_response(message), std::invalid_argument);
}

class HeartbeatHandler : public ::testing::Test {
protected:
    const ElectionTimer::Clock::time_point now{};
    ElectionTimer timer{200ms, 200ms, now};
    VoteState state{5, 1, 0, 0};
    Role role{Role::Candidate};
};

TEST_F(HeartbeatHandler, CurrentTermLeaderStepsCandidateDownAndResetsTimer) {
    const auto result = handle_heartbeat({5, 2, 0, 0, 0}, state, role, timer, now + 100ms);
    EXPECT_TRUE(result.success);
    EXPECT_EQ(role, Role::Follower);
    EXPECT_EQ(state.leader_id, 2u);
    EXPECT_EQ(state.voted_for, 1u);
    EXPECT_EQ(timer.remaining(now + 100ms), 200ms);
}

TEST_F(HeartbeatHandler, HigherTermUpdatesStateAndClearsVote) {
    const auto result = handle_heartbeat({6, 2, 0, 0, 0}, state, role, timer, now + 100ms);
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.term, 6u);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_EQ(role, Role::Follower);
}

TEST_F(HeartbeatHandler, StaleHeartbeatDoesNotChangeRoleOrResetTimer) {
    const auto result = handle_heartbeat({4, 2, 0, 0, 0}, state, role, timer, now + 100ms);
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.term, 5u);
    EXPECT_EQ(role, Role::Candidate);
    EXPECT_FALSE(state.leader_id.has_value());
    EXPECT_EQ(timer.remaining(now + 100ms), 100ms);
}

TEST_F(HeartbeatHandler, LogMismatchStillResetsElectionTimerButDoesNotCommit) {
    state.last_log_index = 10;
    state.last_log_term = 3;
    const auto result = handle_heartbeat({5, 2, 10, 2, 10}, state, role, timer, now + 100ms);
    EXPECT_FALSE(result.success);
    EXPECT_EQ(role, Role::Follower);
    EXPECT_EQ(timer.remaining(now + 100ms), 200ms);
    EXPECT_EQ(state.commit_index, 0u);
}

TEST_F(HeartbeatHandler, EmptyAppendEntriesCommitIsBoundedByMatchedIndex) {
    state.last_log_index = 10;
    state.last_log_term = 3;
    EXPECT_TRUE(handle_heartbeat({5, 2, 10, 3, 20}, state, role, timer, now).success);
    EXPECT_EQ(state.commit_index, 10u);
    handle_heartbeat({5, 2, 10, 3, 1}, state, role, timer, now);
    EXPECT_EQ(state.commit_index, 10u);
}

TEST(HeartbeatScheduling, SendsImmediatelyThenEveryFiftyMilliseconds) {
    const ElectionTimer::Clock::time_point now{};
    HeartbeatTimer timer{now};
    EXPECT_TRUE(timer.tick(now));
    EXPECT_EQ(timer.remaining(now + 49ms + 500us), 1ms);
    EXPECT_FALSE(timer.tick(now + 49ms));
    EXPECT_TRUE(timer.tick(now + 50ms));
    timer.restart(now + 60ms);
    EXPECT_TRUE(timer.tick(now + 60ms));
}

TEST(HeartbeatPeerRpc, ExchangesHeartbeatBetweenServerSockets) {
    zmq::context_t context{1};
    zmq::socket_t receiver{context, zmq::socket_type::rep};
    configure_socket(receiver);
    receiver.bind("inproc://heartbeat-test");
    PeerRpc sender{context, {2, "inproc://heartbeat-test"}};
    sender.requestHeartbeat({1, 1, 0, 0, 0}, 42);
    const auto incoming = receive_message(receiver);
    EXPECT_EQ(decode_heartbeat_request(incoming).leader_id, 1u);
    send_message(receiver, make_heartbeat_response(incoming.id, {1, true}));
    zmq::pollitem_t item{sender.handle(), 0, ZMQ_POLLIN, 0};
    ASSERT_GT(zmq::poll(&item, 1, 1000ms), 0);
    EXPECT_TRUE(sender.receiveHeartbeat().success);
    EXPECT_FALSE(sender.pending());
}

TEST(HeartbeatTermObservation, HigherTermReplyStepsLeaderDownAndClearsVote) {
    NodeConfig config;
    VoteState state;
    Role role{Role::Follower};
    Election election{config, state, role};
    election.start();
    ASSERT_EQ(role, Role::Leader);
    EXPECT_FALSE(election.observe_term(1));
    EXPECT_TRUE(election.observe_term(2));
    EXPECT_EQ(role, Role::Follower);
    EXPECT_EQ(state.current_term, 2u);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_FALSE(state.leader_id.has_value());
}
} // namespace
} // namespace zmq_template::server
