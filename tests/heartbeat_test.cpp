#include "server/rpc/heartbeat_rpc.hpp"
#include "server/heartbeat.hpp"
#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"
#include "server/log_replication.hpp"

#include <gtest/gtest.h>

namespace zmq_template::server {
namespace {
using namespace std::chrono_literals;

TEST(HeartbeatRpc, RoundTripsRequestAndResponse) {
    const HeartbeatRequest request{0x0102030405060708ULL, 2, 10, 3, 8};
    const auto wire = deserialize(serialize(make_heartbeat_request(42, request)));
    EXPECT_EQ(wire.id, 42u);
    EXPECT_EQ(wire.payload.size(), 48u);
    EXPECT_EQ(static_cast<unsigned char>(wire.payload[0]), 1);
    const auto decoded = decode_heartbeat_request(wire);
    EXPECT_EQ(decoded.term, request.term);
    EXPECT_EQ(decoded.leader_id, 2u);
    EXPECT_EQ(decoded.prev_log_index, 10u);
    EXPECT_EQ(decoded.prev_log_term, 3u);
    EXPECT_EQ(decoded.leader_commit, 8u);
    EXPECT_TRUE(decoded.entries.empty());
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
    TermVoteState state{5, 1};
    LogState log_state;
    CommitState commit_state;
    LeadershipState leadership{Role::Candidate, std::nullopt};
    std::vector<LogEntry> log;
};

TEST_F(HeartbeatHandler, CurrentTermLeaderStepsCandidateDownAndResetsTimer) {
    const auto result = handle_heartbeat({5, 2, 0, 0, 0},
        state, log_state, commit_state, leadership, timer, log, now + 100ms);
    EXPECT_TRUE(result.response.success);
    EXPECT_EQ(leadership.role, Role::Follower);
    EXPECT_EQ(leadership.leader_id, 2u);
    EXPECT_EQ(state.voted_for, 1u);
    EXPECT_EQ(timer.remaining(now + 100ms), 200ms);
}

TEST_F(HeartbeatHandler, HigherTermUpdatesStateAndClearsVote) {
    const auto result = handle_heartbeat({6, 2, 0, 0, 0},
        state, log_state, commit_state, leadership, timer, log, now + 100ms);
    EXPECT_TRUE(result.response.success);
    EXPECT_EQ(result.response.term, 6u);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_EQ(leadership.role, Role::Follower);
}

TEST_F(HeartbeatHandler, StaleHeartbeatDoesNotChangeRoleOrResetTimer) {
    const auto result = handle_heartbeat({4, 2, 0, 0, 0},
        state, log_state, commit_state, leadership, timer, log, now + 100ms);
    EXPECT_FALSE(result.response.success);
    EXPECT_EQ(result.response.term, 5u);
    EXPECT_EQ(leadership.role, Role::Candidate);
    EXPECT_FALSE(leadership.leader_id.has_value());
    EXPECT_EQ(timer.remaining(now + 100ms), 100ms);
}

TEST_F(HeartbeatHandler, LogMismatchStillResetsElectionTimerButDoesNotCommit) {
    log_state.last_log_index = 10;
    log_state.last_log_term = 3;
    for (std::uint64_t i = 1; i <= 10; ++i) log.push_back({i, 3, "command"});
    const auto result = handle_heartbeat({5, 2, 10, 2, 10},
        state, log_state, commit_state, leadership, timer, log, now + 100ms);
    EXPECT_FALSE(result.response.success);
    EXPECT_EQ(leadership.role, Role::Follower);
    EXPECT_EQ(timer.remaining(now + 100ms), 200ms);
    EXPECT_EQ(commit_state.commit_index, 0u);
}

TEST_F(HeartbeatHandler, EmptyAppendEntriesCommitIsBoundedByMatchedIndex) {
    log_state.last_log_index = 10;
    log_state.last_log_term = 3;
    for (std::uint64_t i = 1; i <= 10; ++i) log.push_back({i, 3, "command"});
    EXPECT_TRUE(handle_heartbeat({5, 2, 10, 3, 20},
        state, log_state, commit_state, leadership, timer, log, now).response.success);
    EXPECT_EQ(commit_state.commit_index, 10u);
    handle_heartbeat({5, 2, 10, 3, 1}, state, log_state, commit_state, leadership, timer, log, now);
    EXPECT_EQ(commit_state.commit_index, 10u);
}

TEST(HeartbeatRpc, RoundTripsBinaryEntriesAndRejectsMalformedSequences) {
    const std::string binary{"a\0b", 3};
    const HeartbeatRequest request{6, 2, 1, 3, 2, {{2, 4, binary}, {3, 6, ""}}};
    const auto wire = deserialize(serialize(make_heartbeat_request(9, request)));
    const auto decoded = decode_heartbeat_request(wire);
    ASSERT_EQ(decoded.entries.size(), 2u);
    EXPECT_EQ(decoded.entries[0].index, 2u);
    EXPECT_EQ(decoded.entries[0].term, 4u);
    EXPECT_EQ(decoded.entries[0].command, binary);
    EXPECT_TRUE(decoded.entries[1].command.empty());
    for (std::size_t size = 0; size < wire.payload.size(); ++size) {
        auto truncated = wire;
        truncated.payload.resize(size);
        EXPECT_THROW(decode_heartbeat_request(truncated), std::invalid_argument);
    }
    auto malformed = wire;
    malformed.payload.push_back('x');
    EXPECT_THROW(decode_heartbeat_request(malformed), std::invalid_argument);
    // Entry count, index, term, and command length must each be validated on input.
    for (const auto offset : {47u, 55u, 63u, 71u}) {
        malformed = wire;
        malformed.payload[offset] = static_cast<char>(0xff);
        EXPECT_THROW(decode_heartbeat_request(malformed), std::invalid_argument);
    }
    EXPECT_THROW(make_heartbeat_request(1, {6, 2, 1, 3, 0, {{3, 4, "gap"}}}), std::invalid_argument);
    EXPECT_THROW(make_heartbeat_request(1, {6, 2, 1, 3, 0, {{2, 2, "old term"}}}), std::invalid_argument);
    EXPECT_THROW(make_heartbeat_request(1, {6, 2, 1, 3, 0, {{2, 7, "future term"}}}), std::invalid_argument);
    EXPECT_THROW(make_heartbeat_request(1, {6, 2, 0, 0, 0, {{1, 1, std::string(max_payload_size, 'x')}}}),
                 std::invalid_argument);
}

TEST_F(HeartbeatHandler, MatchesInteriorPredecessorAndRepairsOnlyConflictingSuffix) {
    log = {{1, 1, "one"}, {2, 2, "old"}, {3, 2, "old tail"}};
    log_state.last_log_index = 3;
    log_state.last_log_term = 2;
    const auto result = handle_heartbeat({5, 2, 1, 1, 10, {{2, 4, "new"}}},
        state, log_state, commit_state, leadership, timer, log, now);
    EXPECT_TRUE(result.response.success);
    EXPECT_EQ(result.changed_from, 2u);
    ASSERT_EQ(log.size(), 2u);
    EXPECT_EQ(log[0].command, "one");
    EXPECT_EQ(log[1].command, "new");
    EXPECT_EQ(log_state.last_log_index, 2u);
    EXPECT_EQ(log_state.last_log_term, 4u);
    EXPECT_EQ(commit_state.commit_index, 2u);
}

TEST_F(HeartbeatHandler, DuplicateAndShorterRequestsPreserveMatchingExtraSuffix) {
    log = {{1, 1, "one"}, {2, 2, "two"}, {3, 3, "three"}};
    const HeartbeatRequest request{5, 2, 1, 1, 10, {{2, 2, "two"}}};
    for (int i = 0; i < 2; ++i) {
        const auto result = handle_heartbeat(request, state, log_state, commit_state, leadership, timer, log, now);
        EXPECT_TRUE(result.response.success);
        EXPECT_FALSE(result.changed_from);
        EXPECT_EQ(log.size(), 3u);
        EXPECT_EQ(commit_state.commit_index, 2u);
    }
    const auto heartbeat = handle_heartbeat({5, 2, 0, 0, 10},
        state, log_state, commit_state, leadership, timer, log, now);
    EXPECT_TRUE(heartbeat.response.success);
    EXPECT_EQ(log.size(), 3u);
    EXPECT_EQ(commit_state.commit_index, 2u);
}

TEST_F(HeartbeatHandler, RejectsCommittedConflictsAndSameTermCommandMismatch) {
    log = {{1, 1, "one"}, {2, 2, "two"}};
    commit_state.commit_index = 2;
    EXPECT_FALSE(handle_heartbeat({5, 2, 1, 1, 2, {{2, 4, "replace"}}},
        state, log_state, commit_state, leadership, timer, log, now).response.success);
    EXPECT_EQ(log[1].command, "two");
    commit_state.commit_index = 0;
    EXPECT_FALSE(handle_heartbeat({5, 2, 1, 1, 0, {{2, 2, "different"}}},
        state, log_state, commit_state, leadership, timer, log, now).response.success);
    EXPECT_EQ(log[1].command, "two");
    EXPECT_FALSE(handle_heartbeat({5, 2, 3, 2, 0},
        state, log_state, commit_state, leadership, timer, log, now).response.success);
}

TEST(LogReplication, BacksOffThenAdvancesOnlyThroughAcknowledgedEntries) {
    std::vector<LogEntry> log{{1, 1, "one"}, {2, 2, "two"}, {3, 3, "three"}};
    LogReplication progress;
    progress.reset(3);
    auto request = progress.make_request(log, 5, 1, 0);
    EXPECT_EQ(request.prev_log_index, 3u);
    EXPECT_TRUE(request.entries.empty());
    progress.acknowledge(false);
    request = progress.make_request(log, 5, 1, 0);
    EXPECT_EQ(request.prev_log_index, 2u);
    ASSERT_EQ(request.entries.size(), 1u);
    EXPECT_EQ(request.entries[0].index, 3u);
    progress.acknowledge(false);
    progress.acknowledge(false);
    progress.acknowledge(false);
    EXPECT_EQ(progress.next_index(), 1u);
    request = progress.make_request(log, 5, 1, 0);
    EXPECT_EQ(request.entries.size(), 3u);
    progress.acknowledge(true);
    EXPECT_EQ(progress.match_index(), 3u);
    EXPECT_EQ(progress.next_index(), 4u);
}

TEST(LogReplication, SplitsBatchesByCountAndPayloadLimitAndRetriesAfterTimeout) {
    std::vector<LogEntry> log;
    for (std::uint64_t i = 1; i <= 130; ++i) log.push_back({i, 1, "command"});
    LogReplication progress;
    progress.reset(0);
    auto request = progress.make_request(log, 2, 1, 0);
    EXPECT_EQ(request.entries.size(), 128u);
    EXPECT_EQ(progress.match_index(), 0u);
    EXPECT_EQ(progress.make_request(log, 2, 1, 0).entries.size(), 128u); // Retry without acknowledgment.
    progress.acknowledge(true);
    request = progress.make_request(log, 2, 1, 0);
    EXPECT_EQ(request.entries.size(), 2u);
    log = {{1, 1, std::string(max_payload_size / 2, 'a')}, {2, 1, std::string(max_payload_size / 2, 'b')}};
    progress.reset(0);
    request = progress.make_request(log, 2, 1, 0);
    EXPECT_EQ(request.entries.size(), 1u);
    EXPECT_LE(make_heartbeat_request(1, request).payload.size(), max_payload_size);
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

TEST(HeartbeatScheduling, UsesConfiguredIntervalAndRejectsNonpositiveIntervals) {
    const ElectionTimer::Clock::time_point now{};
    HeartbeatTimer timer{120ms, now};
    EXPECT_TRUE(timer.tick(now));
    EXPECT_FALSE(timer.tick(now + 119ms));
    EXPECT_TRUE(timer.tick(now + 120ms));
    timer.restart(now + 130ms);
    EXPECT_TRUE(timer.tick(now + 130ms));
    EXPECT_FALSE(timer.tick(now + 249ms));
    EXPECT_TRUE(timer.tick(now + 250ms));
    EXPECT_THROW((HeartbeatTimer{0ms, now}), std::invalid_argument);
    EXPECT_THROW((HeartbeatTimer{-1ms, now}), std::invalid_argument);
}

TEST(HeartbeatPeerRpc, ConfiguredTimeoutAppliesToHeartbeatButNotVoteRequests) {
    zmq::context_t context{1};
    zmq::socket_t receiver{context, zmq::socket_type::rep};
    configure_socket(receiver);
    receiver.bind("inproc://configured-heartbeat-timeout");
    PeerRpc sender{context, {2, "inproc://configured-heartbeat-timeout"}, 350ms};
    const auto now = PeerRpc::Clock::now();
    sender.requestHeartbeat({1, 1, 0, 0, 0}, 1, now);
    EXPECT_FALSE(sender.expired(now + 349ms));
    EXPECT_TRUE(sender.expired(now + 350ms));
    sender.requestVoteFor({2, 1, 0, 0}, 2, now);
    EXPECT_FALSE(sender.expired(now + 99ms));
    EXPECT_TRUE(sender.expired(now + 100ms));
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
    TermVoteState state;
    LogState log_state;
    LeadershipState leadership;
    Election election{config, state, log_state, leadership};
    election.start();
    ASSERT_EQ(leadership.role, Role::Leader);
    EXPECT_FALSE(election.become_follower_if_higher_term(1));
    EXPECT_TRUE(election.become_follower_if_higher_term(2));
    EXPECT_EQ(leadership.role, Role::Follower);
    EXPECT_EQ(state.current_term, 2u);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_FALSE(leadership.leader_id.has_value());
}
} // namespace
} // namespace zmq_template::server
