#include "server/rpc/vote_rpc.hpp"
#include "server/vote_handler.hpp"

#include <gtest/gtest.h>
#include <limits>

namespace zmq_template {
namespace {
using namespace server;
using namespace std::chrono_literals;

TEST(VoteRpc, RoundTripsRequestAndResponseWithFullWidthIntegers) {
    const RequestVoteRequest request{0x0102030405060708ULL, 42,
        std::numeric_limits<std::uint64_t>::max(), 9};
    const auto message = deserialize(serialize(make_vote_request(123, request)));
    EXPECT_EQ(message.id, 123u);
    EXPECT_EQ(static_cast<unsigned char>(message.payload[0]), 1);
    EXPECT_EQ(static_cast<unsigned char>(message.payload[7]), 8);
    const auto decoded = decode_vote_request(message);
    EXPECT_EQ(decoded.term, request.term);
    EXPECT_EQ(decoded.candidate_id, request.candidate_id);
    EXPECT_EQ(decoded.last_log_index, request.last_log_index);
    EXPECT_EQ(decoded.last_log_term, request.last_log_term);
    for (bool granted : {false, true}) {
        const auto reply = deserialize(serialize(make_vote_response(123, {request.term, granted})));
        EXPECT_EQ(reply.id, 123u);
        const auto result = decode_vote_response(reply);
        EXPECT_EQ(result.term, request.term);
        EXPECT_EQ(result.vote_granted, granted);
    }
}

TEST(VoteRpc, RejectsMalformedPayloadAndWrongTypes) {
    auto request = make_vote_request(1, {1, 2, 0, 0});
    request.payload.pop_back();
    EXPECT_THROW(decode_vote_request(request), std::invalid_argument);
    request.payload.append("xx");
    EXPECT_THROW(decode_vote_request(request), std::invalid_argument);
    auto reply = make_vote_response(1, {1, true});
    EXPECT_THROW(decode_vote_request(reply), std::invalid_argument);
    EXPECT_THROW(decode_vote_response(request), std::invalid_argument);
    reply.payload[8] = 2;
    EXPECT_THROW(decode_vote_response(reply), std::invalid_argument);
    reply.payload.pop_back();
    EXPECT_THROW(decode_vote_response(reply), std::invalid_argument);
}

class VoteHandler : public ::testing::Test {
protected:
    const ElectionTimer::Clock::time_point now{};
    ElectionTimer timer{200ms, 200ms, now};
    TermVoteState state{5, std::nullopt};
    LogState log_state{10, 3};
    LeadershipState leadership;
    RequestVoteResponse vote(RequestVoteRequest request) {
        return voteFor(request, state, log_state, leadership, timer, now + 100ms);
    }
};

TEST_F(VoteHandler, RejectsOlderTermWithoutChangingStateOrTimer) {
    state.voted_for = 7;
    EXPECT_FALSE(vote({4, 2, 100, 10}).vote_granted);
    EXPECT_EQ(state.current_term, 5u);
    EXPECT_EQ(state.voted_for, 7u);
    EXPECT_EQ(timer.remaining(now + 100ms), 100ms);
}

TEST_F(VoteHandler, GrantsVoteResetsTimerAndAllowsSameCandidateRetry) {
    EXPECT_TRUE(vote({5, 2, 10, 3}).vote_granted);
    EXPECT_EQ(state.voted_for, 2u);
    EXPECT_EQ(timer.remaining(now + 100ms), 200ms);
    EXPECT_TRUE(vote({5, 2, 10, 3}).vote_granted);
    EXPECT_FALSE(vote({5, 3, 10, 3}).vote_granted);
}

TEST_F(VoteHandler, HigherTermStepsDownAndClearsPreviousVote) {
    for (auto previous : {Role::Candidate, Role::Leader}) {
        state = {5, 7};
        log_state = {10, 3};
        leadership.role = previous;
        const auto result = vote({6, 2, 10, 3});
        EXPECT_TRUE(result.vote_granted);
        EXPECT_EQ(result.term, 6u);
        EXPECT_EQ(leadership.role, Role::Follower);
        EXPECT_EQ(state.voted_for, 2u);
    }
}

TEST_F(VoteHandler, RejectsStaleLogEvenWhenItsIndexIsLarger) {
    EXPECT_FALSE(vote({5, 2, 100, 2}).vote_granted);
    EXPECT_FALSE(vote({5, 2, 9, 3}).vote_granted);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_EQ(timer.remaining(now + 100ms), 100ms);
    EXPECT_TRUE(vote({5, 2, 1, 4}).vote_granted);
}

TEST_F(VoteHandler, HigherTermWithStaleLogStillUpdatesTermButDoesNotResetTimer) {
    leadership.role = Role::Leader;
    state.voted_for = 7;
    const auto result = vote({6, 2, 9, 3});
    EXPECT_FALSE(result.vote_granted);
    EXPECT_EQ(result.term, 6u);
    EXPECT_EQ(leadership.role, Role::Follower);
    EXPECT_FALSE(state.voted_for.has_value());
    EXPECT_EQ(timer.remaining(now + 100ms), 100ms);
}
} // namespace
} // namespace zmq_template
