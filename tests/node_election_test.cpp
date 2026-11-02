#include "server/election.hpp"
#include "server/rpc/peer_rpc.hpp"
#include "server/vote_handler.hpp"

#include <gtest/gtest.h>

namespace zmq_template::server {
namespace {

class NodeElection : public ::testing::Test {
protected:
    NodeConfig config{1, "inproc://node-1", {{2, "inproc://node-2"}, {3, "inproc://node-3"}}};
    VoteState state;
    Role role{Role::Follower};
    Election election{config, state, role};
};

TEST_F(NodeElection, StartsNewTermAndVotesForSelfOnEveryRound) {
    const auto request = election.start();
    EXPECT_EQ(request.term, 1u);
    EXPECT_EQ(request.candidate_id, 1u);
    EXPECT_EQ(role, Role::Candidate);
    EXPECT_EQ(state.voted_for, 1u);
    EXPECT_EQ(election.votes(), 1u);
    EXPECT_EQ(election.start().term, 2u);
    EXPECT_EQ(state.voted_for, 1u);
}

TEST_F(NodeElection, WinsWithMajorityAndIgnoresStaleOrUnknownReplies) {
    election.start();
    election.handle_vote_response(2, 0, {0, true});
    election.handle_vote_response(99, 1, {1, true});
    election.handle_vote_response(1, 1, {1, true});
    election.handle_vote_response(3, 1, {1, false});
    EXPECT_EQ(role, Role::Candidate);
    EXPECT_EQ(election.votes(), 1u);
    election.handle_vote_response(2, 1, {1, true});
    EXPECT_EQ(role, Role::Leader);
    EXPECT_EQ(election.votes(), 2u);
}

TEST_F(NodeElection, HigherTermResponseStepsLeaderDown) {
    election.start();
    election.handle_vote_response(2, 1, {1, true});
    ASSERT_EQ(role, Role::Leader);
    election.handle_vote_response(3, 1, {5, false});
    EXPECT_EQ(role, Role::Follower);
    EXPECT_EQ(state.current_term, 5u);
    EXPECT_FALSE(state.voted_for.has_value());
}

TEST(NodeElectionCounting, DuplicateVotesDoNotFormMajority) {
    NodeConfig config{1, "inproc://one", {{2, "inproc://two"}, {3, "inproc://three"},
                                           {4, "inproc://four"}, {5, "inproc://five"}}};
    VoteState state;
    Role role{Role::Follower};
    Election election{config, state, role};
    election.start();
    election.handle_vote_response(2, 1, {1, true});
    election.handle_vote_response(2, 1, {1, true});
    EXPECT_EQ(election.votes(), 2u);
    EXPECT_EQ(role, Role::Candidate);
    election.handle_vote_response(3, 1, {1, true});
    EXPECT_EQ(role, Role::Leader);
}

TEST(NodeElectionCounting, SingleNodeElectsItself) {
    NodeConfig config;
    VoteState state;
    Role role{Role::Follower};
    Election election{config, state, role};
    election.start();
    EXPECT_EQ(role, Role::Leader);
}

TEST(PeerVotingRpc, ServerNodesRequestAndRespondWithoutBusinessClient) {
    zmq::context_t context{1};
    zmq::socket_t receiver{context, zmq::socket_type::rep};
    configure_socket(receiver);
    receiver.bind("inproc://peer-vote");
    PeerRpc sender{context, {2, "inproc://peer-vote"}};
    sender.requestVoteFor({1, 1, 0, 0}, 42);
    const auto incoming = receive_message(receiver);
    VoteState voter;
    Role role = Role::Follower;
    ElectionTimer timer;
    const auto result = voteFor(decode_vote_request(incoming), voter, role, timer);
    send_message(receiver, make_vote_response(incoming.id, result));
    zmq::pollitem_t item{sender.handle(), 0, ZMQ_POLLIN, 0};
    ASSERT_GT(zmq::poll(&item, 1, std::chrono::milliseconds{1000}), 0);
    const auto response = sender.receive_vote_response();
    EXPECT_TRUE(response.vote_granted);
    EXPECT_EQ(response.term, 1u);
    EXPECT_FALSE(sender.pending());
    EXPECT_EQ(voter.voted_for, 1u);
}

TEST(PeerVotingRpc, TimeoutDoesNotBlockNextElectionRequest) {
    zmq::context_t context{1};
    zmq::socket_t receiver{context, zmq::socket_type::rep};
    configure_socket(receiver);
    receiver.bind("inproc://peer-retry");
    PeerRpc sender{context, {2, "inproc://peer-retry"}};
    const auto now = PeerRpc::Clock::now();
    sender.requestVoteFor({1, 1, 0, 0}, 1, now);
    const auto first = receive_message(receiver);
    EXPECT_FALSE(sender.expired(now + std::chrono::milliseconds{99}));
    EXPECT_TRUE(sender.expired(now + std::chrono::milliseconds{100}));
    sender.cancel();
    send_message(receiver, make_vote_response(first.id, {1, true}));
    sender.requestVoteFor({2, 1, 0, 0}, 2);
    const auto second = receive_message(receiver);
    EXPECT_EQ(decode_vote_request(second).term, 2u);
    send_message(receiver, make_vote_response(second.id, {2, true}));
    zmq::pollitem_t item{sender.handle(), 0, ZMQ_POLLIN, 0};
    ASSERT_GT(zmq::poll(&item, 1, std::chrono::milliseconds{1000}), 0);
    EXPECT_EQ(sender.receive_vote_response().term, 2u);
}

} // namespace
} // namespace zmq_template::server
