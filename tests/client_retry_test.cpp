#include "client/request_client.hpp"
#include "common/zmq_transport.hpp"

#include <gtest/gtest.h>
#include <future>

namespace zmq_template {
namespace {
using namespace std::chrono_literals;

TEST(ClientRetry, LostResponseRetriesTheSameIdentityAndCommand) {
    zmq::context_t context{1};
    zmq::socket_t receiver{context, zmq::socket_type::router};
    configure_socket(receiver);
    receiver.bind("inproc://retry-lost-response");
    auto future = std::async(std::launch::async, [&] {
        return submit_request(context, {"inproc://retry-lost-response"}, {"alice", 7, "DEL key"}, 2s, 100ms);
    });
    std::string first_route, second_route;
    const auto first = receive_routed_message(receiver, first_route); // Deliberately lose the response.
    const auto second = receive_routed_message(receiver, second_route);
    EXPECT_EQ(first.type, MessageType::client_request);
    EXPECT_EQ(first.payload, second.payload);
    const auto decoded = decode_client_request(second.payload);
    EXPECT_EQ(decoded.client_id, "alice");
    EXPECT_EQ(decoded.sequence, 7u);
    send_routed_message(receiver, second_route, {MessageType::acknowledgement, second.id, "OK"});
    EXPECT_EQ(future.get().payload, "OK");
}

TEST(ClientRetry, FindsLeaderAfterFollowerResponseAndBoundsUnavailableCluster) {
    zmq::context_t context{1};
    zmq::socket_t follower{context, zmq::socket_type::rep};
    zmq::socket_t leader{context, zmq::socket_type::rep};
    configure_socket(follower);
    configure_socket(leader);
    follower.bind("inproc://retry-follower");
    leader.bind("inproc://retry-leader");
    auto future = std::async(std::launch::async, [&] {
        return submit_request(context, {"inproc://retry-follower", "inproc://retry-leader"},
                              {"alice", 1, "GET key"}, 2s, 100ms);
    });
    const auto rejected = receive_message(follower);
    send_message(follower, {MessageType::error, rejected.id, "Not leader; known leader ID=2"});
    const auto accepted = receive_message(leader);
    EXPECT_EQ(rejected.payload, accepted.payload);
    send_message(leader, {MessageType::acknowledgement, accepted.id, "VALUE hello"});
    EXPECT_EQ(future.get().payload, "VALUE hello");
    EXPECT_THROW(submit_request(context, {"inproc://retry-missing"}, {"alice", 2, "GET key"}, 50ms, 20ms),
                 std::runtime_error);
}
} // namespace
} // namespace zmq_template
