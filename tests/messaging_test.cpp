#include "common/messaging.hpp"

#include <gtest/gtest.h>
#include <exception>
#include <thread>
#include <vector>

namespace zmq_template {
namespace {

void expect_message(const Message& actual, const Message& expected) {
    EXPECT_EQ(actual.type, expected.type);
    EXPECT_EQ(actual.id, expected.id);
    EXPECT_EQ(actual.payload, expected.payload);
}

TEST(RequestReplyCommunication, ExchangesTextEmptyAndBinaryMessagesInSequence) {
    const std::vector<Message> messages{
        {MessageType::text, 1, "ZeroMQ"},
        {MessageType::text, 2, ""},
        {MessageType::text, 3, std::string("a\0b", 3)},
    };
    zmq::context_t context{1};
    std::exception_ptr server_error;
    // Each socket stays on its own thread; only the context is shared.
    std::thread server([&] {
        try {
            zmq::socket_t socket{context, zmq::socket_type::rep};
            configure_socket(socket);
            socket.bind("inproc://request-reply-test");
            for (const auto& message : messages) {
                expect_message(receive_message(socket), message);
                send_message(socket, {MessageType::acknowledgement, message.id, message.payload});
            }
        } catch (...) {
            server_error = std::current_exception();
        }
    });
    // Nonfatal assertions ensure the server thread is always joined on failure.
    EXPECT_NO_THROW(([&] {
        zmq::socket_t socket{context, zmq::socket_type::req};
        configure_socket(socket);
        socket.connect("inproc://request-reply-test");
        for (const auto& message : messages) {
            send_message(socket, message);
            expect_message(receive_message(socket),
                           {MessageType::acknowledgement, message.id, message.payload});
        }
    }()));
    server.join();
    if (server_error) {
        EXPECT_NO_THROW(std::rethrow_exception(server_error));
    }
}

TEST(RoutedCommunication, RepliesToTwoReqClientsInReverseOrder) {
    zmq::context_t context{1};
    zmq::socket_t router{context, zmq::socket_type::router};
    zmq::socket_t first{context, zmq::socket_type::req};
    zmq::socket_t second{context, zmq::socket_type::req};
    configure_socket(router);
    configure_socket(first);
    configure_socket(second);
    router.bind("inproc://routed-replies");
    first.connect("inproc://routed-replies");
    second.connect("inproc://routed-replies");
    send_message(first, {MessageType::text, 1, "first"});
    send_message(second, {MessageType::text, 2, "second"});
    std::string route_a;
    std::string route_b;
    const auto a = receive_routed_message(router, route_a);
    const auto b = receive_routed_message(router, route_b);
    EXPECT_NE(route_a, route_b);
    send_routed_message(router, route_b, {MessageType::acknowledgement, b.id, b.payload});
    send_routed_message(router, route_a, {MessageType::acknowledgement, a.id, a.payload});
    expect_message(receive_message(first), {MessageType::acknowledgement, 1, "first"});
    expect_message(receive_message(second), {MessageType::acknowledgement, 2, "second"});
}

TEST(RoutedCommunication, MalformedPayloadRetainsRouteAndDoesNotBreakNextRequest) {
    zmq::context_t context{1};
    zmq::socket_t router{context, zmq::socket_type::router};
    zmq::socket_t client{context, zmq::socket_type::req};
    configure_socket(router);
    configure_socket(client);
    router.bind("inproc://routed-invalid");
    client.connect("inproc://routed-invalid");
    ASSERT_TRUE(client.send(zmq::buffer("invalid", 7)));
    std::string route;
    EXPECT_THROW(receive_routed_message(router, route), std::invalid_argument);
    EXPECT_FALSE(route.empty());
    send_routed_message(router, route, {MessageType::error, 0, "invalid"});
    EXPECT_EQ(receive_message(client).type, MessageType::error);
    send_message(client, {MessageType::text, 3, "valid"});
    EXPECT_EQ(receive_routed_message(router, route).id, 3u);
    send_routed_message(router, route, {MessageType::acknowledgement, 3, "valid"});
    EXPECT_EQ(receive_message(client).payload, "valid");
}

} // namespace
} // namespace zmq_template
