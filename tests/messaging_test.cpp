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

} // namespace
} // namespace zmq_template
