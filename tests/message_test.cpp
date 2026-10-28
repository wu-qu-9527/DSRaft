#include "common/message.hpp"

#include <gtest/gtest.h>
#include <stdexcept>

namespace zmq_template {
namespace {

TEST(MessageSerialization, UsesNetworkByteOrder) {
    const Message sample{MessageType::text, 0x01020304, "hi"};
    const std::string expected("ZMQ1\0\0\0\1\1\2\3\4\0\0\0\2hi", 18);
    EXPECT_EQ(serialize(sample), expected);
}

TEST(MessageSerialization, RoundTripsTextEmptyAndBinaryPayloads) {
    for (const auto& payload : {std::string("ZeroMQ"), std::string(), std::string("a\0b", 3)}) {
        SCOPED_TRACE("Payload size: " + std::to_string(payload.size()));
        const Message original{MessageType::acknowledgement, 42, payload};
        const auto decoded = deserialize(serialize(original));
        EXPECT_EQ(decoded.type, original.type);
        EXPECT_EQ(decoded.id, original.id);
        EXPECT_EQ(decoded.payload, original.payload);
    }
}

TEST(MessageSerialization, RejectsEmptyAndTruncatedHeaders) {
    EXPECT_THROW(deserialize(""), std::invalid_argument);
    const auto encoded = serialize({MessageType::text, 1, "hi"});
    EXPECT_THROW(deserialize(encoded.substr(0, 15)), std::invalid_argument);
}

TEST(MessageSerialization, RejectsTruncatedAndTrailingPayloadBytes) {
    const auto encoded = serialize({MessageType::text, 1, "hi"});
    EXPECT_THROW(deserialize(encoded.substr(0, encoded.size() - 1)), std::invalid_argument);
    EXPECT_THROW(deserialize(encoded + "x"), std::invalid_argument);
}

TEST(MessageSerialization, RejectsInvalidMagic) {
    auto encoded = serialize({MessageType::text, 1, "hi"});
    encoded[0] = 'X';
    EXPECT_THROW(deserialize(encoded), std::invalid_argument);
}

TEST(MessageSerialization, RejectsUnknownMessageTypes) {
    auto encoded = serialize({MessageType::text, 1, "hi"});
    encoded[7] = 99;
    EXPECT_THROW(deserialize(encoded), std::invalid_argument);
    EXPECT_THROW(serialize((Message{static_cast<MessageType>(99), 1, "hi"})),
                 std::invalid_argument);
}

TEST(MessageSerialization, RejectsOversizedPayloadLength) {
    auto encoded = serialize({MessageType::text, 1, "hi"});
    encoded[12] = 1;
    EXPECT_THROW(deserialize(encoded), std::invalid_argument);
    EXPECT_THROW(serialize((Message{MessageType::text, 1, std::string(max_payload_size + 1, 'x')})),
                 std::invalid_argument);
}

} // namespace
} // namespace zmq_template
