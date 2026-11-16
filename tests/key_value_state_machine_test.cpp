#include "server/state_machine/key_value_state_machine.hpp"
#include "common/binary_codec.hpp"
#include "common/client_request.hpp"

#include <gtest/gtest.h>
#include <stdexcept>

namespace zmq_template::server {
namespace {
std::string identified(const std::string& client, std::uint64_t sequence, const std::string& command) {
    std::string bytes{"\0RQC1", 5};
    binary::append_u64(bytes, sequence);
    binary::append_u64(bytes, client.size());
    bytes += client;
    bytes += command;
    return bytes;
}

TEST(KeyValueStateMachine, DeduplicatesRequestsAndRestoresResultsByReplay) {
    const std::vector<LogEntry> log{{1, 1, identified("alice", 1, "SET key original")},
        {2, 1, identified("alice", 2, "DEL key")},
        {3, 2, identified("bob", 1, "SET key replacement")},
        {4, 2, identified("alice", 2, "DEL key")}};
    KeyValueStateMachine machine;
    auto applied = machine.apply_committed(log, 4);
    EXPECT_EQ(applied[1].result, "OK");
    EXPECT_EQ(applied[3].result, "OK");
    EXPECT_EQ(machine.get("key"), "replacement");
    KeyValueStateMachine recovered;
    EXPECT_EQ(recovered.apply_committed(log, 4)[3].result, "OK");
    EXPECT_EQ(recovered.get("key"), "replacement");
}

TEST(KeyValueStateMachine, RejectsStaleOrConflictingSessionRequests) {
    const std::vector<LogEntry> log{{1, 1, identified("alice", 2, "SET key original")},
        {2, 1, identified("alice", 2, "SET key conflict")},
        {3, 1, identified("alice", 1, "DEL key")}};
    KeyValueStateMachine machine;
    const auto applied = machine.apply_committed(log, 3);
    EXPECT_EQ(applied[1].result, "ERROR request sequence reused with different command");
    EXPECT_EQ(applied[2].result, "ERROR stale request sequence");
    EXPECT_EQ(machine.get("key"), "original");
}

TEST(ClientRequest, ValidatesVersionLengthsAndIdentityAndPreservesBinaryCommands) {
    const ClientRequest request{"alice", 7, std::string{"SET k a\0b", 9}};
    const auto bytes = encode_client_request(request);
    const auto decoded = decode_client_request(bytes);
    EXPECT_EQ(decoded.client_id, "alice");
    EXPECT_EQ(decoded.sequence, 7u);
    EXPECT_EQ(decoded.command, request.command);
    for (std::size_t size = 0; size < 26; ++size) {
        EXPECT_THROW(decode_client_request(bytes.substr(0, size)), std::invalid_argument);
    }
    auto bad = bytes;
    bad[4] = '2';
    EXPECT_THROW(decode_client_request(bad), std::invalid_argument);
    EXPECT_THROW(encode_client_request({"", 1, "GET k"}), std::invalid_argument);
    EXPECT_THROW(encode_client_request({"alice", 0, "GET k"}), std::invalid_argument);
}

TEST(KeyValueStateMachine, AppliesOnlyCommittedPrefixInOrderAndSkipsAlreadyAppliedEntries) {
    const std::vector<LogEntry> log{{1, 1, "SET key one"}, {2, 1, "GET key"},
                                  {3, 2, "SET key two"}, {4, 2, "DEL key"}};
    KeyValueStateMachine machine;
    const auto first = machine.apply_committed(log, 2);
    ASSERT_EQ(first.size(), 2u);
    EXPECT_EQ(first[0].result, "OK");
    EXPECT_EQ(first[1].result, "VALUE one");
    EXPECT_EQ(machine.get("key"), "one");
    EXPECT_EQ(machine.last_applied(), 2u);
    EXPECT_TRUE(machine.apply_committed(log, 2).empty());
    EXPECT_EQ(machine.apply_committed(log, 3)[0].result, "OK");
    EXPECT_EQ(machine.get("key"), "two");
    machine.apply_committed(log, 4);
    EXPECT_FALSE(machine.get("key"));
    EXPECT_THROW(machine.apply_committed(log, 3), std::logic_error);
    EXPECT_THROW(machine.apply_committed(log, 5), std::logic_error);
}

TEST(KeyValueStateMachine, ReplaysDeterministicallyAndHandlesBinaryValuesNoopsAndInvalidCommands) {
    const std::string binary{"SET key a\0b", 11};
    const std::vector<LogEntry> log{{1, 1, ""}, {2, 1, binary}, {3, 1, "GET key"},
                                  {4, 1, "unknown"}, {5, 1, "GET missing"}};
    KeyValueStateMachine first;
    KeyValueStateMachine replay;
    const auto results = first.apply_committed(log, 5);
    const auto recovered = replay.apply_committed(log, 5);
    EXPECT_EQ(first.last_applied(), 5u);
    EXPECT_TRUE(results[0].result.empty());
    EXPECT_EQ(results[2].result, (std::string{"VALUE a\0b", 9}));
    EXPECT_EQ(results[3].result, "ERROR invalid command");
    EXPECT_EQ(results[4].result, "NOT_FOUND");
    ASSERT_EQ(results.size(), recovered.size());
    for (std::size_t i = 0; i < results.size(); ++i) EXPECT_EQ(results[i].result, recovered[i].result);
    EXPECT_EQ(first.get("key"), replay.get("key"));
    EXPECT_THROW(KeyValueStateMachine::validate_command(""), std::invalid_argument);
    EXPECT_THROW(KeyValueStateMachine::validate_command("SET key"), std::invalid_argument);
    EXPECT_THROW(KeyValueStateMachine::validate_command("GET two keys"), std::invalid_argument);
    EXPECT_NO_THROW(KeyValueStateMachine::validate_command("SET key "));
}
} // namespace
} // namespace zmq_template::server
