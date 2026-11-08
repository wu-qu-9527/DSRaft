#include "server/state_machine/key_value_state_machine.hpp"

#include <gtest/gtest.h>
#include <stdexcept>

namespace zmq_template::server {
namespace {
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
