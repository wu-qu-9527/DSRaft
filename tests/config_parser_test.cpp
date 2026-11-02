#include "server/config_parser.hpp"

#include <gtest/gtest.h>

#include <initializer_list>
#include <stdexcept>
#include <string>
#include <vector>

namespace zmq_template::server {
namespace {
using namespace std::chrono_literals;

NodeConfig parse(std::initializer_list<std::string> options) {
    std::vector<std::string> arguments{"zmq_server"};
    arguments.insert(arguments.end(), options.begin(), options.end());
    std::vector<char*> argv;
    for (auto& argument : arguments) argv.push_back(argument.data());
    return parse_node_config(static_cast<int>(argv.size()), argv.data());
}

TEST(ConfigParser, KeepsExistingTimingDefaults) {
    const auto config = parse({});
    EXPECT_EQ(config.heartbeat_interval, 50ms);
    EXPECT_EQ(config.heartbeat_timeout, 100ms);
    EXPECT_EQ(config.election_timeout_min, 150ms);
    EXPECT_EQ(config.election_timeout_max, 300ms);
}

TEST(ConfigParser, ParsesTimingOptionsAlongsideNodeAndPeers) {
    const auto config = parse({"tcp://127.0.0.1:6000", "--node-id", "7",
        "--heartbeat-interval-ms", "80", "--heartbeat-timeout-ms", "240",
        "--election-timeout-min-ms", "400", "--election-timeout-max-ms", "800",
        "--peer", "8=tcp://127.0.0.1:6001", "--peer", "9=tcp://127.0.0.1:6002"});
    EXPECT_EQ(config.id, 7u);
    EXPECT_EQ(config.endpoint, "tcp://127.0.0.1:6000");
    ASSERT_EQ(config.peers.size(), 2u);
    EXPECT_EQ(config.peers[1].id, 9u);
    EXPECT_EQ(config.heartbeat_interval, 80ms);
    EXPECT_EQ(config.heartbeat_timeout, 240ms);
    EXPECT_EQ(config.election_timeout_min, 400ms);
    EXPECT_EQ(config.election_timeout_max, 800ms);
}

TEST(ConfigParser, RejectsInvalidOrRepeatedTimingValues) {
    for (const auto* option : {"--heartbeat-interval-ms", "--heartbeat-timeout-ms",
                              "--election-timeout-min-ms", "--election-timeout-max-ms"}) {
        SCOPED_TRACE(option);
        for (const auto* value : {"0", "-1", "1.5", "abc", "100ms", "", "2147483648"}) {
            SCOPED_TRACE(value);
            EXPECT_THROW(parse({option, value}), std::invalid_argument);
        }
        EXPECT_THROW(parse({option}), std::invalid_argument);
        EXPECT_THROW(parse({option, "100", option, "200"}), std::invalid_argument);
    }
}

TEST(ConfigParser, RejectsInvertedRangeAndHeartbeatIntervalAtOrAboveMinimum) {
    EXPECT_THROW(parse({"--election-timeout-min-ms", "400"}), std::invalid_argument);
    EXPECT_THROW(parse({"--heartbeat-interval-ms", "150"}), std::invalid_argument);
    EXPECT_THROW(parse({"--heartbeat-interval-ms", "200"}), std::invalid_argument);
    EXPECT_THROW(parse({"--election-timeout-min-ms", "40"}), std::invalid_argument);
}

TEST(ConfigParser, ValidatesCombinedSettingsAfterAllOptionsAreParsed) {
    const auto config = parse({"--heartbeat-interval-ms", "300",
        "--election-timeout-max-ms", "600", "--election-timeout-min-ms", "600"});
    EXPECT_EQ(config.heartbeat_interval, 300ms);
    EXPECT_EQ(config.election_timeout_min, 600ms);
    EXPECT_EQ(config.election_timeout_max, 600ms);
}

} // namespace
} // namespace zmq_template::server
