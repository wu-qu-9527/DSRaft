#include "common/client_request.hpp"
#include "server/storage/log_storage.hpp"
#include "server/state_machine/key_value_state_machine.hpp"
#include "server/log_replication.hpp"
#include "server/commit_index.hpp"
#include "server/heartbeat.hpp"
#include "server/rpc/snapshot_rpc.hpp"

#include <gtest/gtest.h>
#include <cstdlib>

namespace zmq_template::server {
namespace {
TEST(Snapshot, RestoresBinaryStateAndDeduplicationAtAbsoluteIndex) {
    KeyValueStateMachine original;
    const std::vector<LogEntry> prefix{{1, 1, encode_client_request({"alice", 1, "SET key original"})},
        {2, 1, encode_client_request({"bob", 1, "SET key newer"})},
        {3, 1, std::string{"SET bin a\0b", 11}}};
    original.apply_committed(prefix, 3);
    KeyValueStateMachine restored;
    restored.restore(original.snapshot(), 3);
    const std::vector<LogEntry> suffix{{4, 2, encode_client_request({"alice", 1, "SET key original"})}};
    EXPECT_EQ(restored.apply_committed(suffix, 4, 3)[0].result, "OK");
    EXPECT_EQ(restored.get("key"), "newer");
    EXPECT_EQ(restored.get("bin"), (std::string{"a\0b", 3}));
    EXPECT_EQ(restored.last_applied(), 4u);
    EXPECT_THROW(restored.restore("bad", 2), std::invalid_argument);
    EXPECT_EQ(restored.get("key"), "newer");
}

TEST(Snapshot, AtomicallyCompactsAndRetainsOnlyMatchingSuffix) {
    std::string pattern = "/tmp/raft-snapshot-XXXXXX";
    ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
    {
        LogStorage storage{pattern};
        storage.append({{1, 1, "one"}, {2, 2, "two"}, {3, 2, "three"}});
        storage.install_snapshot({2, 2, std::string{"a\0b", 3}, {}});
        EXPECT_EQ(storage.load_snapshot().index, 2u);
        ASSERT_EQ(storage.load().size(), 1u);
        EXPECT_EQ(storage.load()[0].index, 3u);
        storage.append({{4, 3, "four"}});
        EXPECT_THROW(storage.replace_suffix(2, {}), StorageError);
        EXPECT_THROW(storage.install_snapshot({1, 1, "old", {}}), StorageError);
        std::filesystem::create_directory(std::filesystem::path{pattern} / "log.tmp");
        EXPECT_THROW(storage.install_snapshot({3, 2, "failed", {}}), StorageError);
        EXPECT_EQ(storage.load_snapshot().index, 2u);
        EXPECT_EQ(storage.load().back().index, 4u);
        std::filesystem::remove(std::filesystem::path{pattern} / "log.tmp");
    }
    {
        LogStorage reopened{pattern};
        EXPECT_EQ(reopened.load_snapshot().state, (std::string{"a\0b", 3}));
        EXPECT_EQ(reopened.load().back().index, 4u);
        reopened.install_snapshot({3, 4, "new state", {}});
        EXPECT_TRUE(reopened.load().empty());
        reopened.append({{4, 4, "replacement"}});
    }
    std::filesystem::remove_all(pattern);
}

TEST(Snapshot, ReplicatesAndCommitsWithCompactedAbsoluteIndices) {
    const std::vector<LogEntry> log{{6, 3, "six"}, {7, 4, "seven"}};
    LogReplication replication;
    replication.reset(5);
    const auto request = replication.make_request(log, 4, 1, 5, 5, 3);
    EXPECT_EQ(request.prev_log_index, 5u);
    EXPECT_EQ(request.prev_log_term, 3u);
    EXPECT_EQ(request.entries.size(), 2u);
    replication.acknowledge(true);
    EXPECT_EQ(replication.match_index(), 7u);
    EXPECT_EQ(majority_commit_index(log, 4, 5, {7, 0}, 5), 7u);
}

TEST(SnapshotRpc, RejectsMalformedChunksAndRoundTripsBinaryData) {
    const SnapshotRequest request{5, 1, 10, 4, 2, 6, std::string{"a\0bc", 4}};
    const auto encoded = make_snapshot_request(9, request);
    const auto decoded = decode_snapshot_request(encoded);
    EXPECT_EQ(decoded.data, request.data);
    EXPECT_EQ(decoded.offset, 2u);
    EXPECT_THROW(make_snapshot_request(1, {5, 1, 10, 6, 0, 1, "x"}), std::invalid_argument);
    EXPECT_THROW(make_snapshot_request(1, {5, 1, 10, 4, 1, 1, "x"}), std::invalid_argument);
    EXPECT_THROW(make_snapshot_request(1, {5, 0, 10, 4, 0, 1, "x"}), std::invalid_argument);
    auto truncated = encoded;
    truncated.payload.resize(47);
    EXPECT_THROW(decode_snapshot_request(truncated), std::invalid_argument);
}
} // namespace
} // namespace zmq_template::server
