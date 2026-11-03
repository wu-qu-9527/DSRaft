#include "server/storage/persistent_state.hpp"
#include "server/storage/log_storage.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>

namespace zmq_template::server {
namespace {
class PersistentLogStorage : public ::testing::Test {
protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() / "raft-log-XXXXXX").string();
        const auto* path = ::mkdtemp(pattern.data());
        ASSERT_NE(path, nullptr);
        directory = path;
    }
    void TearDown() override {
        std::error_code error;
        std::filesystem::remove_all(directory, error);
    }
    std::filesystem::path directory;
};

TEST_F(PersistentLogStorage, AppendsAndRecoversBinaryCommandsAndFullWidthTerms) {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const std::string binary{"a\0b\xff", 4};
    {
        auto shared = std::make_shared<StorageDirectory>(directory);
        LogStorage storage{shared};
        StateStorage state{shared};
        EXPECT_TRUE(storage.load().empty());
        storage.append({{1, 2, binary}, {2, 2, ""}});
        storage.append({{3, maximum, "last"}});
        state.save({maximum, 7});
    }
    auto shared = std::make_shared<StorageDirectory>(directory);
    LogStorage reopened{shared};
    StateStorage state{shared};
    const auto log = reopened.load();
    ASSERT_EQ(log.size(), 3u);
    EXPECT_EQ(log[0].index, 1u);
    EXPECT_EQ(log[0].term, 2u);
    EXPECT_EQ(log[0].command, binary);
    EXPECT_EQ(log[1].index, 2u);
    EXPECT_TRUE(log[1].command.empty());
    EXPECT_EQ(log[2].index, 3u);
    EXPECT_EQ(log[2].term, maximum);
    EXPECT_EQ(log[2].command, "last");
    EXPECT_EQ(state.load().voted_for, 7u);
}

TEST_F(PersistentLogStorage, SharedDirectoryStaysLockedUntilBothStoresAreDestroyed) {
    auto shared = std::make_shared<StorageDirectory>(directory);
    auto state = std::make_unique<StateStorage>(shared);
    auto log = std::make_unique<LogStorage>(shared);
    shared.reset();
    state->save({2, 7});
    log->append({{1, 2, "command"}});
    EXPECT_THROW((LogStorage{directory}), StorageError);
    state.reset();
    EXPECT_THROW((StateStorage{directory}), StorageError);
    EXPECT_EQ(log->load().back().command, "command");
    log.reset();
    StateStorage reopened{directory};
    EXPECT_EQ(reopened.load().current_term, 2u);
}

TEST_F(PersistentLogStorage, TruncatesSuffixAndAllowsReplacementAfterRestart) {
    {
        LogStorage storage{directory};
        storage.append({{1, 1, "one"}, {2, 2, "old"}, {3, 2, "old"}});
        storage.truncate_suffix(2);
    }
    {
        LogStorage reopened{directory};
        ASSERT_EQ(reopened.load().size(), 1u);
        reopened.append({{2, 3, "replacement"}});
        reopened.truncate_suffix(3); // One past the end is a no-op.
        EXPECT_EQ(reopened.load().back().command, "replacement");
        reopened.truncate_suffix(1);
    }
    LogStorage reopened{directory};
    EXPECT_TRUE(reopened.load().empty());
    EXPECT_NO_THROW(reopened.truncate_suffix(1));
    EXPECT_NO_THROW(reopened.append({}));
}

TEST_F(PersistentLogStorage, ReplacesSuffixAtomicallyAndRejectsInvalidReplacement) {
    LogStorage log{directory};
    log.append({{1, 1, "prefix"}, {2, 2, "old"}, {3, 2, "tail"}});
    EXPECT_THROW(log.replace_suffix(2, {{3, 3, "gap"}}), StorageError);
    EXPECT_EQ(log.load().size(), 3u);
    std::filesystem::create_directory(directory / "log.tmp");
    EXPECT_THROW(log.replace_suffix(2, {{2, 3, "new"}}), StorageError);
    EXPECT_EQ(log.load().size(), 3u);
    EXPECT_EQ(log.load()[1].command, "old");
    std::filesystem::remove(directory / "log.tmp");
    log.replace_suffix(2, {{2, 3, "new"}});
    const auto entries = log.load();
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].command, "prefix");
    EXPECT_EQ(entries[1].command, "new");
}

TEST_F(PersistentLogStorage, InvalidBatchesAndTruncationLeaveOldLogUnchanged) {
    LogStorage storage{directory};
    storage.append({{1, 3, "old"}});
    EXPECT_THROW(storage.append({{2, 3, "valid"}, {4, 3, "gap"}}), StorageError);
    EXPECT_THROW(storage.append({{1, 3, "duplicate"}}), StorageError);
    EXPECT_THROW(storage.append({{0, 3, "zero index"}}), StorageError);
    EXPECT_THROW(storage.append({{2, 0, "zero term"}}), StorageError);
    EXPECT_THROW(storage.append({{2, 2, "older term"}}), StorageError);
    EXPECT_THROW(storage.truncate_suffix(0), StorageError);
    EXPECT_THROW(storage.truncate_suffix(3), StorageError);
    const auto log = storage.load();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_EQ(log[0].command, "old");
}

TEST_F(PersistentLogStorage, RejectsCorruptionTruncationAndMalformedRecords) {
    LogStorage storage{directory};
    storage.append({{1, 1, "command"}});
    std::ifstream input{directory / "log.bin", std::ios::binary};
    const std::string original{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
    const auto write = [&](const std::string& bytes) {
        std::ofstream output{directory / "log.bin", std::ios::binary | std::ios::trunc};
        output.write(bytes.data(), bytes.size());
    };
    write(original.substr(0, 12));
    EXPECT_THROW(storage.load(), StorageError);
    write(original + "extra");
    EXPECT_THROW(storage.load(), StorageError);
    auto damaged = original;
    damaged[40] ^= 1;
    write(damaged);
    EXPECT_THROW(storage.load(), StorageError);
    // Recompute checksums so structural validation is exercised independently.
    for (const auto offset : {0u, 15u, 23u, 31u, 39u}) {
        damaged = original;
        damaged[offset] = offset == 31 ? 0 : static_cast<char>(0xff);
        std::uint64_t hash = 14695981039346656037ULL;
        for (std::size_t i = 0; i < damaged.size() - 8; ++i) {
            hash ^= static_cast<unsigned char>(damaged[i]);
            hash *= 1099511628211ULL;
        }
        for (std::size_t i = 0; i < 8; ++i) {
            damaged[damaged.size() - 8 + i] = static_cast<char>(hash >> (56 - 8 * i));
        }
        write(damaged);
        EXPECT_THROW(storage.load(), StorageError) << "offset=" << offset;
    }
}

TEST_F(PersistentLogStorage, IncompleteTemporaryFileIsIgnoredAndFailedWritesPreserveOldLog) {
    {
        LogStorage storage{directory};
        storage.append({{1, 1, "old"}});
    }
    std::ofstream{directory / "log.tmp", std::ios::binary} << "partial";
    LogStorage reopened{directory};
    EXPECT_EQ(reopened.load().back().command, "old");
    reopened.append({{2, 2, "new"}});
    EXPECT_FALSE(std::filesystem::exists(directory / "log.tmp"));
    std::filesystem::create_directory(directory / "log.tmp");
    EXPECT_THROW(reopened.append({{3, 3, "failed"}}), StorageError);
    EXPECT_THROW(reopened.truncate_suffix(1), StorageError);
    EXPECT_EQ(reopened.load().size(), 2u);
    EXPECT_EQ(reopened.load().back().command, "new");
}
} // namespace
} // namespace zmq_template::server
