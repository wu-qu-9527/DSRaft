#include "server/storage/raft_state_storage.hpp"
#include "server/storage/log_storage.hpp"

#include "storage_test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>

namespace zmq_template::server {
namespace {
class LogStorageTest : public ::testing::Test {
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

TEST_F(LogStorageTest, AppendsAndRecoversBinaryCommandsAndFullWidthTerms) {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const std::string binary{"a\0b\xff", 4};
    {
        auto shared = std::make_shared<StorageDirectory>(directory);
        LogStorage storage{shared};
        RaftStateStorage state{shared};
        EXPECT_TRUE(storage.load().empty());
        storage.append({{1, 2, binary}, {2, 2, ""}});
        storage.append({{3, maximum, "last"}});
        state.save_term_vote({maximum, 7});
    }
    auto shared = std::make_shared<StorageDirectory>(directory);
    LogStorage reopened{shared};
    RaftStateStorage state{shared};
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
    EXPECT_EQ(state.load_term_vote().voted_for, 7u);
}

TEST_F(LogStorageTest, SharedDirectoryStaysLockedUntilBothStoresAreDestroyed) {
    auto shared = std::make_shared<StorageDirectory>(directory);
    auto state = std::make_unique<RaftStateStorage>(shared);
    auto log = std::make_unique<LogStorage>(shared);
    shared.reset();
    state->save_term_vote({2, 7});
    log->append({{1, 2, "command"}});
    EXPECT_THROW((LogStorage{directory}), StorageError);
    state.reset();
    EXPECT_THROW((RaftStateStorage{directory}), StorageError);
    EXPECT_EQ(log->load().back().command, "command");
    log.reset();
    RaftStateStorage reopened{directory};
    EXPECT_EQ(reopened.load_term_vote().current_term, 2u);
}

TEST_F(LogStorageTest, TruncatesSuffixAndAllowsReplacementAfterRestart) {
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

TEST_F(LogStorageTest, ReplacesSuffixAtomicallyAndRejectsInvalidReplacement) {
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

TEST_F(LogStorageTest, InvalidBatchesAndTruncationLeaveOldLogUnchanged) {
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

TEST_F(LogStorageTest, RejectsCorruptionTruncationAndMalformedRecords) {
    LogStorage storage{directory};
    storage.append({{1, 1, "command"}});
    std::ifstream input{directory / "log.json", std::ios::binary};
    const std::string original{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
    const auto write = [&](const std::string& bytes) {
        std::ofstream output{directory / "log.json", std::ios::binary | std::ios::trunc};
        output.write(bytes.data(), bytes.size());
    };
    write(original.substr(0, 12));
    EXPECT_THROW(storage.load(), StorageError);
    write(original + "extra");
    EXPECT_THROW(storage.load(), StorageError);
    auto damaged = nlohmann::json::parse(original);
    damaged["data"]["entries"][0]["command"] = "changed";
    write(damaged.dump()); // Intact JSON with an invalid checksum.
    EXPECT_THROW(storage.load(), StorageError);
    const auto valid = nlohmann::json::parse(original);
    for (const auto& field : {"index", "term", "command"}) {
        damaged = valid;
        damaged["data"]["entries"][0][field] = nullptr;
        test::write_json(directory / "log.json", damaged);
        EXPECT_THROW(storage.load(), StorageError) << "field=" << field;
    }
    damaged = valid;
    damaged["data"]["entries"][0]["index"] = 2;
    test::write_json(directory / "log.json", damaged);
    EXPECT_THROW(storage.load(), StorageError);
}

TEST_F(LogStorageTest, IncompleteTemporaryFileIsIgnoredAndFailedWritesPreserveOldLog) {
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

TEST_F(LogStorageTest, PreservesTextAndBinaryCommandsInJson) {
    LogStorage storage{directory};
    const std::string binary{"a\0\xff", 3};
    storage.append({{1, 7, binary}, {2, 7, "SET greeting 你好"}, {3, 7, ""}});
    const auto record = test::read_json(directory / "log.json");
    EXPECT_EQ(record.at("format"), "raft-log");
    EXPECT_EQ(record.at("data").at("entries")[0].at("command_hex"), "6100ff");
    EXPECT_EQ(record.at("data").at("entries")[1].at("command"), "SET greeting 你好");
    EXPECT_EQ(storage.load()[0].command, binary);
    EXPECT_EQ(storage.load()[2].command, "");
    std::ofstream{directory / "log.json"} << "partial";
    EXPECT_THROW(storage.load(), StorageError);
}

TEST_F(LogStorageTest, RejectsInvalidHexAndAmbiguousCommandEncoding) {
    LogStorage storage{directory};
    storage.append({{1, 1, "command"}});
    const auto original = test::read_json(directory / "log.json");
    for (const auto* hex : {"0", "zz"}) {
        auto record = original;
        auto& entry = record["data"]["entries"][0];
        entry.erase("command");
        entry["command_hex"] = hex;
        test::write_json(directory / "log.json", record);
        EXPECT_THROW(storage.load(), StorageError);
    }
    auto record = original;
    record["data"]["entries"][0]["command_hex"] = "61";
    test::write_json(directory / "log.json", record);
    EXPECT_THROW(storage.load(), StorageError);
}

TEST_F(LogStorageTest, RoundTripsAllByteValuesAndEscapedUtf8Commands) {
    std::string binary;
    for (int byte = 0; byte < 256; ++byte) binary.push_back(static_cast<char>(byte));
    const std::string text{"SET key a\0b\n\"\\", 14};
    LogStorage storage{directory};
    storage.append({{1, 1, binary}, {2, 1, text}});
    const auto entries = storage.load();
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].command, binary);
    EXPECT_EQ(entries[1].command, text);
    EXPECT_EQ(test::read_json(directory / "log.json").at("data").at("entries")[1].at("command"), text);
}
} // namespace
} // namespace zmq_template::server
