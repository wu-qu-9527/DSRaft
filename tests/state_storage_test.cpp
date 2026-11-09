#include "server/storage/raft_state_storage.hpp"

#include "storage_test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>

namespace zmq_template::server {
namespace {
class StateStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() / "raft-state-XXXXXX").string();
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

TEST_F(StateStorageTest, MissingFileStartsEmptyAndSavedStateSurvivesReopen) {
    {
        RaftStateStorage storage{directory};
        EXPECT_EQ(storage.load_term_vote().current_term, 0u);
        EXPECT_FALSE(storage.load_term_vote().voted_for);
        storage.save_term_vote({7, 42});
    }
    RaftStateStorage reopened{directory};
    EXPECT_EQ(reopened.load_term_vote().current_term, 7u);
    ASSERT_TRUE(reopened.load_term_vote().voted_for);
    EXPECT_EQ(*reopened.load_term_vote().voted_for, 42u);
    reopened.save_term_vote({8, std::nullopt});
    EXPECT_EQ(reopened.load_term_vote().current_term, 8u);
    EXPECT_FALSE(reopened.load_term_vote().voted_for);
}

TEST_F(StateStorageTest, TermVoteAndCommitRemainIndependentAcrossUpdatesAndReopen) {
    {
        RaftStateStorage storage{directory};
        EXPECT_EQ(storage.load_commit().index, 0u);
        storage.save_term_vote({5, 2});
        storage.save_commit({3, 4});
        storage.save_term_vote({6, std::nullopt});
        EXPECT_EQ(storage.load_commit().index, 3u);
        EXPECT_EQ(storage.load_commit().term, 4u);
        storage.save_commit({4, 5});
        EXPECT_EQ(storage.load_term_vote().current_term, 6u);
        EXPECT_FALSE(storage.load_term_vote().voted_for);
    }
    RaftStateStorage recovered{directory};
    EXPECT_EQ(recovered.load_term_vote().current_term, 6u);
    EXPECT_FALSE(recovered.load_term_vote().voted_for);
    EXPECT_EQ(recovered.load_commit().index, 4u);
    EXPECT_EQ(recovered.load_commit().term, 5u);
}

TEST_F(StateStorageTest, PreservesFullWidthValuesAndRejectsZeroNodeId) {
    RaftStateStorage storage{directory};
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    storage.save_term_vote({maximum, maximum});
    EXPECT_EQ(storage.load_term_vote().current_term, maximum);
    EXPECT_EQ(storage.load_term_vote().voted_for, maximum);
    EXPECT_THROW(storage.save_term_vote({maximum, 0}), StorageError);
    EXPECT_EQ(storage.load_term_vote().voted_for, maximum);
}

TEST_F(StateStorageTest, WritesReadableJsonWithExactTermAndNullableVote) {
    RaftStateStorage storage{directory};
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    storage.save_term_vote({maximum, 42});
    std::ifstream input{directory / "state.json"};
    ASSERT_TRUE(input.good());
    const auto record = nlohmann::json::parse(input);
    EXPECT_EQ(record.at("format"), "raft-state");
    EXPECT_EQ(record.at("version"), 1);
    EXPECT_EQ(record.at("data").at("current_term").get<std::uint64_t>(), maximum);
    EXPECT_EQ(record.at("data").at("voted_for"), 42);
    storage.save_term_vote({maximum, std::nullopt});
    std::ifstream updated{directory / "state.json"};
    EXPECT_TRUE(nlohmann::json::parse(updated).at("data").at("voted_for").is_null());
}

TEST_F(StateStorageTest, RejectsConcurrentOwnerAndReleasesLockOnDestruction) {
    {
        RaftStateStorage first{directory};
        EXPECT_THROW((RaftStateStorage{directory}), StorageError);
    }
    EXPECT_NO_THROW((RaftStateStorage{directory}));
}

TEST_F(StateStorageTest, RejectsTruncatedCorruptAndUnexpectedlyLongRecords) {
    RaftStateStorage storage{directory};
    storage.save_term_vote({5, 2});
    {
        std::fstream file{directory / "state.json", std::ios::binary | std::ios::in | std::ios::out};
        file.seekp(15);
        file.put('\x04');
    }
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    storage.save_term_vote({5, 2});
    std::filesystem::resize_file(directory / "state.json", 12);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    storage.save_term_vote({5, 2});
    std::filesystem::resize_file(directory / "state.json", 33);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    storage.save_term_vote({5, 2});
    {
        std::fstream file{directory / "state.json", std::ios::binary | std::ios::in | std::ios::out};
        file.put('X');
    }
    EXPECT_THROW(storage.load_term_vote(), StorageError);
}

TEST_F(StateStorageTest, IncompleteTemporaryFileDoesNotReplaceCommittedState) {
    {
        RaftStateStorage storage{directory};
        storage.save_term_vote({5, 2});
    }
    std::ofstream{directory / "state.tmp", std::ios::binary} << "incomplete";
    RaftStateStorage reopened{directory};
    EXPECT_EQ(reopened.load_term_vote().current_term, 5u);
    EXPECT_EQ(reopened.load_term_vote().voted_for, 2u);
    reopened.save_term_vote({6, 3});
    EXPECT_EQ(reopened.load_term_vote().current_term, 6u);
    EXPECT_FALSE(std::filesystem::exists(directory / "state.tmp"));
}

TEST_F(StateStorageTest, FailedReplacementPreservesPreviouslySavedState) {
    RaftStateStorage storage{directory};
    storage.save_term_vote({5, 2});
    std::filesystem::create_directory(directory / "state.tmp");
    EXPECT_THROW(storage.save_term_vote({6, 3}), StorageError);
    EXPECT_EQ(storage.load_term_vote().current_term, 5u);
    EXPECT_EQ(storage.load_term_vote().voted_for, 2u);
}

TEST_F(StateStorageTest, RejectsInvalidJsonFieldsAndChecksum) {
    RaftStateStorage storage{directory};
    storage.save_term_vote({7, 42});
    const auto original = test::read_json(directory / "state.json");
    auto damaged = original;
    damaged["data"]["current_term"] = 8;
    std::ofstream{directory / "state.json"} << damaged.dump();
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    for (const auto& value : {nlohmann::json(-1), nlohmann::json(1.5), nlohmann::json(true),
                              nlohmann::json("7"), nlohmann::json(18446744073709551616.0)}) {
        damaged = original;
        damaged["data"]["current_term"] = value;
        test::write_json(directory / "state.json", damaged);
        EXPECT_THROW(storage.load_term_vote(), StorageError);
    }
    for (const auto& field : {"version", "format", "data"}) {
        damaged = original;
        damaged.erase(field);
        test::write_json(directory / "state.json", damaged);
        EXPECT_THROW(storage.load_term_vote(), StorageError) << field;
    }
    damaged = original;
    damaged["version"] = 2;
    test::write_json(directory / "state.json", damaged);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    damaged = original;
    damaged["data"]["voted_for"] = 0;
    test::write_json(directory / "state.json", damaged);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    damaged = original;
    damaged["format"] = "raft-commit";
    test::write_json(directory / "state.json", damaged);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
    const auto duplicate = original.dump();
    std::ofstream{directory / "state.json"} << "{\"version\":1," << duplicate.substr(1);
    EXPECT_THROW(storage.load_term_vote(), StorageError);
}

TEST_F(StateStorageTest, AcceptsReformattedJsonAndRejectsExcessiveNesting) {
    RaftStateStorage storage{directory};
    storage.save_term_vote({7, 42});
    const auto record = test::read_json(directory / "state.json");
    std::ofstream{directory / "state.json"} << record.dump(4) << "\n\n";
    EXPECT_EQ(storage.load_term_vote().current_term, 7u);
    std::ofstream{directory / "state.json"} << std::string(100, '[') << '0' << std::string(100, ']');
    EXPECT_THROW(storage.load_term_vote(), StorageError);
}

TEST_F(StateStorageTest, RejectsDataDirectoryThatIsAFile) {
    const auto file = directory / "not-a-directory";
    std::ofstream{file} << "file";
    EXPECT_THROW((RaftStateStorage{file}), StorageError);
}
} // namespace
} // namespace zmq_template::server
