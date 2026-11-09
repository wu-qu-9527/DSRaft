#include "server/storage/raft_state_storage.hpp"

#include "storage_test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <limits>

namespace zmq_template::server {
namespace {
class CommitStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() / "raft-commit-XXXXXX").string();
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

TEST_F(CommitStorageTest, RecoversMonotonicCheckpointAndRejectsInvalidTransitions) {
    {
        auto shared = std::make_shared<StorageDirectory>(directory);
        RaftStateStorage storage{shared};
        EXPECT_EQ(storage.load_commit().index, 0u);
        storage.save_commit({2, 3});
        EXPECT_THROW(storage.save_commit({1, 3}), StorageError);
        EXPECT_THROW(storage.save_commit({2, 4}), StorageError);
        EXPECT_THROW(storage.save_commit({3, 2}), StorageError);
        EXPECT_THROW(storage.save_commit({0, 1}), StorageError);
    }
    RaftStateStorage recovered{std::make_shared<StorageDirectory>(directory)};
    EXPECT_EQ(recovered.load_commit().index, 2u);
    EXPECT_EQ(recovered.load_commit().term, 3u);
    recovered.save_commit({4, 5});
    EXPECT_EQ(recovered.load_commit().index, 4u);
}

TEST_F(CommitStorageTest, FailedWritePreservesOldCheckpointAndCorruptionIsFatal) {
    RaftStateStorage storage{std::make_shared<StorageDirectory>(directory)};
    storage.save_commit({2, 3});
    std::filesystem::create_directory(directory / "commit.tmp");
    EXPECT_THROW(storage.save_commit({3, 4}), StorageError);
    EXPECT_EQ(storage.load_commit().index, 2u);
    std::ofstream{directory / "commit.json", std::ios::binary | std::ios::trunc} << "partial";
    EXPECT_THROW(storage.load_commit(), StorageError);
    EXPECT_THROW(storage.save_commit({3, 4}), StorageError);
}

TEST_F(CommitStorageTest, WritesFullWidthJsonCheckpoint) {
    RaftStateStorage storage{std::make_shared<StorageDirectory>(directory)};
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    storage.save_commit({maximum, maximum});
    const auto record = test::read_json(directory / "commit.json");
    EXPECT_EQ(record.at("format"), "raft-commit");
    EXPECT_EQ(record.at("data").at("index").get<std::uint64_t>(), maximum);
    EXPECT_EQ(storage.load_commit().index, maximum);
    EXPECT_EQ(storage.load_commit().term, maximum);
    std::ofstream{directory / "commit.json"} << "partial";
    EXPECT_THROW(storage.load_commit(), StorageError);
}

TEST_F(CommitStorageTest, RejectsInvalidJsonCheckpoint) {
    RaftStateStorage storage{std::make_shared<StorageDirectory>(directory)};
    storage.save_commit({2, 3});
    auto record = test::read_json(directory / "commit.json");
    record["data"]["term"] = 0;
    test::write_json(directory / "commit.json", record);
    EXPECT_THROW(storage.load_commit(), StorageError);
    EXPECT_THROW(storage.save_commit({3, 4}), StorageError);
}
} // namespace
} // namespace zmq_template::server
