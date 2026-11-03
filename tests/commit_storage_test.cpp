#include "server/storage/commit_storage.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>

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
        CommitStorage storage{shared};
        EXPECT_EQ(storage.load().index, 0u);
        storage.save({2, 3});
        EXPECT_THROW(storage.save({1, 3}), StorageError);
        EXPECT_THROW(storage.save({2, 4}), StorageError);
        EXPECT_THROW(storage.save({3, 2}), StorageError);
        EXPECT_THROW(storage.save({0, 1}), StorageError);
    }
    CommitStorage recovered{std::make_shared<StorageDirectory>(directory)};
    EXPECT_EQ(recovered.load().index, 2u);
    EXPECT_EQ(recovered.load().term, 3u);
    recovered.save({4, 5});
    EXPECT_EQ(recovered.load().index, 4u);
}

TEST_F(CommitStorageTest, FailedWritePreservesOldCheckpointAndCorruptionIsFatal) {
    CommitStorage storage{std::make_shared<StorageDirectory>(directory)};
    storage.save({2, 3});
    std::filesystem::create_directory(directory / "commit.tmp");
    EXPECT_THROW(storage.save({3, 4}), StorageError);
    EXPECT_EQ(storage.load().index, 2u);
    std::ofstream{directory / "commit.bin", std::ios::binary | std::ios::trunc} << "partial";
    EXPECT_THROW(storage.load(), StorageError);
    EXPECT_THROW(storage.save({3, 4}), StorageError);
}
} // namespace
} // namespace zmq_template::server
