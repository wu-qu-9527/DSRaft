#include "server/storage/persistent_state.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>

namespace zmq_template::server {
namespace {
class PersistentStateStorage : public ::testing::Test {
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

TEST_F(PersistentStateStorage, MissingFileStartsEmptyAndSavedStateSurvivesReopen) {
    {
        StateStorage storage{directory};
        EXPECT_EQ(storage.load().current_term, 0u);
        EXPECT_FALSE(storage.load().voted_for);
        storage.save({7, 42});
    }
    StateStorage reopened{directory};
    EXPECT_EQ(reopened.load().current_term, 7u);
    ASSERT_TRUE(reopened.load().voted_for);
    EXPECT_EQ(*reopened.load().voted_for, 42u);
    reopened.save({8, std::nullopt});
    EXPECT_EQ(reopened.load().current_term, 8u);
    EXPECT_FALSE(reopened.load().voted_for);
}

TEST_F(PersistentStateStorage, PreservesFullWidthValuesAndRejectsZeroNodeId) {
    StateStorage storage{directory};
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    storage.save({maximum, maximum});
    EXPECT_EQ(storage.load().current_term, maximum);
    EXPECT_EQ(storage.load().voted_for, maximum);
    EXPECT_THROW(storage.save({maximum, 0}), StorageError);
    EXPECT_EQ(storage.load().voted_for, maximum);
}

TEST_F(PersistentStateStorage, RejectsConcurrentOwnerAndReleasesLockOnDestruction) {
    {
        StateStorage first{directory};
        EXPECT_THROW((StateStorage{directory}), StorageError);
    }
    EXPECT_NO_THROW((StateStorage{directory}));
}

TEST_F(PersistentStateStorage, RejectsTruncatedCorruptAndUnexpectedlyLongRecords) {
    StateStorage storage{directory};
    storage.save({5, 2});
    {
        std::fstream file{directory / "state.bin", std::ios::binary | std::ios::in | std::ios::out};
        file.seekp(15);
        file.put('\x04');
    }
    EXPECT_THROW(storage.load(), StorageError);
    storage.save({5, 2});
    std::filesystem::resize_file(directory / "state.bin", 12);
    EXPECT_THROW(storage.load(), StorageError);
    storage.save({5, 2});
    std::filesystem::resize_file(directory / "state.bin", 33);
    EXPECT_THROW(storage.load(), StorageError);
    storage.save({5, 2});
    {
        std::fstream file{directory / "state.bin", std::ios::binary | std::ios::in | std::ios::out};
        file.put('X');
    }
    EXPECT_THROW(storage.load(), StorageError);
}

TEST_F(PersistentStateStorage, IncompleteTemporaryFileDoesNotReplaceCommittedState) {
    {
        StateStorage storage{directory};
        storage.save({5, 2});
    }
    std::ofstream{directory / "state.tmp", std::ios::binary} << "incomplete";
    StateStorage reopened{directory};
    EXPECT_EQ(reopened.load().current_term, 5u);
    EXPECT_EQ(reopened.load().voted_for, 2u);
    reopened.save({6, 3});
    EXPECT_EQ(reopened.load().current_term, 6u);
    EXPECT_FALSE(std::filesystem::exists(directory / "state.tmp"));
}

TEST_F(PersistentStateStorage, FailedReplacementPreservesPreviouslySavedState) {
    StateStorage storage{directory};
    storage.save({5, 2});
    std::filesystem::create_directory(directory / "state.tmp");
    EXPECT_THROW(storage.save({6, 3}), StorageError);
    EXPECT_EQ(storage.load().current_term, 5u);
    EXPECT_EQ(storage.load().voted_for, 2u);
}

TEST_F(PersistentStateStorage, RejectsDataDirectoryThatIsAFile) {
    const auto file = directory / "not-a-directory";
    std::ofstream{file} << "file";
    EXPECT_THROW((StateStorage{file}), StorageError);
}
} // namespace
} // namespace zmq_template::server
