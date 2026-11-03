#include "server/commit_index.hpp"

#include <gtest/gtest.h>

namespace zmq_template::server {
namespace {
TEST(CommitIndex, CountsSelfAndRequiresStrictMajority) {
    const std::vector<LogEntry> log{{1, 1, "old"}, {2, 2, "current"}, {3, 2, "latest"}};
    EXPECT_EQ(majority_commit_index(log, 2, 0, {0, 0}), 0u);
    EXPECT_EQ(majority_commit_index(log, 2, 0, {2, 0}), 2u);
    EXPECT_EQ(majority_commit_index(log, 2, 0, {3, 2}), 3u);
    EXPECT_EQ(majority_commit_index(log, 2, 0, {3, 0, 0}), 0u); // Four members need three copies.
    EXPECT_EQ(majority_commit_index(log, 2, 0, {3, 2, 0}), 2u);
}

TEST(CommitIndex, DoesNotDirectlyCommitOlderTermsOrMoveBackwards) {
    const std::vector<LogEntry> log{{1, 1, "old"}, {2, 2, "current"}};
    EXPECT_EQ(majority_commit_index(log, 2, 0, {1, 1}), 0u);
    EXPECT_EQ(majority_commit_index(log, 3, 0, {2, 2}), 0u);
    EXPECT_EQ(majority_commit_index(log, 2, 2, {0, 0}), 2u);
    EXPECT_EQ(majority_commit_index(log, 2, 0, {2, 0}), 2u); // Also commits the preceding old entry.
}

TEST(CommitIndex, SingleNodeCommitsOnlyCurrentTermEntries) {
    EXPECT_EQ(majority_commit_index({}, 1, 0, {}), 0u);
    EXPECT_EQ(majority_commit_index({{1, 1, "old"}}, 2, 0, {}), 0u);
    EXPECT_EQ(majority_commit_index({{1, 1, "old"}, {2, 2, ""}}, 2, 0, {}), 2u);
}
} // namespace
} // namespace zmq_template::server
