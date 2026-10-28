#include "server/election_timer.hpp"

#include <gtest/gtest.h>

namespace zmq_template::server {
namespace {
using namespace std::chrono_literals;
using Clock = ElectionTimer::Clock;

TEST(ElectionTimer, StartsWithRandomTimeoutWithinConfiguredRange) {
    const auto now = Clock::time_point{};
    ElectionTimer timer{150ms, 300ms, now};
    for (int i = 0; i < 100; ++i) {
        timer.reset(now);
        EXPECT_GE(timer.timeout(), 150ms);
        EXPECT_LE(timer.timeout(), 300ms);
        EXPECT_EQ(timer.remaining(now), timer.timeout());
    }
}

TEST(ElectionTimer, ExpiresOnlyAtDeadline) {
    const auto now = Clock::time_point{};
    ElectionTimer timer{200ms, 200ms, now};
    EXPECT_FALSE(timer.expired(now + 199ms));
    EXPECT_FALSE(timer.expired(now + 200ms - 1ns));
    EXPECT_TRUE(timer.expired(now + 200ms));
}

TEST(ElectionTimer, ResetRearmsAfterExpiry) {
    const auto now = Clock::time_point{};
    ElectionTimer timer{200ms, 200ms, now};
    EXPECT_TRUE(timer.expired(now + 200ms));
    timer.reset(now + 200ms);
    EXPECT_FALSE(timer.expired(now + 399ms));
    EXPECT_TRUE(timer.expired(now + 400ms));
}

TEST(ElectionTimer, ResetPostponesExpiry) {
    const auto now = Clock::time_point{};
    ElectionTimer timer{200ms, 200ms, now};
    timer.reset(now + 100ms);
    EXPECT_FALSE(timer.expired(now + 200ms));
    EXPECT_TRUE(timer.expired(now + 300ms));
}

TEST(ElectionTimer, CheckingExpiryDoesNotResetDeadline) {
    const auto now = Clock::time_point{};
    const ElectionTimer timer{200ms, 200ms, now};
    EXPECT_TRUE(timer.expired(now + 200ms));
    EXPECT_TRUE(timer.expired(now + 500ms));
    EXPECT_EQ(timer.remaining(now + 200ms), 0ms);
}

TEST(ElectionTimer, RemainingTimeRoundsUpAndStopsAtZero) {
    const auto now = Clock::time_point{};
    ElectionTimer timer{200ms, 200ms, now};
    EXPECT_EQ(timer.remaining(now + 199ms + 500us), 1ms);
    EXPECT_EQ(timer.remaining(now + 200ms), 0ms);
    EXPECT_EQ(timer.remaining(now + 500ms), 0ms);
}

TEST(ElectionTimer, RejectsInvalidTimeoutRange) {
    EXPECT_THROW((ElectionTimer{0ms, 200ms}), std::invalid_argument);
    EXPECT_THROW((ElectionTimer{300ms, 150ms}), std::invalid_argument);
}
} // namespace
} // namespace zmq_template::server
