#pragma once

#include "server/state.hpp"

#include <chrono>
#include <random>
#include <stdexcept>

namespace zmq_template::server {

class ElectionTimer {
public:
    using Clock = std::chrono::steady_clock;
    using Milliseconds = std::chrono::milliseconds;

    explicit ElectionTimer(Milliseconds minimum = Milliseconds{150},
                           Milliseconds maximum = Milliseconds{300},
                           Clock::time_point now = Clock::now())
        : random_(std::random_device{}()) {
        if (minimum.count() <= 0 || maximum < minimum) {
            throw std::invalid_argument("Invalid election timeout range");
        }
        distribution_ = decltype(distribution_){minimum.count(), maximum.count()};
        reset(now);
    }

    // Call when starting an election, granting a vote, or accepting leader contact.
    // Ordinary client requests must not reset this timer.
    void reset(Clock::time_point now = Clock::now()) {
        timeout_ = Milliseconds{distribution_(random_)};
        deadline_ = now + timeout_;
    }

    Milliseconds remaining(Clock::time_point now = Clock::now()) const {
        if (now >= deadline_) return Milliseconds{0};
        return std::chrono::ceil<Milliseconds>(deadline_ - now);
    }

    Milliseconds timeout() const noexcept { return timeout_; }

    // The node starts its next election round after this returns true.
    bool tick(Role& current_role, Clock::time_point now = Clock::now()) {
        if (current_role == Role::Leader || now < deadline_) return false;
        current_role = Role::Candidate;
        reset(now);
        return true;
    }

private:
    std::mt19937 random_;
    std::uniform_int_distribution<Milliseconds::rep> distribution_;
    Milliseconds timeout_{};
    Clock::time_point deadline_{};
};

} // namespace zmq_template::server
