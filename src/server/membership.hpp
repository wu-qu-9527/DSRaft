#pragma once

#include "server/config/config_parser.hpp"
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace zmq_template::server {
using Voters = std::set<std::uint64_t>;

inline Voters parse_voters(std::string_view text) {
    Voters voters;
    for (;;) {
        const auto comma = text.find(',');
        if (!voters.insert(parse_node_id(text.substr(0, comma))).second) {
            throw std::invalid_argument("Duplicate voter");
        }
        if (comma == text.npos) return voters;
        text.remove_prefix(comma + 1);
    }
}

inline std::string encode_voters(const Voters& voters) {
    std::string text;
    for (const auto id : voters) {
        if (!text.empty()) text += ',';
        text += std::to_string(id);
    }
    return text;
}

struct Membership {
    Voters old_voters;
    Voters new_voters;

    bool contains(std::uint64_t id) const { return old_voters.count(id) || new_voters.count(id); }
    bool joint() const { return !new_voters.empty(); }

    template<class Predicate>
    bool quorum(Predicate confirmed) const {
        const auto majority = [&](const Voters& voters) {
            std::size_t count = 0;
            for (auto id : voters) if (confirmed(id)) ++count;
            return count > voters.size() / 2;
        };
        return majority(old_voters) && (!joint() || majority(new_voters));
    }

    std::string encode() const {
        return encode_voters(old_voters) + (joint() ? "/" + encode_voters(new_voters) : "");
    }

    static Membership decode(std::string_view text) {
        const auto split = text.find('/');
        return {parse_voters(text.substr(0, split)),
            split == text.npos ? Voters{} : parse_voters(text.substr(split + 1))};
    }
};

inline constexpr std::string_view membership_prefix{"\0MEM1", 5};
inline bool is_membership(std::string_view command) {
    return command.substr(0, membership_prefix.size()) == membership_prefix;
}
} // namespace zmq_template::server
