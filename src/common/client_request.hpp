#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace zmq_template {

struct ClientRequest {
    std::string client_id;
    std::uint64_t sequence;
    std::string command;
};

// Versioned log command; disjoint from the legacy text command grammar.
bool is_client_request(std::string_view bytes);
std::string encode_client_request(const ClientRequest& request);
ClientRequest decode_client_request(std::string_view bytes);

} // namespace zmq_template
