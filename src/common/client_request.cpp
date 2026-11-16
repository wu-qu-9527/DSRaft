#include "common/client_request.hpp"
#include "common/binary_codec.hpp"
#include "common/message.hpp"

#include <stdexcept>

namespace zmq_template {
namespace {
constexpr std::string_view magic{"\0RQC1", 5};
void validate(const ClientRequest& request) {
    if (request.client_id.empty() || request.client_id.size() > 128 || request.sequence == 0
        || request.command.empty()) throw std::invalid_argument("Invalid client identity, sequence or command");
}
} // namespace

bool is_client_request(std::string_view bytes) { return !bytes.empty() && bytes.front() == '\0'; }

std::string encode_client_request(const ClientRequest& request) {
    validate(request);
    if (request.command.size() > max_payload_size - 21 - request.client_id.size()) {
        throw std::invalid_argument("Client request exceeds payload limit");
    }
    std::string bytes{magic};
    binary::append_u64(bytes, request.sequence);
    binary::append_u64(bytes, request.client_id.size());
    return bytes + request.client_id + request.command;
}

ClientRequest decode_client_request(std::string_view bytes) {
    if (bytes.size() < 21 || bytes.size() > max_payload_size || bytes.substr(0, 5) != magic) {
        throw std::invalid_argument("Invalid client request envelope");
    }
    const auto size = binary::read_u64(bytes, 13);
    if (size == 0 || size > 128 || size > bytes.size() - 21) {
        throw std::invalid_argument("Invalid client identity length");
    }
    ClientRequest request{std::string{bytes.substr(21, size)}, binary::read_u64(bytes, 5),
                          std::string{bytes.substr(21 + size)}};
    validate(request);
    return request;
}

} // namespace zmq_template
