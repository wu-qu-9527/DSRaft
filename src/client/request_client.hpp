#pragma once

#include "common/client_request.hpp"
#include "common/zmq_transport.hpp"

#include <chrono>
#include <vector>

namespace zmq_template {

// Every transport retry carries exactly the same durable request identity.
Message submit_request(zmq::context_t& context, const std::vector<std::string>& endpoints,
                       const ClientRequest& request, std::chrono::milliseconds timeout,
                       std::chrono::milliseconds attempt_timeout = std::chrono::milliseconds{500});

} // namespace zmq_template
