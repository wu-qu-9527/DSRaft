#pragma once

#include "server/node_config.hpp"

#include <string_view>

namespace zmq_template::server {

std::uint64_t parse_node_id(std::string_view text);
NodeConfig parse_node_config(int argc, char* argv[]);

} // namespace zmq_template::server
