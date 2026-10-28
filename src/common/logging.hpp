#pragma once

#include <string>
#include <spdlog/spdlog.h>

namespace zmq_template {

// Call once at startup. Diagnostics go to stderr, leaving stdout for results.
// SPDLOG_LEVEL controls the runtime level (default: info).
void init_logging(const std::string& name);

} // namespace zmq_template
