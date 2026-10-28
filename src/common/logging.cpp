#include "common/logging.hpp"

#include <spdlog/cfg/env.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace zmq_template {

void init_logging(const std::string& name) {
    auto logger = spdlog::stderr_color_mt(name);
    spdlog::set_default_logger(logger);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [pid %P] [%^%l%$] %v");
    spdlog::set_level(spdlog::level::info);
    spdlog::cfg::load_env_levels();
    spdlog::flush_on(spdlog::level::info);
}

} // namespace zmq_template
