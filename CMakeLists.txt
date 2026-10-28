cmake_minimum_required(VERSION 3.24)
project(zmq_template VERSION 0.1.0 LANGUAGES CXX)

option(ZMQ_TEMPLATE_USE_SYSTEM_DEPS "Use installed cppzmq, ZeroMQ, spdlog and GoogleTest" OFF)
include(cmake/Dependencies.cmake)
include(CTest)

add_library(zmq_messaging src/common/messaging.cpp src/common/message.cpp)
add_library(zmq_template::messaging ALIAS zmq_messaging)
target_compile_features(zmq_messaging PUBLIC cxx_std_17)
set_target_properties(zmq_messaging PROPERTIES CXX_EXTENSIONS OFF)
target_include_directories(zmq_messaging PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(zmq_messaging PUBLIC cppzmq)
if(MSVC)
    target_compile_options(zmq_messaging PRIVATE /W4 /permissive-)
else()
    target_compile_options(zmq_messaging PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_library(zmq_logging src/common/logging.cpp)
add_library(zmq_template::logging ALIAS zmq_logging)
target_compile_features(zmq_logging PUBLIC cxx_std_17)
set_target_properties(zmq_logging PROPERTIES CXX_EXTENSIONS OFF)
target_include_directories(zmq_logging PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(zmq_logging PUBLIC spdlog::spdlog)

add_library(zmq_server_core
    src/server/node_config.cpp
    src/server/rpc/vote_rpc.cpp
    src/server/rpc/heartbeat_rpc.cpp)
add_library(zmq_template::server_core ALIAS zmq_server_core)
target_link_libraries(zmq_server_core PUBLIC zmq_template::messaging)
set_target_properties(zmq_server_core PROPERTIES CXX_EXTENSIONS OFF)
if(MSVC)
    target_compile_options(zmq_server_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(zmq_server_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

foreach(app IN ITEMS server client)
    add_executable(zmq_${app} src/${app}/main.cpp)
    target_link_libraries(zmq_${app} PRIVATE zmq_template::messaging zmq_template::logging)
endforeach()
target_link_libraries(zmq_server PRIVATE zmq_template::server_core)

if(BUILD_TESTING)
    include(cmake/Testing.cmake)
    find_package(Threads REQUIRED)
    add_executable(zmq_tests tests/message_test.cpp tests/messaging_test.cpp tests/election_timer_test.cpp tests/vote_rpc_test.cpp tests/node_election_test.cpp tests/heartbeat_test.cpp)
    target_link_libraries(zmq_tests PRIVATE
        zmq_template::server_core GTest::gtest_main Threads::Threads)
    include(GoogleTest)
    gtest_discover_tests(zmq_tests PROPERTIES TIMEOUT 20)
endif()
