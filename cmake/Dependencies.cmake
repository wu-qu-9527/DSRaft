if(ZMQ_TEMPLATE_USE_SYSTEM_DEPS)
    find_package(cppzmq CONFIG REQUIRED)
    find_package(spdlog 1.9 CONFIG REQUIRED)
else()
    include(FetchContent)
    # libzmq 4.3.5 declares an old policy baseline; CMake 4 needs this override.
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 4.0 AND NOT DEFINED CMAKE_POLICY_VERSION_MINIMUM)
        set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
    endif()
    # Pin versions so a new upstream release does not change the build.
    set(BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(WITH_PERF_TOOL OFF CACHE BOOL "" FORCE)
    set(WITH_LIBSODIUM OFF CACHE BOOL "" FORCE)
    set(WITH_TLS OFF CACHE BOOL "" FORCE)
    set(ENABLE_CURVE OFF CACHE BOOL "" FORCE)
    set(ENABLE_DRAFTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(libzmq
        URL https://github.com/zeromq/libzmq/archive/refs/tags/v4.3.5.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    # Only cppzmq's headers are needed; its own examples/tests are excluded.
    FetchContent_Declare(cppzmq_headers
        URL https://github.com/zeromq/cppzmq/archive/refs/tags/v4.10.0.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SOURCE_SUBDIR unused)
    FetchContent_MakeAvailable(libzmq cppzmq_headers)
    add_library(cppzmq INTERFACE)
    target_include_directories(cppzmq SYSTEM INTERFACE "${cppzmq_headers_SOURCE_DIR}")
    target_link_libraries(cppzmq INTERFACE libzmq-static)
    set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
    set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
    set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(spdlog
        URL https://github.com/gabime/spdlog/archive/refs/tags/v1.15.3.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    FetchContent_MakeAvailable(spdlog)
endif()
