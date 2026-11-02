#include "common/messaging.hpp"
#include "server/rpc/heartbeat_rpc.hpp"
#include "server/rpc/vote_rpc.hpp"
#include "server/storage/persistent_state.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace zmq_template::server {
namespace {
using namespace std::chrono_literals;

class ServerProcess {
public:
    explicit ServerProcess(std::vector<std::string> arguments, const std::filesystem::path& log) {
        arguments.insert(arguments.begin(), ZMQ_SERVER_PATH);
        std::vector<char*> argv;
        for (auto& value : arguments) argv.push_back(value.data());
        argv.push_back(nullptr);
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);
        const int result = posix_spawn(&pid_, ZMQ_SERVER_PATH, &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        if (result != 0) throw std::runtime_error("Could not start test server");
    }
    ~ServerProcess() { stop(); }
    void stop() {
        if (pid_ <= 0) return;
        ::kill(pid_, SIGKILL);
        int status{};
        while (::waitpid(pid_, &status, 0) == -1 && errno == EINTR) {}
        pid_ = -1;
    }
    int wait_for_exit() {
        for (int i = 0; i < 300; ++i) {
            int status{};
            if (::waitpid(pid_, &status, WNOHANG) == pid_) {
                pid_ = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            }
            std::this_thread::sleep_for(10ms);
        }
        throw std::runtime_error("Test server did not exit");
    }
private:
    pid_t pid_{-1};
};

class PersistentServer : public ::testing::Test {
protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() / "raft-recovery-XXXXXX").string();
        const auto* path = ::mkdtemp(pattern.data());
        ASSERT_NE(path, nullptr);
        root = path;
        data_dir = root / "data";
        endpoint = "ipc://" + (root / "server.sock").string();
    }
    void TearDown() override {
        process.reset();
        std::error_code error;
        std::filesystem::remove_all(root, error);
    }
    void start(bool fast = false, const std::string& peer = {}) {
        process.reset();
        std::vector<std::string> arguments{endpoint, "--node-id", "1", "--data-dir", data_dir.string(),
            "--heartbeat-interval-ms", "10", "--election-timeout-min-ms", fast ? "100" : "60000",
            "--election-timeout-max-ms", fast ? "100" : "60000"};
        if (!peer.empty()) arguments.insert(arguments.end(), {"--peer", "2=" + peer});
        process = std::make_unique<ServerProcess>(std::move(arguments), root / "server.log");
    }
    Message call(const Message& request) {
        zmq::context_t context{1};
        zmq::socket_t socket{context, zmq::socket_type::req};
        configure_socket(socket);
        socket.set(zmq::sockopt::linger, 0);
        socket.set(zmq::sockopt::rcvtimeo, 2000);
        socket.connect(endpoint);
        send_message(socket, request);
        return receive_message(socket);
    }
    std::filesystem::path root;
    std::filesystem::path data_dir;
    std::string endpoint;
    std::unique_ptr<ServerProcess> process;
};

TEST_F(PersistentServer, VoteAndHigherHeartbeatTermSurviveAbruptRestart) {
    start();
    EXPECT_TRUE(decode_vote_response(call(make_vote_request(1, {5, 2, 0, 0}))).vote_granted);
    start(); // SIGKILL the old process, then recover the same directory.
    const auto rejected = decode_vote_response(call(make_vote_request(2, {5, 3, 0, 0})));
    EXPECT_EQ(rejected.term, 5u);
    EXPECT_FALSE(rejected.vote_granted);
    EXPECT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(3, {7, 2, 0, 0, 0}))).success);
    start();
    const auto old_term = decode_vote_response(call(make_vote_request(4, {6, 3, 0, 0})));
    EXPECT_EQ(old_term.term, 7u);
    EXPECT_FALSE(old_term.vote_granted);
    EXPECT_TRUE(decode_vote_response(call(make_vote_request(5, {7, 3, 0, 0}))).vote_granted);
}

TEST_F(PersistentServer, SelfVoteIsPersistedBeforeRestart) {
    start(true);
    for (int i = 0; i < 200 && !std::filesystem::exists(data_dir / "state.bin"); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(std::filesystem::exists(data_dir / "state.bin"));
    process.reset();
    {
        StateStorage storage{data_dir};
        EXPECT_EQ(storage.load().current_term, 1u);
        EXPECT_EQ(storage.load().voted_for, 1u);
    }
    start();
    const auto response = decode_vote_response(call(make_vote_request(1, {1, 2, 0, 0})));
    EXPECT_EQ(response.term, 1u);
    EXPECT_FALSE(response.vote_granted);
}

TEST_F(PersistentServer, FailedVotePersistenceExitsWithoutSuccessfulReply) {
    start();
    ASSERT_TRUE(decode_vote_response(call(make_vote_request(1, {5, 2, 0, 0}))).vote_granted);
    std::filesystem::create_directory(data_dir / "state.tmp");
    zmq::context_t context{1};
    zmq::socket_t socket{context, zmq::socket_type::req};
    configure_socket(socket);
    socket.set(zmq::sockopt::linger, 0);
    socket.connect(endpoint);
    send_message(socket, make_vote_request(2, {6, 3, 0, 0}));
    zmq::pollitem_t item{socket.handle(), 0, ZMQ_POLLIN, 0};
    EXPECT_EQ(zmq::poll(&item, 1, 500ms), 0);
    EXPECT_EQ(process->wait_for_exit(), 1);
    StateStorage storage{data_dir};
    EXPECT_EQ(storage.load().current_term, 5u);
    EXPECT_EQ(storage.load().voted_for, 2u);
}

TEST_F(PersistentServer, StorageFailureOnHigherTermResponseIsNotSwallowedAsPeerError) {
    zmq::context_t context{1};
    zmq::socket_t peer{context, zmq::socket_type::rep};
    configure_socket(peer);
    peer.set(zmq::sockopt::linger, 0);
    const auto peer_endpoint = "ipc://" + (root / "peer.sock").string();
    peer.bind(peer_endpoint);
    start(true, peer_endpoint);
    const auto request = receive_message(peer);
    EXPECT_EQ(decode_vote_request(request).term, 1u);
    std::filesystem::create_directory(data_dir / "state.tmp");
    send_message(peer, make_vote_response(request.id, {9, false}));
    EXPECT_EQ(process->wait_for_exit(), 1);
    StateStorage storage{data_dir};
    EXPECT_EQ(storage.load().current_term, 1u);
    EXPECT_EQ(storage.load().voted_for, 1u);
}

TEST_F(PersistentServer, CorruptStateRefusesStartupBeforeListening) {
    {
        StateStorage storage{data_dir};
        storage.save({5, 2});
    }
    std::ofstream{data_dir / "state.bin", std::ios::binary | std::ios::trunc} << "partial";
    start();
    EXPECT_EQ(process->wait_for_exit(), 1);
    EXPECT_FALSE(std::filesystem::exists(root / "server.sock"));
}
} // namespace
} // namespace zmq_template::server
