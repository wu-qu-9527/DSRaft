#include "common/zmq_transport.hpp"
#include "server/rpc/heartbeat_rpc.hpp"
#include "server/rpc/vote_rpc.hpp"
#include "server/storage/raft_state_storage.hpp"
#include "server/storage/log_storage.hpp"
#include "server/state_machine/key_value_state_machine.hpp"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iterator>
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

TEST_F(PersistentServer, SingleNodeBusinessCommandsCommitAndRecoverState) {
    start(true);
    for (int i = 0; i < 200 && !std::filesystem::exists(data_dir / "commit.json"); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_EQ(call({MessageType::text, 1, "SET key value"}).payload, "OK");
    EXPECT_EQ(call({MessageType::text, 2, "GET key"}).payload, "VALUE value");
    start(true);
    for (int i = 0; i < 200; ++i) {
        const auto response = call({MessageType::text, 3, "GET key"});
        if (response.type == MessageType::acknowledgement) {
            EXPECT_EQ(response.payload, "VALUE value");
            return;
        }
        std::this_thread::sleep_for(10ms);
    }
    FAIL() << "Server did not recover leadership";
}

TEST_F(PersistentServer, RecoveryReplaysOnlyCommittedPrefix) {
    start();
    ASSERT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(1,
        {5, 2, 0, 0, 1, {{1, 4, "SET key committed"}, {2, 4, "SET key uncommitted"}}}))).success);
    process.reset();
    auto directory = std::make_shared<StorageDirectory>(data_dir);
    RaftStateStorage commit{directory};
    LogStorage log{directory};
    KeyValueStateMachine machine;
    machine.apply_committed(log.load(), commit.load_commit().index);
    EXPECT_EQ(machine.last_applied(), 1u);
    EXPECT_EQ(machine.get("key"), "committed");
}

TEST_F(PersistentServer, SingleNodeCommitsAnElectionNoop) {
    start(true);
    for (int i = 0; i < 200 && !std::filesystem::exists(data_dir / "commit.json"); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(std::filesystem::exists(data_dir / "commit.json"));
    process.reset();
    auto directory = std::make_shared<StorageDirectory>(data_dir);
    LogStorage log{directory};
    RaftStateStorage commit{directory};
    ASSERT_EQ(log.load().size(), 1u);
    EXPECT_EQ(log.load()[0].term, 1u);
    EXPECT_TRUE(log.load()[0].command.empty());
    EXPECT_EQ(commit.load_commit().index, 1u);
}

TEST_F(PersistentServer, DoesNotCommitUntilPeerConfirmsCurrentTermNoop) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage state{directory};
        LogStorage log{directory};
        state.save_term_vote({1, std::nullopt});
        log.append({{1, 1, "SET key old"}});
    }
    zmq::context_t context{1};
    zmq::socket_t peer{context, zmq::socket_type::rep};
    configure_socket(peer);
    peer.set(zmq::sockopt::linger, 0);
    const auto address = "ipc://" + (root / "commit-peer.sock").string();
    peer.bind(address);
    start(true, address);
    auto incoming = receive_message(peer);
    ASSERT_EQ(incoming.type, MessageType::request_vote);
    send_message(peer, make_vote_response(incoming.id, {2, true}));
    incoming = receive_message(peer);
    auto request = decode_heartbeat_request(incoming);
    EXPECT_EQ(request.leader_commit, 0u);
    ASSERT_EQ(request.entries.size(), 1u);
    EXPECT_EQ(request.entries.back().term, 2u);
    EXPECT_TRUE(request.entries.back().command.empty());
    EXPECT_FALSE(std::filesystem::exists(data_dir / "commit.json"));
    send_message(peer, make_heartbeat_response(incoming.id, {2, true}));
    incoming = receive_message(peer);
    EXPECT_EQ(decode_heartbeat_request(incoming).leader_commit, 2u);
    send_message(peer, make_heartbeat_response(incoming.id, {2, true}));
    process.reset();
    RaftStateStorage commit{std::make_shared<StorageDirectory>(data_dir)};
    EXPECT_EQ(commit.load_commit().index, 2u);
}

TEST_F(PersistentServer, RestoresCommittedBoundaryAndRejectsConflictingCommittedEntries) {
    start();
    EXPECT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(1,
        {5, 2, 0, 0, 1, {{1, 4, "SET key committed"}, {2, 4, "SET key uncommitted"}}}))).success);
    start();
    EXPECT_FALSE(decode_heartbeat_response(call(make_heartbeat_request(2,
        {6, 2, 0, 0, 1, {{1, 6, "SET key overwritten"}}}))).success);
    process.reset();
    auto directory = std::make_shared<StorageDirectory>(data_dir);
    RaftStateStorage commit{directory};
    LogStorage log{directory};
    EXPECT_EQ(commit.load_commit().index, 1u);
    EXPECT_EQ(log.load()[0].command, "SET key committed");
}

TEST_F(PersistentServer, CommitPersistenceFailureDoesNotAcknowledgeSuccess) {
    start();
    EXPECT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(1,
        {5, 2, 0, 0, 0, {{1, 4, "SET key value"}}}))).success);
    std::filesystem::create_directory(data_dir / "commit.tmp");
    EXPECT_THROW(call(make_heartbeat_request(2, {5, 2, 1, 4, 1})), std::runtime_error);
    EXPECT_EQ(process->wait_for_exit(), 1);
    RaftStateStorage commit{std::make_shared<StorageDirectory>(data_dir)};
    EXPECT_EQ(commit.load_commit().index, 0u);
}

TEST_F(PersistentServer, CheckpointBeyondLogRefusesStartup) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage state{directory};
        state.save_term_vote({3, std::nullopt});
        state.save_commit({1, 3});
    }
    start();
    EXPECT_EQ(process->wait_for_exit(), 1);
}

TEST_F(PersistentServer, AppendEntriesRepairsConflictAndSurvivesAbruptRestart) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage state{directory};
        LogStorage log{directory};
        state.save_term_vote({3, std::nullopt});
        log.append({{1, 1, "prefix"}, {2, 2, "conflict"}, {3, 2, "old tail"}});
    }
    start();
    const HeartbeatRequest request{5, 2, 1, 1, 0, {{2, 4, std::string{"a\0b", 3}}}};
    EXPECT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(1, request))).success);
    // A duplicate requires no disk write, even if the temporary path is unavailable.
    std::filesystem::create_directory(data_dir / "log.tmp");
    EXPECT_TRUE(decode_heartbeat_response(call(make_heartbeat_request(2, request))).success);
    start();
    EXPECT_FALSE(decode_vote_response(call(make_vote_request(3, {6, 3, 1, 4}))).vote_granted);
    process.reset();
    auto directory = std::make_shared<StorageDirectory>(data_dir);
    RaftStateStorage state{directory};
    LogStorage log{directory};
    EXPECT_EQ(state.load_term_vote().current_term, 6u);
    const auto entries = log.load();
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].command, "prefix");
    EXPECT_EQ(entries[1].term, 4u);
    EXPECT_EQ(entries[1].command, (std::string{"a\0b", 3}));
}

TEST_F(PersistentServer, FailedLogReplacementDoesNotReplySuccessOrLosePreviousLog) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage state{directory};
        LogStorage log{directory};
        state.save_term_vote({3, std::nullopt});
        log.append({{1, 2, "old"}});
    }
    std::filesystem::create_directory(data_dir / "log.tmp");
    start();
    EXPECT_THROW(call(make_heartbeat_request(1, {5, 2, 0, 0, 0, {{1, 4, "new"}}})), std::runtime_error);
    EXPECT_EQ(process->wait_for_exit(), 1);
    auto directory = std::make_shared<StorageDirectory>(data_dir);
    RaftStateStorage state{directory};
    LogStorage log{directory};
    EXPECT_EQ(state.load_term_vote().current_term, 5u);
    EXPECT_EQ(log.load().back().command, "old");
}

TEST_F(PersistentServer, LeaderRetriesAndCopiesMultipleBatchesToDivergentFollower) {
    const auto follower_data = root / "follower";
    const auto follower_endpoint = "ipc://" + (root / "follower.sock").string();
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage state{directory};
        LogStorage log{directory};
        state.save_term_vote({3, std::nullopt});
        std::vector<LogEntry> entries{{1, 1, "prefix"}};
        for (std::uint64_t i = 2; i <= 131; ++i) entries.push_back({i, 2, "command-" + std::to_string(i)});
        log.append(entries);
    }
    {
        auto directory = std::make_shared<StorageDirectory>(follower_data);
        RaftStateStorage state{directory};
        LogStorage log{directory};
        state.save_term_vote({3, std::nullopt});
        log.append({{1, 1, "prefix"}, {2, 1, "conflict"}, {3, 1, "old"}});
    }
    auto follower = std::make_unique<ServerProcess>(std::vector<std::string>{follower_endpoint,
        "--node-id", "2", "--data-dir", follower_data.string(),
        "--election-timeout-min-ms", "60000", "--election-timeout-max-ms", "60000"}, root / "follower.log");
    start(true, follower_endpoint);
    const auto contents = [](const std::filesystem::path& path) {
        std::ifstream file{path, std::ios::binary};
        return std::string{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
    };
    const auto expected = contents(data_dir / "log.json");
    bool replicated = false;
    for (int i = 0; i < 500; ++i) {
        const auto copied = contents(follower_data / "log.json");
        if (copied.size() > expected.size() && copied.find("command-131") != std::string::npos) {
            replicated = true;
            break;
        }
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(replicated);
    follower.reset();
    LogStorage recovered{follower_data};
    const auto entries = recovered.load();
    ASSERT_EQ(entries.size(), 132u);
    EXPECT_EQ(entries[0].command, "prefix");
    for (std::uint64_t i = 2; i <= 131; ++i) {
        EXPECT_EQ(entries[i - 1].term, 2u);
        EXPECT_EQ(entries[i - 1].command, "command-" + std::to_string(i));
    }
    EXPECT_TRUE(entries.back().command.empty());
    EXPECT_GE(entries.back().term, 4u);
}

TEST_F(PersistentServer, RecoversLogMetadataBeforeVoting) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage storage{directory};
        LogStorage log{directory};
        storage.save_term_vote({5, std::nullopt});
        log.append({{1, 3, "first"}, {2, 5, "second"}});
    }
    start();
    EXPECT_FALSE(decode_vote_response(call(make_vote_request(1, {6, 2, 1, 5}))).vote_granted);
    EXPECT_TRUE(decode_vote_response(call(make_vote_request(2, {6, 2, 2, 5}))).vote_granted);
}

TEST_F(PersistentServer, CorruptLogRefusesStartupBeforeListening) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage storage{directory};
        LogStorage log{directory};
        storage.save_term_vote({5, std::nullopt});
        log.append({{1, 5, "command"}});
    }
    std::ofstream{data_dir / "log.json", std::ios::binary | std::ios::trunc} << "partial";
    start();
    EXPECT_EQ(process->wait_for_exit(), 1);
}

TEST_F(PersistentServer, LogTermBeyondPersistentCurrentTermRefusesStartup) {
    {
        auto directory = std::make_shared<StorageDirectory>(data_dir);
        RaftStateStorage storage{directory};
        LogStorage log{directory};
        storage.save_term_vote({4, std::nullopt});
        log.append({{1, 5, "command"}});
    }
    start();
    EXPECT_EQ(process->wait_for_exit(), 1);
}

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
    for (int i = 0; i < 200 && !std::filesystem::exists(data_dir / "state.json"); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(std::filesystem::exists(data_dir / "state.json"));
    process.reset();
    {
        RaftStateStorage storage{data_dir};
        EXPECT_EQ(storage.load_term_vote().current_term, 1u);
        EXPECT_EQ(storage.load_term_vote().voted_for, 1u);
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
    RaftStateStorage storage{data_dir};
    EXPECT_EQ(storage.load_term_vote().current_term, 5u);
    EXPECT_EQ(storage.load_term_vote().voted_for, 2u);
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
    RaftStateStorage storage{data_dir};
    EXPECT_EQ(storage.load_term_vote().current_term, 1u);
    EXPECT_EQ(storage.load_term_vote().voted_for, 1u);
}

TEST_F(PersistentServer, CorruptStateRefusesStartupBeforeListening) {
    {
        RaftStateStorage storage{data_dir};
        storage.save_term_vote({5, 2});
    }
    std::ofstream{data_dir / "state.json", std::ios::binary | std::ios::trunc} << "partial";
    start();
    EXPECT_EQ(process->wait_for_exit(), 1);
    EXPECT_FALSE(std::filesystem::exists(root / "server.sock"));
}
class CommittedCluster : public PersistentServer {
protected:
    void SetUp() override {
        PersistentServer::SetUp();
        nodes.resize(3);
        for (int i = 0; i < 3; ++i) {
            directories.push_back(root / ("node-" + std::to_string(i + 1)));
            endpoints.push_back("ipc://" + (root / ("node-" + std::to_string(i + 1) + ".sock")).string());
        }
        for (int i = 0; i < 3; ++i) start_node(i);
    }
    void TearDown() override {
        nodes.clear();
        PersistentServer::TearDown();
    }
    void start_node(int i) {
        std::vector<std::string> args{endpoints[i], "--node-id", std::to_string(i + 1),
            "--data-dir", directories[i].string(), "--heartbeat-interval-ms", "10",
            "--election-timeout-min-ms", "200", "--election-timeout-max-ms", "400"};
        for (int j = 0; j < 3; ++j) {
            if (i != j) args.insert(args.end(), {"--peer", std::to_string(j + 1) + "=" + endpoints[j]});
        }
        nodes[i] = std::make_unique<ServerProcess>(std::move(args), root / ("cluster-" + std::to_string(i) + ".log"));
    }
    Message command(int i, const std::string& text, int timeout = 3000) {
        zmq::context_t context{1};
        zmq::socket_t client{context, zmq::socket_type::req};
        configure_socket(client);
        client.set(zmq::sockopt::linger, 0);
        client.set(zmq::sockopt::rcvtimeo, timeout);
        client.connect(endpoints[i]);
        send_message(client, {MessageType::text, 42, text});
        return receive_message(client);
    }
    int find_leader() {
        for (int attempt = 0; attempt < 40; ++attempt) {
            for (int i = 0; i < 3; ++i) {
                if (!nodes[i]) continue;
                try {
                    if (command(i, "GET probe", 500).type == MessageType::acknowledgement) return i;
                } catch (const std::runtime_error&) {}
            }
            std::this_thread::sleep_for(20ms);
        }
        throw std::runtime_error("Cluster did not elect an available leader");
    }
    static std::uint64_t read_index(const std::filesystem::path& path) {
        std::ifstream input{path};
        if (!input) return 0;
        const auto data = nlohmann::json::parse(input).at("data");
        if (path.stem() == "state") return data.at("current_term").get<std::uint64_t>();
        if (path.stem() == "log") return data.at("entries").size();
        return data.at("index").get<std::uint64_t>();
    }
    bool wait_for_commit(int i, std::uint64_t index) {
        for (int attempt = 0; attempt < 300; ++attempt) {
            if (read_index(directories[i] / "commit.json") >= index) return true;
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }
    std::vector<std::unique_ptr<ServerProcess>> nodes;
    std::vector<std::filesystem::path> directories;
    std::vector<std::string> endpoints;
};

TEST_F(CommittedCluster, CommitsWithOneFollowerDownAndPreservesStateAcrossLeaderChange) {
    const int original = find_leader();
    ASSERT_EQ(command(original, "SET key first").payload, "OK");
    const int follower = (original + 1) % 3;
    nodes[follower].reset();
    ASSERT_EQ(command(original, "SET key majority").payload, "OK");
    const auto committed = read_index(directories[original] / "commit.json");
    start_node(follower);
    ASSERT_TRUE(wait_for_commit(follower, committed));
    nodes[original].reset();
    const int successor = find_leader();
    ASSERT_NE(successor, original);
    ASSERT_EQ(command(successor, "GET key").payload, "VALUE majority");
    ASSERT_EQ(command(successor, "SET key successor").payload, "OK");
    start_node(original);
    const auto latest = read_index(directories[successor] / "commit.json");
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(wait_for_commit(i, latest));
    for (auto& node : nodes) node.reset();
    for (int i = 0; i < 3; ++i) {
        auto directory = std::make_shared<StorageDirectory>(directories[i]);
        RaftStateStorage commit{directory};
        LogStorage log{directory};
        KeyValueStateMachine machine;
        machine.apply_committed(log.load(), commit.load_commit().index);
        EXPECT_EQ(machine.get("key"), "successor");
    }
}

TEST_F(CommittedCluster, IsolatedLeaderCannotReturnSuccessfulWrite) {
    const int leader = find_leader();
    ASSERT_EQ(command(leader, "SET key committed").payload, "OK");
    for (int i = 0; i < 3; ++i) if (i != leader) nodes[i].reset();
    const auto committed = read_index(directories[leader] / "commit.json");
    const auto response = command(leader, "SET key isolated");
    EXPECT_EQ(response.type, MessageType::error);
    EXPECT_NE(response.payload.find("outcome unknown"), std::string::npos);
    EXPECT_EQ(read_index(directories[leader] / "commit.json"), committed);
}

TEST_F(CommittedCluster, PendingClientDoesNotBlockHeartbeatAndCannotReceiveOverwrittenCommandsResult) {
    const int leader = find_leader();
    for (int i = 0; i < 3; ++i) if (i != leader) nodes[i].reset();
    zmq::context_t context{1};
    zmq::socket_t client{context, zmq::socket_type::req};
    zmq::socket_t contact{context, zmq::socket_type::req};
    configure_socket(client);
    configure_socket(contact);
    client.set(zmq::sockopt::linger, 0);
    contact.set(zmq::sockopt::linger, 0);
    client.connect(endpoints[leader]);
    contact.connect(endpoints[leader]);
    const auto before = read_index(directories[leader] / "log.json");
    const auto term = read_index(directories[leader] / "state.json");
    send_message(client, {MessageType::text, 51, "SET key pending"});
    for (int i = 0; i < 100 && read_index(directories[leader] / "log.json") == before; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_EQ(read_index(directories[leader] / "log.json"), before + 1);
    send_message(contact, make_heartbeat_request(52, {term + 1, 99, before, term, before + 1,
        {{before + 1, term + 1, "SET key replacement"}}}));
    ASSERT_TRUE(decode_heartbeat_response(receive_message(contact)).success);
    const auto response = receive_message(client);
    EXPECT_EQ(response.id, 51u);
    EXPECT_EQ(response.type, MessageType::error);
    EXPECT_NE(response.payload.find("Leadership changed"), std::string::npos);
}

} // namespace
} // namespace zmq_template::server
