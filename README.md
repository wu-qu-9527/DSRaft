# CMake + Ninja + C++ + ZeroMQ 项目模板

C++17，使用 cppzmq（ZeroMQ 官方 C++ 绑定），提供基本 REQ/REP 请求响应通信示例。

## 环境

- CMake >= 3.24、Ninja、支持 C++17 的编译器。
- 首次配置需要联网，从 GitHub 下载 libzmq 4.3.5、cppzmq 4.10.0 和 spdlog 1.15.3，后续构建使用缓存。启用测试时还会下载 GoogleTest 1.17.0。
- Linux 示例：`sudo apt install build-essential cmake ninja-build`。
- Windows 请在 Visual Studio Developer PowerShell 中运行，并安装 CMake 和 Ninja。
- macOS 需安装 Xcode Command Line Tools、CMake 和 Ninja。

## 构建和测试

```sh
cmake --preset debug
cmake --build --preset debug --parallel
ctest --preset debug
```

Release 构建将上述 `debug` 替换为 `release`。构建目录分别为 `build/debug` 和 `build/release`。
生成的 `compile_commands.json` 位于相应构建目录，可供 clangd 等工具使用。

## 运行

先在终端一启动服务端，再在终端二启动客户端：

```sh
# 终端一：持续运行，Ctrl+C 停止
./build/debug/zmq_server

# 终端二：预期输出 Hello, ZeroMQ
./build/debug/zmq_client
```

自定义地址和消息：

```sh
./build/debug/zmq_server tcp://127.0.0.1:6000
./build/debug/zmq_client tcp://127.0.0.1:6000 "C++"
```

Windows 可执行文件带 `.exe` 后缀。示例默认只监听本机，客户端收发超时为 5 秒，出错返回非零退出码。
客户端使用 `zmq::socket_type::req` 并 connect，服务端使用 `zmq::socket_type::rep` 并 bind。
每个 REQ socket 严格按“发送请求 → 接收响应”交替执行；REP 按“接收请求 → 发送响应”交替执行。
服务端循环接收请求并回复；客户端请求超时后应重建 REQ socket，再发送新的请求。
服务端通过 `zmq::poll` 等待消息，收发不阻塞事件循环；关闭 socket 时最多等待 1 秒发送队列排空。
同一 context 可以跨线程共享，同一 socket 应固定由一个线程使用。

## 一键启动：K 个 server + 1 个 client

在 Linux 当前终端（支持 SSH）中安装 tmux，然后执行：

```sh
sudo apt install tmux
./scripts/start_demo.sh
```

SSH 登录后可直接运行上述命令；从本机通过一条 SSH 命令启动时，需要分配伪终端：

```sh
ssh -t user@host 'cd /path/to/Raft && ./scripts/start_demo.sh'
```

使用 `--nodes K` 指定 server 数量，默认 `K=3`。脚本会先配置并构建 server/client，
再在当前终端连接 tmux 会话，自动平铺 `K+1` 个窗格；已在 tmux 中时切换当前客户端到新会话。
无需图形桌面或 GNOME Terminal。默认三节点布局示例：

```text
┌───────────────────────┬───────────────────────┐
│ server-1 :5555        │ server-2 :5556        │
├───────────────────────┼───────────────────────┤
│ server-3 :5557        │ client                │
└───────────────────────┴───────────────────────┘
```

节点 ID 自动分配为 `1..K`，端口为 `base-port` 到 `base-port+K-1`。
一个 client 进程通过 K 个独立 REQ socket 分别连接 K 个 server，各发送一次请求并打印响应，每个 socket 只有一个待响应请求。
server 持续运行并显示选举超时日志；client 完成请求后退出，窗格保留输出并进入 shell。
K 个 server 配置为同一固定成员集群，每个节点自动配置其余 `K-1` 个 peers，
节点之间自动发起投票 RPC 并统计多数票；client 仅发送普通业务请求。
Leader 当选后立即发送心跳，之后每 50ms 发送一次；Follower 在持续收到有效心跳时不会发起选举。
当前尚未实现日志条目复制和持久化。

```sh
# 已构建时跳过构建
./scripts/start_demo.sh --no-build
# 创建 5 个 server，端口为 6000–6004，外加 1 个 client
./scripts/start_demo.sh --nodes 5 --preset release --session raft-test --base-port 6000
# 单节点运行
./scripts/start_demo.sh --nodes 1 --session raft-single
# 重新连接仍然存在的会话
tmux attach-session -t raft-demo
# 关闭本次演示的所有窗格和进程
tmux kill-session -t raft-demo
```

默认快捷键：`Ctrl+b` 后按方向键切换窗格，`Ctrl+b` 后按 `z` 放大/还原当前窗格，
`Ctrl+b` 后按 `d` 分离会话。关闭终端窗口通常只会分离，会话仍可重新连接。
同名会话存在时脚本会提示并退出；请连接旧会话、关闭旧会话，或指定其他名称。
`K` 必须为正整数，分配的全部端口必须位于 1024–65535；节点较多时可放大终端或使用 `Ctrl+b z` 查看单个窗格。

也可以不使用终端脚本，直接运行多目标客户端：

```sh
./build/debug/zmq_client --all tcp://127.0.0.1:5555 tcp://127.0.0.1:5556 tcp://127.0.0.1:5557
```

## 使用本机依赖（离线配置）

预先安装 ZeroMQ 和 cppzmq，例如 Ubuntu/Debian：

```sh
sudo apt install libzmq3-dev cppzmq-dev libspdlog-dev libgtest-dev
cmake --preset debug -DZMQ_TEMPLATE_USE_SYSTEM_DEPS=ON
cmake --build --preset debug --parallel
ctest --preset debug
```

非标准安装路径可传入 `-DCMAKE_PREFIX_PATH=/path/to/prefix`。
切回自动下载模式时显式传入 `-DZMQ_TEMPLATE_USE_SYSTEM_DEPS=OFF`。
默认下载构建关闭 CURVE/TLS 和开发中 API，适合本机通信示例；需要认证或加密时应单独配置依赖。

## 目录

```text
CMakeLists.txt                  # 目标、编译选项和测试
CMakePresets.json               # Ninja Debug / Release 配置
cmake/Dependencies.cmake        # 固定版本下载或使用本机依赖
scripts/start_demo.sh          # 当前终端 tmux K 个 server + client 启动脚本
src/
  server/main.cpp              # 帮助信息、日志初始化、配置解析和服务入口
  server/server.hpp            # server 类、实例角色和 wait_util_stop 接口
  server/server.cpp            # 服务初始化、RPC 事件循环与停止处理
  server/state.hpp             # 角色类型、任期、已投票节点和日志末尾元数据
  server/vote_handler.hpp      # voteFor：接收 RequestVote 并返回结果
  server/rpc/peer_rpc.hpp          # requestVoteFor：节点间异步 REQ/REP 调用
  server/node_config.hpp       # 节点 ID 和固定 peer 配置数据结构
  server/config_parser.hpp     # 命令行配置解析接口
  server/config_parser.cpp     # 命令行配置解析与校验
  server/rpc/vote_rpc.hpp          # RequestVote 请求与响应结构体
  server/rpc/vote_rpc.cpp          # 投票 RPC 编解码
  server/rpc/heartbeat_rpc.hpp     # 空 AppendEntries 心跳请求与响应
  server/rpc/heartbeat_rpc.cpp     # 心跳 RPC 编解码
  server/election.hpp          # 任期、自投、计票和 Leader 转换
  server/heartbeat.hpp         # 心跳定时器与接收处理
  server/election_timer.hpp    # 随机选举定时器，使用单调时钟
  client/main.cpp              # REQ 客户端
  common/message.hpp           # 双方共用的 Message 结构体、类型和协议声明
  common/message.cpp           # 消息序列化与协议校验
  common/messaging.hpp         # 共用收发接口
  common/messaging.cpp         # socket 配置及结构体收发
  common/logging.hpp           # 共用日志初始化接口和 spdlog API
  common/logging.cpp           # 日志格式、终端输出及环境变量配置
cmake/Testing.cmake             # GoogleTest 依赖配置
tests/message_test.cpp         # GoogleTest 序列化与协议校验测试
tests/messaging_test.cpp       # GoogleTest REQ/REP 请求响应测试
tests/election_timer_test.cpp  # 选举超时判断、显式重置与剩余时间测试
tests/vote_rpc_test.cpp        # 投票协议与投票规则测试
tests/node_election_test.cpp   # 节点计票和节点间 RPC、超时恢复测试
tests/heartbeat_test.cpp       # 心跳协议、定时器、任期及收发测试
```

新业务代码可链接 `zmq_template::messaging`；服务器配置及节点间 RPC 由 `zmq_template::server_core` 提供。
项目名和命名空间可按需替换。
依赖接入方式参考 [cppzmq 官方文档](https://github.com/zeromq/cppzmq)。

## 共用通信结构体

`src/common/message.hpp` 定义双方使用的消息：

```cpp
enum class MessageType : std::uint32_t {
    text = 1,
    acknowledgement = 2,
    request_vote = 3,
    request_vote_response = 4,
    error = 5,
    heartbeat = 6,
    heartbeat_response = 7,
};

struct Message {
    MessageType type{MessageType::text};
    std::uint32_t id{0};
    std::string payload;
};
```

通过 `send_message(socket, message)` / `receive_message(socket)` 收发结构体。
通信采用单帧协议：`ZMQ1` 魔数和版本标识、4 字节类型、4 字节消息 ID、4 字节负载长度、负载内容。
整数使用网络字节序，负载最大 1 MiB，支持空消息和含零字节的二进制数据。
不直接传输结构体内存，避免 `std::string` 指针、内存对齐和字节序问题；接收端校验版本、类型和长度。
扩展消息类型时，应同步更新 `MessageType` 和协议校验逻辑。

## GoogleTest 测试

测试链接 `GTest::gtest_main`，由 `gtest_discover_tests` 自动注册到 CTest。

```sh
# 列出用例
ctest --preset debug -N
# 按测试套件筛选
ctest --preset debug -R MessageSerialization
# 也可以直接使用 GoogleTest 的筛选参数
./build/debug/zmq_tests --gtest_filter='RequestReplyCommunication.*'
```

禁用测试可使用 `cmake --preset debug -DBUILD_TESTING=OFF`，此时不下载或查找 GoogleTest。
重新启用时传入 `-DBUILD_TESTING=ON`。本机依赖模式下需要预先安装 GoogleTest。

## 日志（spdlog）

客户端和服务端通过 `common/logging.hpp` 使用 spdlog，默认级别为 `info`。
日志输出到标准错误，包含毫秒时间、程序名、进程 ID 和级别；客户端响应仍输出到标准输出。
终端支持彩色级别显示，各个 server 可以通过进程 ID 和监听端口区分。

```sh
# 查看连接、发送请求和接收消息的调试日志
SPDLOG_LEVEL=debug ./scripts/start_demo.sh --no-build
# 单独运行程序
SPDLOG_LEVEL=debug ./build/debug/zmq_server
# 仅显示错误
SPDLOG_LEVEL=error ./build/debug/zmq_client
```

可用级别：`trace`、`debug`、`info`、`warn`、`error`、`critical`、`off`。
每个程序启动时调用一次 `zmq_template::init_logging("程序名")`，然后使用：

```cpp
#include "common/logging.hpp"

spdlog::info("Node {} started", node_id);
spdlog::debug("Request id={}", request_id);
spdlog::error("Connection failed: {}", error.what());
```

新增目标链接 `zmq_template::logging` 即可。默认使用同步终端日志；当前不创建日志文件。
接口参考 [spdlog 官方文档](https://github.com/gabime/spdlog)。

## 服务端选举定时器

服务端默认以 `Follower` 启动，使用 `std::chrono::steady_clock` 维护截止时间，
每次从 **150–300ms** 范围内独立随机选择超时：

- `Follower` 超时：切换到 `Candidate`，重新随机计时。
- `Candidate` 再次超时：保持 `Candidate`，重新随机计时并记录日志。
- `Leader`：不触发选举超时动作。

选举定时器和 REQ/REP 消息由同一线程处理。`zmq::poll` 最长等待到下一次到期时间，
并以最多 100ms 的间隔检查退出信号；server 可用 `Ctrl+C` 或 SIGTERM 停止。
普通客户端请求不重置选举定时器。后续实现有效的 Leader 联系或授予投票时，
可调用 `ElectionTimer::reset()` 重置；开始新一轮选举时也应重新随机计时。

定时器到期后启动新一轮选举：递增任期、投自己一票、向 peers 请求投票。收到多数票后切换为 Leader。
收到更高任期的请求或响应时更新任期并退回 Follower。
定时器测试传入模拟时间点，不依赖真实休眠。

## RequestVote RPC 接口

服务端在原有 REP 端口接收投票请求，外层 `Message.id` 用于匹配请求与响应。
`server/rpc/vote_rpc.hpp` 提供共用结构体及编解码接口：

```cpp
struct RequestVoteRequest {
    std::uint64_t term;
    std::uint64_t candidate_id;
    std::uint64_t last_log_index;
    std::uint64_t last_log_term;
};
struct RequestVoteResponse {
    std::uint64_t term;
    bool vote_granted;
};
```

请求负载为四个网络字节序 uint64（共 32 字节）；响应为 uint64 任期和 1 字节布尔值（共 9 字节）。
`make_vote_request` / `make_vote_response` 创建消息，`decode_vote_request` / `decode_vote_response` 校验和解析消息。
格式错误或不支持的请求返回 `MessageType::error`；合法请求的响应保留 RPC ID。

`server/vote_handler.hpp` 提供 `voteFor(request, state, role, timer)`：

- 请求任期较小：拒绝，并返回本地任期。
- 请求任期较大：更新本地任期、清除上一任期投票、切换为 Follower，然后判断是否授票。
- 当前任期尚未投票，或已投给相同候选人，并且候选人日志至少与本地一样新时，授予投票。
- 日志新旧先比较最后日志任期，任期相同时再比较最后日志索引。
- 授予投票时重置选举定时器；拒绝时不重置。

节点间调用由 `server/rpc/peer_rpc.hpp` 的 `PeerRpc::requestVoteFor` 发起：

```text
候选节点 REQ ── RequestVote ──→ 其他节点 REP
候选节点 REQ ←─ voteFor 结果 ── 其他节点 REP
```

`voteFor` 是 RequestVote 的响应处理，不额外创建反向 RPC。每个 peer 有独立的 REQ socket，
统一由节点事件循环轮询；不会阻塞其他请求。响应携带原 RPC ID 和任期，
只统计当前任期、当前选举轮次的有效票，同一节点只计一票。
RPC 超时为 100ms，失败后释放连接，下一轮重新建立，避免 REQ 状态机阻塞。
收到更高任期的响应会退回 Follower，清除旧投票并重设选举定时器。

手动在三个终端启动 server（每个节点配置其他两个节点）：

```sh
./build/debug/zmq_server tcp://127.0.0.1:5555 --node-id 1 --peer 2=tcp://127.0.0.1:5556 --peer 3=tcp://127.0.0.1:5557
./build/debug/zmq_server tcp://127.0.0.1:5556 --node-id 2 --peer 1=tcp://127.0.0.1:5555 --peer 3=tcp://127.0.0.1:5557
./build/debug/zmq_server tcp://127.0.0.1:5557 --node-id 3 --peer 1=tcp://127.0.0.1:5555 --peer 2=tcp://127.0.0.1:5556
```

也可以运行 `./scripts/start_demo.sh --nodes 3`，脚本会自动生成以上节点配置；修改 `--nodes K` 可创建其他规模的集群。
节点 ID 必须是唯一的正整数，所有节点应配置一致的完整成员集合；不配置 peers 时按单节点集群运行。
业务客户端只支持普通文本请求及 `--all`，不提供投票命令。

目前为内存中的选举原型：任期和 `voted_for` 尚未持久化，日志元数据尚未接入存储。
已支持空 AppendEntries 心跳维持 Leader，失去心跳后 Follower 会重新选举。
后续仍需补充日志条目复制和响应前的状态持久化。
投票规则参考 [Raft 论文 Figure 2](https://raft.github.io/raft.pdf)。

## Leader 心跳

Leader 当选后立即通过节点间 REQ/REP 发送空 AppendEntries 心跳，之后每 **50ms** 发送一轮。
心跳间隔小于随机选举超时（150–300ms），使用同一事件循环和单调时钟调度。
每个 peer 最多一个待响应 RPC，尚未完成的调用不会被下一轮心跳覆盖；100ms 超时后关闭连接，再尝试发送。

`server/rpc/heartbeat_rpc.hpp` 中的请求包含 `term`、`leader_id`、`prev_log_index`、`prev_log_term`、`leader_commit`，
负载为 5 个网络字节序 uint64（40 字节）；响应包含 `term` 和 `success`（9 字节）。
消息不包含日志条目，当前实现仅支持空 AppendEntries。

接收规则：

- 旧任期心跳：拒绝，不重置选举定时器。
- 当前或更高任期心跳：退回 Follower、记录 Leader ID、重置选举定时器；更高任期还会清除旧投票。
- 即使日志匹配失败，当前任期 Leader 的联系也会重置选举定时器，但响应 `success=false`。
- Leader 收到更高任期的响应：退回 Follower，并停止发送心跳。

当前只保存日志尾部元数据，可检查日志原点及本地尾部是否匹配，尚不能查询中间日志项或修复不一致日志。
空 AppendEntries 只在匹配成功时更新提交索引，上界为本次匹配的 `prev_log_index`；尚未实现状态机应用。

```sh
# 查看心跳发送、接收及响应日志
SPDLOG_LEVEL=debug ./scripts/start_demo.sh --nodes 3 --no-build
```

正常情况下选举完成后任期保持稳定；在 Leader 窗格按 `Ctrl+C` 停止它，剩余多数节点会选举新 Leader 并恢复心跳。
心跳与投票状态仍仅保存在内存中，重启安全性依赖后续持久化实现。

## 服务端计时参数

服务端支持以下参数，单位均为正整数毫秒：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--heartbeat-interval-ms` | 50 | Leader 心跳发送间隔 |
| `--heartbeat-timeout-ms` | 100 | 单次心跳 RPC 等待响应的超时；超时后取消该调用，不直接触发选举 |
| `--election-timeout-min-ms` | 150 | 每轮随机选举超时的最小值 |
| `--election-timeout-max-ms` | 300 | 每轮随机选举超时的最大值（包含此值） |

心跳间隔必须小于选举超时最小值，最小值不能大于最大值。
配置值上限为 2147483647ms；重复参数、缺失值、非整数及非正数会报错退出。
选举超时最小值和最大值相等时使用固定超时；正常多节点部署建议保留随机范围。
投票 RPC 超时仍为 100ms，不受心跳 RPC 超时参数影响。

```sh
./build/debug/zmq_server tcp://127.0.0.1:5555 --node-id 1 \
  --peer 2=tcp://127.0.0.1:5556 --peer 3=tcp://127.0.0.1:5557 \
  --heartbeat-interval-ms 80 --heartbeat-timeout-ms 240 \
  --election-timeout-min-ms 400 --election-timeout-max-ms 800
```

其余节点可使用相同的计时配置；每次开始新选举、授票或接受有效 Leader 联系时，
都会在配置范围内重新随机选取选举超时。具体生效参数会在服务启动日志中输出。
