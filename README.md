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
当前已持久化任期、投票及日志条目，已支持日志条目复制，尚未实现多数提交。

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
  server/rpc/heartbeat_rpc.hpp     # AppendEntries 日志复制与空心跳请求/响应
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

目前已支持任期、`voted_for` 和日志条目的持久化与启动恢复，最后日志索引和任期从真实日志恢复。
已支持携带日志的 AppendEntries，空条目列表维持心跳；失去心跳后 Follower 会重新选举。
后续仍需补充多数提交和状态机执行。
投票规则参考 [Raft 论文 Figure 2](https://raft.github.io/raft.pdf)。

## 日志条目

`src/server/log_entry.hpp` 定义 `zmq_template::server::LogEntry`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `index` | `std::uint64_t` | 日志索引，实际条目从 1 开始 |
| `term` | `std::uint64_t` | Leader 创建该条目时的任期 |
| `command` | `std::string` | 状态机命令的原始字节，可包含 NUL，不限定为文本 |

索引 0、任期 0 表示空日志边界，默认构造值不代表一条有效业务日志。
条目已接入本地日志存储、启动恢复和 AppendEntries 编解码，尚未接入状态机执行。

## Leader 心跳

Leader 当选后立即通过节点间 REQ/REP 发送 AppendEntries，之后默认每 **50ms** 发送一轮。
心跳间隔小于随机选举超时（150–300ms），使用同一事件循环和单调时钟调度。
每个 peer 最多一个待响应 RPC，尚未完成的调用不会被下一轮心跳覆盖；100ms 超时后关闭连接，再尝试发送。

`server/rpc/heartbeat_rpc.hpp` 中的请求包含 `term`、`leader_id`、`prev_log_index`、`prev_log_term`、
`leader_commit`、条目数量及条目列表。固定头为 48 字节；每条记录包含索引、任期、命令长度和命令字节。
响应仍包含 `term` 和 `success`（9 字节）。新的请求格式不兼容旧的 40 字节心跳，集群节点需要使用同一版本。

接收规则：

- 拒绝旧任期请求；当前或更高任期请求使节点退回 Follower，并重置选举计时器。
- 从真实日志校验前置索引和任期，包括日志中间位置；不匹配则回复失败。
- 跳过已存在的匹配条目，仅替换冲突的未提交后缀；空请求或较短的匹配请求不会删除额外后缀。
- 索引必须连续，任期必须为正、不递减且不超过 Leader 任期；相同索引/任期对应不同命令时拒绝请求。
- 先持久化更高任期，再将日志后缀原子替换并落盘，最后回复成功；存储失败会停止服务。
- Follower 提交索引只向前推进，上界是本次请求确认匹配的最后索引；空请求的上界为 `prev_log_index`。

Leader 为每个 Follower 维护 `next_index` 和 `match_index`。拒绝时回退复制位置，成功后推进到已确认索引之后；
超时重试原位置，新任期当选时重置进度。每批最多 128 条且负载不超过 1 MiB，单条命令最大为
`1 MiB - 48 - 24` 字节；超大本地条目无法发送，节点进入 Leader 发送流程时会报错退出。
只使用当前领导任期的响应更新进度；更高任期响应使 Leader 退位并持久化新任期。

当前业务请求仍直接返回示例响应，不自动追加日志。多数提交、提交索引的持久恢复和状态机应用尚未实现。
冲突保护使用当前进程已知的 `commit_index`，不代表已完成跨重启的提交安全闭环。

```sh
# 查看心跳发送、接收及响应日志
SPDLOG_LEVEL=debug ./scripts/start_demo.sh --nodes 3 --no-build
```

正常情况下选举完成后任期保持稳定；在 Leader 窗格按 `Ctrl+C` 停止它，剩余多数节点会选举新 Leader 并恢复心跳。
任期和投票记录在协议输出前持久化；角色、已知 Leader 及心跳计时器不持久化，重启后从 Follower 开始。

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

## 任期与投票持久化

服务端使用 `src/server/storage/persistent_state.hpp/.cpp` 中的 `StateStorage`，
通过 `load()` / `save()` 整体读写 `PersistentState`，包括当前任期和本任期投给的节点 ID。
该实现使用 Linux/POSIX 文件系统接口；当前不提供 Windows 存储后端。

```sh
./build/debug/zmq_server tcp://127.0.0.1:5555 --node-id 1 --data-dir ./data/node-1
```

不指定 `--data-dir` 时，通过 `/proc/self/exe` 定位可执行文件，默认目录为可执行文件所在目录下的
`data/node-<id>`，不受启动时工作目录影响。例如，运行 `build/debug/zmq_server` 时默认使用
`build/debug/data/node-<id>`。显式指定的相对 `--data-dir` 路径仍相对于工作目录。
每个节点应使用独立的数据目录，
重启时复用原目录；目录被独占锁定，两个服务进程不能同时使用同一目录。
原工作目录下的数据不会自动迁移；升级后请将原数据移至新目录，或通过 `--data-dir` 指定原目录。
`/build/` 和 `/data/` 已加入 Git 忽略规则，自定义目录请自行选择适当位置。

目录中的文件：

| 文件 | 用途 |
| --- | --- |
| `state.bin` | 正式状态记录，包含版本标识、任期、投票和校验和 |
| `state.tmp` | 原子替换前的临时记录，未完成的临时文件不会作为恢复状态 |
| `state.lock` | 生命周期内持有的目录锁，进程退出后由系统释放 |
| `log.bin` | 带版本标识、条目数和校验和的正式日志文件 |
| `log.tmp` | 日志原子替换前的临时文件，启动恢复时忽略 |
| `commit.bin` / `commit.tmp` | 已提交索引及对应任期的正式检查点 / 原子替换临时文件 |

保存顺序为写入临时文件、同步临时文件、原子替换正式文件、同步数据目录。
记录固定为 32 字节：8 字节版本标识 `RAFTST01`、8 字节大端任期、8 字节大端投票 ID、
8 字节大端 FNV-1a 校验和；投票 ID 为 0 表示尚未投票。校验和用于检测意外损坏，不用于身份认证。

发起新选举时先保存任期和自投票，再发送 RequestVote；处理投票或心跳请求后，
先保存发生变化的任期/投票再回复；处理更高任期响应时也先保存状态再继续运行。
状态没有变化时跳过写盘。存储错误会使服务退出，不会按普通 peer 超时继续运行。

首次启动缺少正式状态文件时使用任期 0、未投票；已有正式文件格式或校验和不合法时拒绝启动。
不要通过删除节点已有的数据目录来处理故障，否则会丢失任期和投票历史。

日志接口由 `src/server/storage/log_storage.hpp/.cpp` 中的 `LogStorage` 提供。
两者共享 `StorageDirectory`，由它统一持有目录锁并执行文件读写、同步和原子替换：

```cpp
auto directory = std::make_shared<StorageDirectory>("./data/node-1");
StateStorage state{directory};
LogStorage log{directory};
state.save({1, std::nullopt});
log.append({{1, 1, "set key value"}, {2, 1, "delete key"}});
auto entries = log.load();
log.truncate_suffix(2); // 删除索引 >= 2 的后缀，保留索引 1
```

日志索引必须从 1 连续递增，任期必须为正且不能递减；命令支持包含 NUL 的字节串。
`truncate_suffix(last_index + 1)` 是空操作，其他超出范围的截断会报错。
调用方必须保证不会截断已提交条目，并在追加新任期日志前持久化当前任期。
也可以单独通过目录路径构造任一存储；同一节点同时使用两者时，必须共享同一个 `StorageDirectory`，
避免重复获取目录独占锁。只要任一存储仍引用该目录对象，锁就会保持。
每次追加或截断都完整重写日志文件，经临时文件同步、原子替换和目录同步后才返回；当前实现适合小规模演示，
写入成本随日志总大小增长。文件头为 8 字节 `RAFTLG01` 和 8 字节条目数，每条记录包含
8 字节索引、8 字节任期、8 字节命令长度及命令字节；文件末尾是覆盖前面全部内容的 8 字节 FNV-1a 校验和。
整数均使用大端编码。缺少 `log.bin` 表示空日志；已有日志损坏、索引不连续或日志任期超过当前任期时拒绝启动。
启动恢复不代表日志已提交；提交位置恢复和状态机重放仍属于后续工作。
业务请求目前仍直接返回示例响应，不会自动追加日志；多数提交尚未实现。

## 提交位置与状态机恢复

`CommitStorage` 与任期/投票、日志存储共享目录锁。`commit.bin` 使用 32 字节记录：
`RAFTCT01`、大端提交索引、大端对应日志任期、FNV-1a 校验和，各占 8 字节。
提交位置只向前推进；启动时校验其索引和任期与日志一致，损坏或日志缺失时拒绝启动。
缺少检查点的旧数据目录从提交索引 0 开始，不把仅已落盘的日志误认为已提交。

持久化顺序为日志、提交检查点、状态机应用，再输出依赖提交的响应。
`KeyValueStateMachine` 在内存中处理 `SET key value`、`GET key`、`DEL key`，按索引顺序应用，
`last_applied` 始终不超过提交索引。空命令是内部 no-op；旧日志中的非法命令产生确定性错误且不修改键值表。
重启时从空表重放检查点以内的日志，未提交后缀不执行；不保存外部副作用或单独的应用进度，因此不会向外部重复发送事件。

Leader 统计每个 Follower 的 `match_index`，并将本节点的已持久化日志计入多数。
只有复制到严格多数的当前任期条目才能直接推进提交位置；此前条目随此前缀一起提交。
每次当选都会先落盘一条当前任期的空命令 no-op，再复制给其他成员；单节点可直接提交，
多节点缺少多数确认时不会推进。Leader 将已持久化的提交位置随下一轮 AppendEntries 传播给 Follower。
