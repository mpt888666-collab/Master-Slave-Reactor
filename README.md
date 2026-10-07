# Master-Slave Reactor TCP Server

基于 C++20 的主从 Reactor 高并发 TCP 服务器，使用 `epoll` ET 模式，支持多 acceptor、粘包/半包解析、异步回包和简单的 JSON 业务示例。

## 特性

- 主从 Reactor 模型：多个 `CServer` 负责 `accept`，`ThreadLoopPool` 管理从 Reactor 线程池
- `epoll` ET + 非阻塞 IO
- `eventfd` 唤醒 + `runInLoop/queueInLoop`，所有 `epoll_ctl` 都收敛到 loop 线程
- `SO_REUSEPORT` 多监听 socket，多个 acceptor 绑定同一端口
- `Channel` / `CSession` 连接抽象，读写关闭回调
- 自定义协议拆包：粘包 / 半包处理
- 发送队列 + `EPOLLOUT` 异步回包
- `LogicSystem` / `LogicWork` 业务线程池，业务与网络 IO 解耦
- JSON 回包示例（`nlohmann/json`）

## 架构

```text
main.cpp
  |
  v
MainReactor                 # 主 Reactor 容器
  |-- CServer (acceptor 1)  -- accept --> ThreadLoopPool
  |-- CServer (acceptor 2)  -- accept --> ThreadLoopPool
  |
  v
ThreadLoopPool              # 从 Reactor 池
  |-- EventLoop 1 (+ epoll + wakeupFd)
  |     |-- Channel -- CSession
  |-- EventLoop 2 (+ epoll + wakeupFd)
  |     |-- Channel -- CSession
  |-- ...
  |
  v
LogicSystem / LogicWork     # 业务层
```

### 文件对应关系

| 文件 | 角色 | 说明 |
| --- | --- | --- |
| `main.cpp` | 入口 | 信号初始化、创建 `MainReactor`、等待退出 |
| `MainReactor.*` | 主 Reactor 容器 | 启动 N 个 acceptor 线程，统一退出 |
| `CServer.*` | 主 Reactor | `socket / bind / listen / accept`，创建会话 |
| `ThreadLoopPool.*` | 从 Reactor 池 | 管理 N 个 `EventLoop` 和 worker 线程 |
| `EventLoop.*` | 从 Reactor 核心 | 每个线程一个 `epoll`，事件分发与唤醒 |
| `Channel.*` | 事件通道 | 封装 fd、事件和读/写/关闭回调 |
| `CSession.*` | 连接会话 | 收包、拆包、发送队列、关闭连接 |
| `MsgNode.*` | 消息节点 | `RecvNode` / `SendNode` 缓冲区 |
| `LogicSystem.*` | 业务层 | `LogicWork` 消费消息、执行回调、回包 |
| `Singleton.h` | 工具 | 单例模板 |
| `const.h` | 常量 | 协议长度、`b_stop`、非阻塞设置 |

## 数据流

1. 主 Reactor `accept` 新连接。
2. 从 `ThreadLoopPool` 轮询取一个 `EventLoop`。
3. 创建 `Channel` 和 `CSession`，注册读/写/关闭回调。
4. `EventLoop::AddChannel()` 通过 `runInLoop` 在从 Reactor 线程执行 `epoll_ctl`。
5. 客户端发数据，从 Reactor 报 `EPOLLIN`，`CSession::handleRead()` 收包。
6. `parseBuffer()` 处理粘包/半包，投递到 `LogicSystem`。
7. 业务回调调用 `session->Send()`，开启 `EPOLLOUT`。
8. 从 Reactor 报 `EPOLLOUT`，`handleWrite()` 发送，发完关闭写事件。

## 协议

大端字节序，包头固定 4 字节：

```text
+----------+----------+------------------+
| msg_id   | body_len | body             |
| 2 bytes  | 2 bytes  | body_len bytes   |
+----------+----------+------------------+
```

- `msg_id = 0`：测试消息，服务端返回 JSON。
- `msg_id = 1`：测试响应。
- `body_len` 最大值：`MAX_LENGTH = 2048`。

回包格式与请求一致，例如：

```json
{"data":"我收到你的消息了，我回包给你"}
```

## 依赖

- CMake 3.28+
- GCC 13+ / Clang 17+（C++20）
- `libuuid`
- `nlohmann/json`（header-only）
- POSIX Threads

Ubuntu/Debian:

```bash
sudo apt install cmake g++ uuid-dev nlohmann-json3-dev
```

## 构建与运行

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 8
./build/Server
```

默认监听 `127.0.0.1:8090`。

## 压测

### 环境

| 项目 | 值 |
| --- | --- |
| OS | Ubuntu 24.04.4 LTS (WSL2) |
| Kernel | 6.18.26.1-microsoft-standard-WSL2 |
| CPU | Intel Core i9-13900HX, 32 vCPU (WSL) |
| Memory | 15 GiB |
| 构建 | Release (`-O3 -DNDEBUG`) |
| 服务端日志 | 关闭（压测时为减少 IO 开销） |
| 测试方式 | 客户端与服务端同机 loopback |

### 压测客户端

仓库自带 `bench/bench_client.cpp`，基于 `epoll` + 非阻塞 socket，支持多连接和消息流水线。

```bash
g++ -O2 -std=c++20 bench/bench_client.cpp -o bench/bench_client
./bench/bench_client 127.0.0.1 8090 <connections> <msgs_per_conn> [body_size]
```

例如：

```bash
./bench/bench_client 127.0.0.1 8090 500 200
```

### 测试结果

每个场景运行 5 次，取中位数。消息体大小 16 字节，服务端回包为 JSON。

| 连接数 | 每连接消息数 | 总消息数 | 最低 (QPS) | 中位数 (QPS) | 最高 (QPS) | 错误 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 500 | 50,000 | 261,169 | 316,663 | 538,196 | 0 |
| 500 | 200 | 100,000 | 289,278 | 374,868 | 408,933 | 0 |
| 1000 | 200 | 200,000 | 276,866 | 352,613 | 417,233 | 0 |
| 2000 | 100 | 200,000 | 302,526 | 369,173 | 410,195 | 0 |
| 500 | 500 | 250,000 | 262,367 | 315,068 | 343,219 | 0 |
| 100 | 1000 | 100,000 | 255,690 | 273,169 | 328,358 | 0 |

测试中所有连接均完成，无错误、无超时。同一场景最快与最慢的两次可差 1.1x–2.1x（WSL2 调度抖动），因此以中位数为准。

### 与上一版的对比

上一版 `CSession::parseBuffer()` 中 `hex` 固定为 `1`，所有消息都投递到同一个 `LogicWork`；现在按 `std::hash(session_uuid)` 对 `LogicWork` 个数取模，两个业务线程同时工作。为排除环境差异，两版在同一台机器、同一时间段内交替实测（`hex = 1` 的对照版本为单独编译的副本，其余代码与当前版本完全一致）：

| 连接数 | 每连接消息数 | `hex = 1` 中位数 (QPS) | 当前中位数 (QPS) | 提升 |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 500 | 158,527 | 316,663 | +100% |
| 500 | 200 | 170,193 | 374,868 | +120% |
| 1000 | 200 | 180,048 | 352,613 | +96% |
| 2000 | 100 | 187,938 | 369,173 | +96% |
| 500 | 500 | 168,351 | 315,068 | +87% |
| 100 | 1000 | 141,999 | 273,169 | +92% |

只有单个业务线程时，所有从 Reactor 线程都要抢同一把 `LogicWork::_mtx`，队列与缓存行反复争用；拆到两个业务线程后锁竞争被摊开，整体约 **2 倍**提升（+87% ~ +120%）。旧版 README 记录的 12 万–17 万 QPS 与本次实测的 `hex = 1` 基线吻合，说明提升来自分片，而非环境变化。

### 结果分析

- 本机 loopback 下，中位数 **27 万–37 万 QPS**，单次峰值约 **54 万 QPS**。
- 相比 `hex = 1`（单业务线程）的上一版，吞吐提升约 **87%–120%**，来自 `LogicWork` 分片后两个业务线程真正并行。
- 性能在 500 连接后就基本饱和，继续增加连接数或每连接消息数不再明显上升。
- Release 构建、关闭每消息日志后，性能比 Debug + 日志有明显提升。
- 每条消息仍有 JSON 序列化、`shared_ptr` 和 `RecvNode/SendNode` 分配开销，业务回调里的 `json::dump()` 是当前主要热点。
- 结果基于同机 loopback，真实网络环境下会低于该数值。

### 不足

- 分片按 `std::hash(session_uuid)` 取模，同一会话固定落在同一个 `LogicWork`，尚未按用户维度分片。
- 发送队列有上限（`MAX_SEND_QUEUE_SIZE`），但触顶时是丢弃并断开连接，缺少更细的背压与限流。
- `_recv_buffer.erase(0, n)` 为 O(n)，高吞吐下可优化为环形缓冲或读偏移。
- 连接关闭与优雅退出时序仍可收敛：loop 停止后 `runInLoop` 的兜底、`LogicSystem::Shutdown()` 与 `PostMsgQueue` 的并发窗口。
- 缺少心跳与空闲连接回收，也没有统一日志组件（目前直接 `std::cout/std::cerr`）。

### 后续优化

1. 增加 `user_id` 登录态，按 `user_id` 分片，充分利用多个 `LogicWork`。
2. 对象池复用 `RecvNode/SendNode`，减少 `new/shared_ptr` 开销。
3. 缓冲区改为环形队列，去掉 `erase(0, n)`。
4. 心跳与空闲连接回收。
5. 统一日志组件，替换散落的 `std::cout/std::cerr`。
6. 增加单元测试、CI 和更完整的性能测试脚本。

## 目录结构

```text
.
├── bench/
│   └── bench_client.cpp
├── Channel.cpp / Channel.h
├── CServer.cpp / CServer.h
├── CSession.cpp / CSession.h
├── EventLoop.cpp / EventLoop.h
├── LogicSystem.cpp / LogicSystem.h
├── MainReactor.cpp / MainReactor.h
├── MsgNode.cpp / MsgNode.h
├── ThreadLoopPool.cpp / ThreadLoopPool.h
├── main.cpp
├── const.h
└── CMakeLists.txt
```

## TODO

- [ ] 多 `LogicWork` 分片
- [ ] 发送队列背压
- [ ] 优雅退出完善
- [ ] 对象池 / 内存池
- [ ] 单元测试与 CI
- [ ] 心跳与断线重连
