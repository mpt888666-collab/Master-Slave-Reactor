# Master-Slave Reactor TCP Server

基于 C++ 的主从 Reactor 高并发 TCP 服务器，使用 `epoll` ET 模式，支持多 acceptor、粘包/半包解析、异步回包和简单的 JSON 业务示例。

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

每个场景运行两次，取中位数。消息体大小 16 字节，服务端回包为 JSON。

| 连接数 | 每连接消息数 | 总消息数 | Run 1 (QPS) | Run 2 (QPS) | 中位数 (QPS) | 错误 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 500 | 50,000 | 127,504 | 150,111 | 138,807 | 0 |
| 500 | 200 | 100,000 | 158,030 | 147,822 | 152,926 | 0 |
| 1000 | 200 | 200,000 | 149,129 | 138,394 | 143,762 | 0 |
| 2000 | 100 | 200,000 | 175,904 | 164,260 | 170,082 | 0 |
| 500 | 500 | 250,000 | 157,252 | 147,366 | 152,309 | 0 |
| 100 | 1000 | 100,000 | 116,868 | 129,407 | 123,137 | 0 |

测试中所有连接均完成，无错误、无超时。

### 结果分析

- 本机 loopback 下，峰值约 **17 万 QPS**，稳定区间约 **12 万–16 万 QPS**。
- Release 构建、关闭每消息日志后，性能比 Debug + 日志有明显提升。
- 每条消息仍有 JSON 序列化、`shared_ptr` 和 `RecvNode/SendNode` 分配开销。
- 结果基于同机 loopback，真实网络环境下会低于该数值。

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
