#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

namespace {

bool setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

void closeConn(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

double nowUs() {
    return std::chrono::duration<double, std::micro>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::size_t percentileIndex(std::size_t size, double p) {
    if (size == 0) return 0;
    std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(size));
    return idx >= size ? size - 1 : idx;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: %s <host> <port> <connections> <msgs_per_conn> [body_size] [window]\n"
                     "  window: 每连接最多同时未回包的消息数，默认等于 msgs_per_conn（流水线到底）；\n"
                     "          设为 1 即发一条等一条，可测得真实往返延迟；\n"
                     "          只有显式给出 window 时才统计并输出延迟分位数\n",
                     argv[0]);
        return 1;
    }

    const char* host = argv[1];
    const int port = std::atoi(argv[2]);
    const int connCount = std::atoi(argv[3]);
    const int msgsPerConn = std::atoi(argv[4]);
    const int bodySize = argc >= 6 ? std::atoi(argv[5]) : 16;
    const int windowArg = argc >= 7 ? std::atoi(argv[6]) : 0;
    const std::size_t window = windowArg > 0 ? static_cast<std::size_t>(windowArg)
                                             : static_cast<std::size_t>(msgsPerConn);
    // 只有显式指定 window 时才逐条采样延迟：流水线场景不采样，避免给吞吐测试增加额外开销
    const bool sampleLatency = windowArg > 0;

    if (port <= 0 || connCount <= 0 || msgsPerConn <= 0 || bodySize <= 0) {
        std::fprintf(stderr, "invalid arguments\n");
        return 1;
    }

    std::vector<uint8_t> packet(4 + bodySize);
    const uint16_t msgId = 0;
    const uint16_t bodyLen = static_cast<uint16_t>(bodySize);
    packet[0] = static_cast<uint8_t>((msgId >> 8) & 0xff);
    packet[1] = static_cast<uint8_t>(msgId & 0xff);
    packet[2] = static_cast<uint8_t>((bodyLen >> 8) & 0xff);
    packet[3] = static_cast<uint8_t>(bodyLen & 0xff);
    std::memset(packet.data() + 4, 'x', bodySize);

    std::vector<uint8_t> sendTemplate;
    sendTemplate.reserve(packet.size() * msgsPerConn);
    for (int i = 0; i < msgsPerConn; ++i) {
        sendTemplate.insert(sendTemplate.end(), packet.begin(), packet.end());
    }

    int epfd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        std::perror("epoll_create1");
        return 1;
    }

    struct Conn {
        int fd{-1};
        bool connected{false};
        bool done{false};
        std::size_t sendOff{0};
        int received{0};
        std::size_t stamped{0};
        std::vector<uint8_t> sendBuf;
        std::vector<uint8_t> recvBuf;
        std::vector<double> sendUs;
    };

    std::vector<Conn> conns(connCount);
    const std::size_t packetSize = packet.size();
    // 允许写出的字节上限 = 已回包数 + 窗口，取不到就说明窗口已满
    const auto sendLimit = [packetSize, window](const Conn& c) {
        const std::size_t allowed = (static_cast<std::size_t>(c.received) + window) * packetSize;
        return allowed < c.sendBuf.size() ? allowed : c.sendBuf.size();
    };
    std::vector<double> latencies;
    latencies.reserve(static_cast<std::size_t>(connCount) * static_cast<std::size_t>(msgsPerConn));
    int launched = 0;
    int finished = 0;
    int errors = 0;

    for (int i = 0; i < connCount; ++i) {
        Conn& c = conns[i];
        c.fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (c.fd < 0) {
            ++errors;
            c.done = true;
            continue;
        }

        setNonBlocking(c.fd);

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
            std::fprintf(stderr, "invalid host: %s\n", host);
            return 1;
        }

        int rc = ::connect(c.fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (rc < 0 && errno != EINPROGRESS) {
            closeConn(c.fd);
            c.done = true;
            ++errors;
            continue;
        }

        c.sendBuf = sendTemplate;
        c.sendUs.assign(static_cast<std::size_t>(msgsPerConn), 0.0);

        epoll_event ev{};
        ev.events = EPOLLOUT;
        ev.data.u32 = static_cast<uint32_t>(i);
        if (::epoll_ctl(epfd, EPOLL_CTL_ADD, c.fd, &ev) < 0) {
            closeConn(c.fd);
            c.done = true;
            ++errors;
            continue;
        }
        ++launched;
    }

    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(120);
    std::vector<epoll_event> events(1024);

    while (finished + errors < connCount) {
        if (std::chrono::steady_clock::now() > deadline) {
            break;
        }

        int n = ::epoll_wait(epfd, events.data(), static_cast<int>(events.size()), 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            const uint32_t idx = events[i].data.u32;
            Conn& c = conns[idx];
            if (c.done) continue;

            const uint32_t evs = events[i].events;
            if (evs & (EPOLLERR | EPOLLHUP)) {
                closeConn(c.fd);
                c.done = true;
                ++errors;
                continue;
            }

            if (!c.connected) {
                int err = 0;
                socklen_t len = sizeof(err);
                if (::getsockopt(c.fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                    closeConn(c.fd);
                    c.done = true;
                    ++errors;
                    continue;
                }
                c.connected = true;

                epoll_event mod{};
                mod.events = EPOLLIN | EPOLLOUT;
                mod.data.u32 = idx;
                ::epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &mod);
            }

            if (evs & EPOLLOUT) {
                while (c.sendOff < sendLimit(c)) {
                    // 单次 send 也不能超过窗口上限，否则内核会把整段流水线一次收下，窗口形同虚设
                    const std::size_t want = sendLimit(c) - c.sendOff;
                    ssize_t w = ::send(c.fd, c.sendBuf.data() + c.sendOff, want, MSG_NOSIGNAL);
                    if (w > 0) {
                        c.sendOff += static_cast<std::size_t>(w);
                        const std::size_t sentMsgs = c.sendOff / packetSize;
                        const double stamp = nowUs();
                        while (c.stamped < sentMsgs) {
                            c.sendUs[c.stamped++] = stamp;
                        }
                    } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    } else if (w < 0 && errno == EINTR) {
                        continue;
                    } else {
                        closeConn(c.fd);
                        c.done = true;
                        ++errors;
                        break;
                    }
                }
                if (c.done) continue;

                if (c.sendOff >= sendLimit(c)) {
                    // 全部发完，或流水线窗口已满：先只监听可读，等回包腾出窗口再续发
                    epoll_event mod{};
                    mod.events = EPOLLIN;
                    mod.data.u32 = idx;
                    ::epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &mod);
                }
            }

            if (evs & EPOLLIN) {
                uint8_t buf[4096];
                bool peerClosed = false;
                while (true) {
                    ssize_t r = ::recv(c.fd, buf, sizeof(buf), 0);
                    if (r > 0) {
                        c.recvBuf.insert(c.recvBuf.end(), buf, buf + r);
                    } else if (r == 0) {
                        peerClosed = true;
                        break;
                    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    } else if (errno == EINTR) {
                        continue;
                    } else {
                        closeConn(c.fd);
                        c.done = true;
                        ++errors;
                        break;
                    }
                }
                if (c.done) continue;

                std::size_t off = 0;
                while (c.recvBuf.size() - off >= 4) {
                    const std::size_t body = (static_cast<std::size_t>(c.recvBuf[off + 2]) << 8) | c.recvBuf[off + 3];
                    const std::size_t total = 4 + body;
                    if (c.recvBuf.size() - off < total) break;
                    off += total;
                    // 同一连接的请求与回包都是先入先出，第 received 条回包对应第 received 条请求
                    if (sampleLatency) {
                        latencies.push_back(nowUs() - c.sendUs[static_cast<std::size_t>(c.received)]);
                    }
                    ++c.received;
                }
                if (off > 0) {
                    c.recvBuf.erase(c.recvBuf.begin(), c.recvBuf.begin() + static_cast<std::ptrdiff_t>(off));
                }

                if (c.received >= msgsPerConn) {
                    closeConn(c.fd);
                    c.done = true;
                    ++finished;
                    continue;
                }

                if (peerClosed) {
                    closeConn(c.fd);
                    c.done = true;
                    ++errors;
                    continue;
                }

                if (c.sendOff < sendLimit(c)) {
                    // 回包腾出窗口空间，恢复发送
                    epoll_event mod{};
                    mod.events = EPOLLIN | EPOLLOUT;
                    mod.data.u32 = idx;
                    ::epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &mod);
                }
            }
        }
    }

    const auto end = std::chrono::steady_clock::now();
    const double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();
    const long long totalMsgs = static_cast<long long>(finished) * msgsPerConn;
    const double qps = elapsedMs > 0 ? totalMsgs / (elapsedMs / 1000.0) : 0.0;

    std::sort(latencies.begin(), latencies.end());
    double meanUs = 0.0;
    for (double v : latencies) {
        meanUs += v;
    }
    if (!latencies.empty()) {
        meanUs /= static_cast<double>(latencies.size());
    }
    const double p50Us = latencies.empty() ? 0.0 : latencies[percentileIndex(latencies.size(), 0.50)];
    const double p99Us = latencies.empty() ? 0.0 : latencies[percentileIndex(latencies.size(), 0.99)];
    const double p999Us = latencies.empty() ? 0.0 : latencies[percentileIndex(latencies.size(), 0.999)];
    const double maxUs = latencies.empty() ? 0.0 : latencies.back();

    std::printf("connections=%d launched=%d finished=%d errors=%d msgs_per_conn=%d window=%zu total_msgs=%lld elapsed_ms=%.0f qps=%.0f sampled=%zu p50_us=%.0f p99_us=%.0f p999_us=%.0f max_us=%.0f mean_us=%.0f\n",
                connCount, launched, finished, errors, msgsPerConn, window, totalMsgs, elapsedMs, qps,
                latencies.size(), p50Us, p99Us, p999Us, maxUs, meanUs);

    for (auto& c : conns) closeConn(c.fd);
    ::close(epfd);
    return errors == 0 ? 0 : 2;
}