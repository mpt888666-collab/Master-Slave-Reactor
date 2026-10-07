#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

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

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: %s <host> <port> <connections> <msgs_per_conn> [body_size]\n",
                     argv[0]);
        return 1;
    }

    const char* host = argv[1];
    const int port = std::atoi(argv[2]);
    const int connCount = std::atoi(argv[3]);
    const int msgsPerConn = std::atoi(argv[4]);
    const int bodySize = argc >= 6 ? std::atoi(argv[5]) : 16;

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
        std::vector<uint8_t> sendBuf;
        std::vector<uint8_t> recvBuf;
    };

    std::vector<Conn> conns(connCount);
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
                while (c.sendOff < c.sendBuf.size()) {
                    ssize_t w = ::send(c.fd, c.sendBuf.data() + c.sendOff, c.sendBuf.size() - c.sendOff, MSG_NOSIGNAL);
                    if (w > 0) {
                        c.sendOff += static_cast<std::size_t>(w);
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

                if (c.sendOff >= c.sendBuf.size()) {
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
            }
        }
    }

    const auto end = std::chrono::steady_clock::now();
    const double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();
    const long long totalMsgs = static_cast<long long>(finished) * msgsPerConn;
    const double qps = elapsedMs > 0 ? totalMsgs / (elapsedMs / 1000.0) : 0.0;

    std::printf("connections=%d launched=%d finished=%d errors=%d msgs_per_conn=%d total_msgs=%lld elapsed_ms=%.0f qps=%.0f\n",
                connCount, launched, finished, errors, msgsPerConn, totalMsgs, elapsedMs, qps);

    for (auto& c : conns) closeConn(c.fd);
    ::close(epfd);
    return errors == 0 ? 0 : 2;
}