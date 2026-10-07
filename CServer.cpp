#include "CServer.h"

#include <uuid/uuid.h>

#include "Channel.h"
#include "const.h"
#include "CSession.h"
#include "EventLoop.h"
#include "ThreadLoopPool.h"

CServer::CServer(const int& port, const uint32_t& host)
    : _port(port), _host(host) {
}

void CServer::create_listen_fd() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        std::cerr << "Can't create socket" << std::endl;
        return;
    }

    int reuse = 1;
    socklen_t len = sizeof(reuse);
    int ret = setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (ret == -1) {
        std::cerr << "Can't set reusable socket" << std::endl;
    }
    ret = setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));
    if (ret == -1) {
        std::cerr << "Can't set reusable socket" << std::endl;
    }
    sockaddr_in addr_in{};
    addr_in.sin_family = AF_INET;
    addr_in.sin_port = htons(_port);
    addr_in.sin_addr.s_addr = _host;
    socklen_t addr_len = sizeof(addr_in);

    ret = bind(listen_fd, reinterpret_cast<sockaddr*>(&addr_in), addr_len);
    if (ret == -1) {
        std::cerr << "Can't bind socket, errno=" << errno << std::endl;
        ::close(listen_fd);
        return;
    }

    ret = listen(listen_fd, BACKLOG);
    if (ret == -1) {
        std::cerr << "Can't listen on socket, errno=" << errno << std::endl;
        ::close(listen_fd);
        return;
    }

    _listen_fd = listen_fd;
    set_nonblock(_listen_fd);
}

int CServer::Start() {
    create_listen_fd();
    if (_listen_fd < 0) {
        return -1;
    }

    _ep_fd = epoll_create1(EPOLL_CLOEXEC);
    if (_ep_fd < 0) {
        std::cerr << "Can't create epoll fd" << std::endl;
        return -1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = _listen_fd;
    if (epoll_ctl(_ep_fd, EPOLL_CTL_ADD, _listen_fd, &ev) < 0) {
        std::cerr << "Can't listen epoll fd, errno=" << errno << std::endl;
        return -1;
    }

    return epoll_listen();
}

int CServer::epoll_listen() {
    while (!b_stop) {
        epoll_event ev2[MAXEPOLLLEN]{};
        int n = epoll_wait(_ep_fd, ev2, MAXEPOLLLEN, 10);

        if (n <= 0) {
            continue;
        }

        for (int i = 0; i < n; i++) {
            if (!(ev2[i].data.fd == _listen_fd && ev2[i].events & EPOLLIN)) {
                continue;
            }

            while (true) {
                sockaddr_in addr{};
                socklen_t addr_len = sizeof(addr);
                int conn_fd = accept(_listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len);

                if (conn_fd == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    if (errno == EMFILE) {
                        sleep(1);
                        continue;
                    }

                    std::cerr << "accept errno == " << errno << std::endl;
                    continue;
                }

                set_nonblock(conn_fd);

                auto loop = ThreadLoopPool::GetInstance()->GetEventLoop();
                auto channel = std::make_shared<Channel>(conn_fd);
                auto session = std::make_shared<CSession>(conn_fd, channel, loop, shared_from_this());

                {
                    std::lock_guard<std::mutex> lock(_session_mtx);
                    _sessions.emplace(session->GetSessionId(), session);
                }

                session->Init();
            }
        }
    }

    return 0;
}

void CServer::RemoveSession(const std::string& uuid) {
    std::lock_guard<std::mutex> lock(_session_mtx);
    _sessions.erase(uuid);
}

void CServer::Stop() {
    if (_listen_fd >= 0) {
        close(_listen_fd);
        _listen_fd = -1;
    }

    if (_ep_fd >= 0) {
        close(_ep_fd);
        _ep_fd = -1;
    }

    std::vector<std::shared_ptr<CSession>> sessions;
    {
        std::lock_guard<std::mutex> lock(_session_mtx);
        for (auto& [_, session] : _sessions) {
            sessions.push_back(session);
        }
        _sessions.clear();
    }

    for (auto& session : sessions) {
        session->Shutdown();
    }
}