#ifndef SERVER_CSERVER_H
#define SERVER_CSERVER_H

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

class EventLoop;
class CSession;

class CServer : public std::enable_shared_from_this<CServer>{
public:
    CServer(const int& port, const uint32_t& host);

    int Start();
    void RemoveSession(const std::string& uuid);

    void Stop();

private:
    void create_listen_fd();
    [[nodiscard]] int epoll_listen();

    std::map<std::string, std::shared_ptr<CSession>> _sessions;
    int _port;
    uint32_t _host;
    int _listen_fd{-1};
    int _ep_fd{-1};

    std::mutex _session_mtx;
};

#endif //SERVER_CSERVER_H