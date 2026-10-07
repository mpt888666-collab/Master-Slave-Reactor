#ifndef SERVER_CSESSION_H
#define SERVER_CSESSION_H

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

#include "MsgNode.h"

class Channel;
class EventLoop;
class CServer;

class CSession : public std::enable_shared_from_this<CSession> {
public:
    CSession(int conn_fd, std::shared_ptr<Channel> channel,
             std::shared_ptr<EventLoop> event_loop, std::weak_ptr<CServer> server);
    ~CSession();

    void Init();

    std::string GetSessionId() const;
    void handleRead();
    void parseBuffer();
    void handleWrite();

    bool Send(MSG_IDS id, std::string msg);

    void Close();

    void Shutdown();
private:
    void closeSession();

    std::weak_ptr<CServer> _server;
    std::atomic<bool> _b_close = false;
    std::string _session_uuid;
    int _conn_fd;
    std::queue<std::shared_ptr<SendNode>> _send_queue;
    std::mutex _send_mutex;
    std::string _recv_buffer{};
    std::shared_ptr<Channel> _channel;
    std::shared_ptr<EventLoop> _event_loop;
};

class LogicNode{
    friend class LogicWork;
public:
    LogicNode(std::shared_ptr<RecvNode>, std::shared_ptr<CSession>);
private:
    std::shared_ptr<RecvNode> _recv_node;
    std::shared_ptr<CSession> _session;
};

#endif //SERVER_CSESSION_H