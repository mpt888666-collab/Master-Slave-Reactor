#include "CSession.h"

#include <uuid/uuid.h>

#include <cstring>
#include <unistd.h>
#include <utility>

#include "Channel.h"
#include "CServer.h"
#include "EventLoop.h"
#include "const.h"
#include "LogicSystem.h"

CSession::CSession(int conn_fd, std::shared_ptr<Channel> channel,
                   std::shared_ptr<EventLoop> event_loop, std::weak_ptr<CServer> server)
    : _server(std::move(server)),
      _conn_fd(conn_fd),
      _channel(std::move(channel)),
      _event_loop(std::move(event_loop)) {
    uuid_t uid;
    char str[37];

    uuid_generate_random(uid);
    uuid_unparse(uid, str);

    _session_uuid = std::string(str);
}

CSession::~CSession() {
    _b_close = true;
    closeSession();
}

void CSession::Init() {
    std::weak_ptr<CSession> weakSession = shared_from_this();

    _channel->SetHandleRead([weakSession]() {
        auto session = weakSession.lock();
        if (session) {
            session->handleRead();
        }
    });

    _channel->SetHandleClose([weakSession]() {
        auto session = weakSession.lock();
        if (session) {
            session->closeSession();
        }
    });

    _channel->SetHandleWrite([weakSession]() {
        auto session = weakSession.lock();
        if (session) {
            session->handleWrite();
        }
    });

    _channel->EnableReading();

    if (_event_loop) {
        _event_loop->AddChannel(_channel.get());
    }
}

std::string CSession::GetSessionId() const {
    return _session_uuid;
}

void CSession::handleRead() {
    char buf[MAX_LENGTH];

    while (true) {
        ssize_t n = recv(_conn_fd, buf, sizeof(buf), 0);

        if (n > 0) {
            _recv_buffer.append(buf, n);
        } else if (n == 0) {
            closeSession();
            return;
        } else if (errno == EINTR){
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        } else {
            closeSession();
            return;
        }
    }

    parseBuffer();
}

void CSession::parseBuffer() {
    while (_recv_buffer.size() >= HEAD_TOTAL_LEN) {
        uint16_t msg_id = 0;
        uint16_t body_len = 0;

        std::memcpy(&msg_id, _recv_buffer.data(), HEAD_ID_LEN);
        std::memcpy(&body_len, _recv_buffer.data() + HEAD_ID_LEN, HEAD_DATA_LEN);

        msg_id = ntohs(msg_id);
        body_len = ntohs(body_len);

        if (body_len == 0 || body_len > MAX_LENGTH) {
            closeSession();
            return;
        }

        if (_recv_buffer.size() < HEAD_TOTAL_LEN + body_len) {
            break;
        }

        auto recv_node = std::make_shared<RecvNode>(body_len, msg_id);

        std::memcpy(recv_node->_data,_recv_buffer.data() + HEAD_TOTAL_LEN,body_len);

        recv_node->_data[body_len] = '\0';
        recv_node->_cur_len = body_len;

        int hex = static_cast<int>(std::hash<std::string>{}(_session_uuid));
        LogicSystem::GetInstance()->PostMsgQueue(
            std::make_shared<LogicNode>(recv_node, shared_from_this()), hex
        );

        _recv_buffer.erase(0, HEAD_TOTAL_LEN + body_len);
    }
}

void CSession::closeSession() {
    if (_conn_fd < 0) {
        return;
    }

    _b_close = true;

    if (_channel && _event_loop) {
        _event_loop->RemoveChannel(_channel.get());
    }

    close(_conn_fd);
    _conn_fd = -1;

    if (auto server = _server.lock()) {
        server->RemoveSession(_session_uuid);
    }
}

void CSession::handleWrite() {
    while (true) {
        std::shared_ptr<MsgNode> msg_node;
        {
            std::lock_guard<std::mutex> lock(_send_mutex);
            if (_send_queue.empty()) {
                _channel->DisableWriting();
                _event_loop->UpdateChannel(_channel.get());
                return;
            }
            msg_node = _send_queue.front();
        }

        int n = send(_conn_fd, msg_node->_data + msg_node->_cur_len, msg_node->_total_len - msg_node->_cur_len, 0);

        if (n > 0) {
            msg_node->_cur_len += static_cast<uint16_t>(n);

            if (msg_node->_cur_len >= msg_node->_total_len) {
                std::lock_guard<std::mutex> lock(_send_mutex);
                _send_queue.pop();
            }
        }else if (n < 0 && errno == EINTR) {
            continue;
        }else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        }else if (n < 0 && (errno == EPIPE || errno == ECONNRESET)) {
            closeSession();
            return;
        }else {
            closeSession();
            return;
        }
    }
}

bool CSession::Send(MSG_IDS id, std::string msg) {
    std::lock_guard<std::mutex> lock(_send_mutex);

    if (_b_close.load()) {
        return false;
    }

    if (msg.size() > MAX_LENGTH) {
        return false;
    }

    if (_send_queue.size() >= MAX_SEND_QUEUE_SIZE) {
        return false;
    }

    _send_queue.push(std::make_shared<SendNode>(msg.c_str(), msg.size(), id));
    _channel->EnableWriting();
    _event_loop->UpdateChannel(_channel.get());
    return true;
}


void CSession::Close() {
    if (_b_close.exchange(true)) {
        return;
    }

    if (_event_loop) {
        _event_loop->runInLoop([self = shared_from_this()] {
            self->closeSession();
        });
    } else {
        closeSession();
    }
}

void CSession::Shutdown() {
    Close();
}

LogicNode::LogicNode(std::shared_ptr<RecvNode> recv_node, std::shared_ptr<CSession> session) :
        _recv_node(std::move(recv_node)), _session(std::move(session)){}

