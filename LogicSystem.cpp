#include "LogicSystem.h"

#include "CSession.h"
#include "const.h"
#include <nlohmann/json.hpp>
#include <utility>

#include "Channel.h"
#include "EventLoop.h"
using json = nlohmann::json;
LogicWork::LogicWork() {
    RegisterCallBack();
    _work_thread = std::thread(&LogicWork::DealMsg, this);
}

LogicWork::~LogicWork() {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _b_stop = true;
    }

    _consume.notify_all();

    if (_work_thread.joinable()) {
        _work_thread.join();
    }
}

void LogicWork::postMsgQueue(std::shared_ptr<LogicNode> logic_node) {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _msg_que.push(std::move(logic_node));
    }

    _consume.notify_one();
}

void LogicWork::RegisterCallBack() {
    _fun_callbacks[MSG_IDS::MSG_TEST_ID] = [this](const std::shared_ptr<CSession>& session,short msg_id,const std::string& msg_data) {
        //std::cout << "msg_id : " << msg_id<< " msg_len : " << msg_data.size()<< " msg_data : " << msg_data << std::endl;
        json sendJson;
        sendJson["data"] = "我收到你的消息了，我回包给你";
        session->Send(MSG_TEST_ID_RSP, sendJson.dump());

    };
}

void LogicWork::DealMsg() {
    for (;;) {
        std::unique_lock<std::mutex> lock(_mtx);

        _consume.wait(lock, [this]() {
            return !_msg_que.empty() || _b_stop;
        });

        if (_b_stop) {
            while (!_msg_que.empty()) {
                auto msg_node = _msg_que.front();
                //std::cout << "recv_msg id is " << msg_node->_recv_node->_msg_id << std::endl;

                auto call_back_iter = _fun_callbacks.find(msg_node->_recv_node->_msg_id);
                if (call_back_iter == _fun_callbacks.end()) {
                    _msg_que.pop();
                    continue;
                }

                try {
                    call_back_iter->second(msg_node->_session,
                                           msg_node->_recv_node->_msg_id,
                                           std::string(msg_node->_recv_node->_data,
                                                       msg_node->_recv_node->_cur_len));
                } catch (const std::exception& e) {
                    std::cerr << "DealMsg handler exception: " << e.what() << std::endl;
                }

                _msg_que.pop();
            }
            return;
        }

        auto msg_node = _msg_que.front();
        //std::cout << "recv_msg id is " << msg_node->_recv_node->_msg_id << std::endl;

        auto call_back_iter = _fun_callbacks.find(msg_node->_recv_node->_msg_id);
        if (call_back_iter == _fun_callbacks.end()) {
            _msg_que.pop();
            //std::cout << "msg id [" << msg_node->_recv_node->_msg_id << "] handler not found" << std::endl;
            continue;
        }

        try {
            call_back_iter->second(msg_node->_session,
                                   msg_node->_recv_node->_msg_id,
                                   std::string(msg_node->_recv_node->_data,
                                               msg_node->_recv_node->_cur_len));
        } catch (const std::exception& e) {
            std::cerr << "DealMsg handler exception: " << e.what() << std::endl;
        }

        _msg_que.pop();
    }
}

LogicSystem::LogicSystem(int size) : _workerThreads(size){
    for (int i = 0; i < _workerThreads.size(); ++i) {
        _workerThreads[i] = std::make_shared<LogicWork>();
    }
}

void LogicSystem::PostMsgQueue(std::shared_ptr<LogicNode> logic_node, int hex) {
    auto work = _workerThreads[hex % _workerThreads.size()];
    work->postMsgQueue(std::move(logic_node));
}

void LogicSystem::Shutdown() {
    _workerThreads.clear();
}

