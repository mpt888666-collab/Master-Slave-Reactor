#include "LogicSystem.h"

#include "CSession.h"
#include "const.h"
#include <nlohmann/json.hpp>
#include <utility>

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
    if (_b_stop) return;
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
        if (!session->Send(MSG_TEST_ID_RSP, sendJson.dump())) {
            session->Close();
        }

    };
}

void LogicWork::DealMsg() {
    for (;;) {
        std::unique_lock<std::mutex> lock(_mtx);

        _consume.wait(lock, [this]() {
            return !_msg_que.empty() || _b_stop;
        });

        if (_b_stop) {
            std::queue<std::shared_ptr<LogicNode>> local;
            local.swap(_msg_que);
            lock.unlock();
            while (!local.empty()) {
                auto msg_node = local.front();
                local.pop();
                //std::cout << "recv_msg id is " << msg_node->_recv_node->_msg_id << std::endl;

                auto call_back_iter = _fun_callbacks.find(msg_node->_recv_node->_msg_id);
                if (call_back_iter == _fun_callbacks.end()) {
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

            }
            return;
        }

        auto msg_node = _msg_que.front();
        _msg_que.pop();
        lock.unlock();
        //std::cout << "recv_msg id is " << msg_node->_recv_node->_msg_id << std::endl;

        auto call_back_iter = _fun_callbacks.find(msg_node->_recv_node->_msg_id);
        if (call_back_iter == _fun_callbacks.end()) {
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

    }
}

LogicSystem::LogicSystem(int size) : _workerThreads(size){
    if (_workerThreads.empty()) return;
    for (int i = 0; i < _workerThreads.size(); ++i) {
        _workerThreads[i] = std::make_shared<LogicWork>();
    }
}

void LogicSystem::PostMsgQueue(std::shared_ptr<LogicNode> logic_node, int hex) {
    if (_b_stop.load() || _workerThreads.empty()) return;
    auto work = _workerThreads[hex % _workerThreads.size()];
    work->postMsgQueue(std::move(logic_node));
}

void LogicSystem::Shutdown() {
    _b_stop.store(true);
    _workerThreads.clear();
}

