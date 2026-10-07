#ifndef SERVER_LOGICSYSTEM_H
#define SERVER_LOGICSYSTEM_H

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <thread>

#include "Singleton.h"

class CSession;
class LogicNode;
class LogicWork {
    friend class LogicSystem;
public:
    LogicWork();
    ~LogicWork();
private:
    void RegisterCallBack();
    void postMsgQueue(std::shared_ptr<LogicNode> logic_node);
    void DealMsg();

    std::queue<std::shared_ptr<LogicNode>> _msg_que;
    std::mutex _mtx;
    std::condition_variable _consume;
    std::thread _work_thread;
    bool _b_stop = false;
    std::map<uint16_t, std::function<void(std::shared_ptr<CSession> session, uint16_t msg_id, const std::string& msg_data)>> _fun_callbacks;
};
class LogicSystem : public Singleton<LogicSystem> {
    friend class Singleton<LogicSystem>;
public:
    explicit LogicSystem(int size = 2);

    void PostMsgQueue(std::shared_ptr<LogicNode> logic_node, int hex);

    void Shutdown();
private:
    std::vector<std::shared_ptr<LogicWork>> _workerThreads;


    std::map<uint16_t, std::function<void(std::shared_ptr<CSession> session, uint16_t msg_id, const std::string& msg_data)>> _fun_callbacks;
};

#endif //SERVER_LOGICSYSTEM_H