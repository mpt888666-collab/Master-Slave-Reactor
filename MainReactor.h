//
// Created by mpt on 2026/9/20.
//

#ifndef SERVER_MAINREACTOR_H
#define SERVER_MAINREACTOR_H
#include <condition_variable>
#include <vector>
#include <memory>
#include <thread>
class CServer;
class MainReactor {
public:
    explicit MainReactor(int size = 2);
    ~MainReactor();
    void Run();
private:
    void Stop();
    std::vector<std::shared_ptr<CServer>> _listen_works;
    std::vector<std::thread> _threads;
    std::mutex _mtx;
    std::condition_variable _cv;
    std::atomic<bool> _stop{false};
};


#endif //SERVER_MAINREACTOR_H