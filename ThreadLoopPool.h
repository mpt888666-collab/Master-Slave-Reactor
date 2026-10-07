#ifndef SERVER_THREADLOOPPOOL_H
#define SERVER_THREADLOOPPOOL_H

#include "Singleton.h"
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

class EventLoop;

class ThreadLoopPool : public Singleton<ThreadLoopPool> {
    friend class Singleton<ThreadLoopPool>;

public:
    ThreadLoopPool(ThreadLoopPool&) = delete;
    ThreadLoopPool& operator=(ThreadLoopPool&) = delete;
    ~ThreadLoopPool();

    std::shared_ptr<EventLoop> GetEventLoop();

    void Shutdown();

private:
    explicit ThreadLoopPool(std::size_t size = std::thread::hardware_concurrency());

    void Stop();

    std::vector<std::shared_ptr<EventLoop>> _eventLoops;
    std::vector<std::thread> _threads;
    std::size_t _nextEventLoop{0};
    std::mutex _mutex;
};

#endif //SERVER_THREADLOOPPOOL_H