#include "ThreadLoopPool.h"

#include "EventLoop.h"

ThreadLoopPool::ThreadLoopPool(std::size_t size) : _eventLoops(size) {
    for (std::size_t i = 0; i < size; ++i) {
        _eventLoops[i] = std::make_shared<EventLoop>();
    }

    for (std::size_t i = 0; i < size; ++i) {
        _threads.emplace_back([this, i]() {
            _eventLoops[i]->loop();
        });
    }
}

ThreadLoopPool::~ThreadLoopPool() {
    Stop();
}

std::shared_ptr<EventLoop> ThreadLoopPool::GetEventLoop() {
    std::lock_guard<std::mutex> lock(_mutex);
    return _eventLoops[_nextEventLoop++ % _eventLoops.size()];
}

void ThreadLoopPool::Stop() {
    for (auto& eventLoop : _eventLoops) {
        if (eventLoop) {
            eventLoop->Stop();
        }
    }

    for (auto& thread : _threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void ThreadLoopPool::Shutdown() {
    Stop();
}
