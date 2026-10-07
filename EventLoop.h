#ifndef SERVER_EVENTLOOP_H
#define SERVER_EVENTLOOP_H

#include <atomic>
#include <mutex>
#include <functional>
#include <memory>
#include <thread>
#include <sys/epoll.h>
#include <vector>

class Channel;

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    void loop();
    void wakeup() const;

    void Stop();

    void AddChannel(Channel* ch);
    void RemoveChannel(Channel* ch) ;
    void UpdateChannel(Channel* ch) ;

    [[nodiscard]] bool isInLoopThread() const {
        return _threadId == std::this_thread::get_id();
    }
    void runInLoop(std::function<void()> cb);
    void queueInLoop(std::function<void()> cb);
    void doPendingFunctors();


private:
    void handleWakeup() const;

    int _ep_fd{-1};
    int _wakeupFd{-1};
    std::shared_ptr<Channel> _wakeupChannel;
    std::vector<std::function<void()>> _pendingFunctions;
    std::atomic<bool> _looping{false};
    std::atomic<bool> _quit{false};
    std::vector<epoll_event> _events;
    std::mutex _mtx;
    std::thread::id _threadId;
};

#endif //SERVER_EVENTLOOP_H