#include "EventLoop.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <sys/eventfd.h>

#include "Channel.h"
#include "const.h"

EventLoop::EventLoop()
    : _ep_fd(epoll_create1(EPOLL_CLOEXEC)),
      _wakeupFd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      _events(1024) {
    if (_ep_fd < 0 || _wakeupFd < 0) {
        throw std::runtime_error("EventLoop init failed");
    }

    _wakeupChannel = std::make_shared<Channel>(_wakeupFd);

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.ptr = _wakeupChannel.get();

    if (epoll_ctl(_ep_fd, EPOLL_CTL_ADD, _wakeupFd, &ev) < 0) {
        throw std::runtime_error("EventLoop add wakeup fd failed");
    }
}

EventLoop::~EventLoop() {
    if (_wakeupFd >= 0) {
        close(_wakeupFd);
    }
    if (_ep_fd >= 0) {
        close(_ep_fd);
    }
}

void EventLoop::loop() {
    _threadId = std::this_thread::get_id();
    _looping = true;

    while (!_quit.load()) {
        int n = epoll_wait(_ep_fd, _events.data(), _events.size(), 10000);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < n; ++i) {
            auto* ch = static_cast<Channel*>(_events[i].data.ptr);
            if (ch == nullptr) {
                continue;
            }

            if (ch->GetFd() == _wakeupFd) {
                handleWakeup();
            } else {
                ch->HandleCallBack(_events[i].events);
            }
        }
        doPendingFunctors();
    }

    _looping = false;
}

void EventLoop::wakeup() const {
    uint64_t one = 1;
    write(_wakeupFd, &one, sizeof(one));
}

void EventLoop::handleWakeup() const {
    uint64_t one = 1;
    while (read(_wakeupFd, &one, sizeof(one)) > 0) {
    }
}

void EventLoop::Stop() {
    _quit.store(true);
    wakeup();
}

void EventLoop::AddChannel(Channel* ch){
    runInLoop([this, ch] {
        epoll_event ev{};
        ev.events = ch->GetEvents();
        ev.data.ptr = ch;
        epoll_ctl(_ep_fd, EPOLL_CTL_ADD, ch->GetFd(), &ev);
    });
}

void EventLoop::RemoveChannel(Channel* ch)  {
    runInLoop([this, ch] {
        epoll_event ev{};
        ev.data.ptr = ch;
        epoll_ctl(_ep_fd, EPOLL_CTL_DEL, ch->GetFd(), &ev);
    });
}

void EventLoop::UpdateChannel(Channel* ch) {
    runInLoop([this, ch] {
        epoll_event ev{};
        ev.events = ch->GetEvents();
        ev.data.ptr = ch;
        epoll_ctl(_ep_fd, EPOLL_CTL_MOD, ch->GetFd(), &ev);
    });
}

void EventLoop::runInLoop(std::function<void()> cb) {
    if (isInLoopThread()) {
        cb();
    }else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(std::function<void()> cb) {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _pendingFunctions.push_back(std::move(cb));
    }
    wakeup();
}

void EventLoop::doPendingFunctors() {
    std::vector<std::function<void()>> functors;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        functors.swap(_pendingFunctions);
    }
    for (auto& f : functors) {
        f();
    }
}

