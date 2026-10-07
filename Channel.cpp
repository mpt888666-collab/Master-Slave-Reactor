#include "Channel.h"

#include <utility>

Channel::Channel(int fd) : _fd(fd) {}

void Channel::SetHandleRead(std::function<void()> cb) {
    _handleRead = std::move(cb);
}

void Channel::SetHandleClose(std::function<void()> cb) {
    _handleClose = std::move(cb);
}
void Channel::SetHandleWrite(std::function<void()> cb) {
    _handleWrite = std::move(cb);
}

void Channel::EnableReading() {
    _events |= EPOLLIN | EPOLLET;
}

void Channel::EnableWriting() {
    _events |= EPOLLOUT;
}

void Channel::DisableWriting() {
    _events &= ~EPOLLOUT;
}

void Channel::HandleCallBack(uint32_t events) {
    if (events & (EPOLLIN | EPOLLRDHUP)) {
        if (_handleRead) {
            _handleRead();
        }
    }

    if (events & (EPOLLHUP | EPOLLERR)) {
        if (_handleClose) {
            _handleClose();
        }
    }

    if (events & EPOLLOUT) {
        if (_handleWrite) {
            _handleWrite();
        }
    }
}