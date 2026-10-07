#ifndef SERVER_CHANNEL_H
#define SERVER_CHANNEL_H

#include <cstdint>
#include <functional>
#include <sys/epoll.h>

using handleRead = std::function<void()>;

class Channel {
public:
    explicit Channel(int fd);

    void SetHandleRead(std::function<void()> cb);
    void SetHandleClose(std::function<void()> cb);
    void SetHandleWrite(std::function<void()> cb);

    void EnableReading();
    void EnableWriting();
    void DisableWriting();

    [[nodiscard]] uint32_t GetEvents() const { return _events; }
    [[nodiscard]] int GetFd() const { return _fd; }

    void HandleCallBack(uint32_t events);

private:
    int _fd;
    uint32_t _events{0};
    std::function<void()> _handleRead;
    std::function<void()> _handleClose;
    std::function<void()> _handleWrite;
};

#endif //SERVER_CHANNEL_H