//
// Created by mpt on 2026/9/16.
//

#ifndef SERVER_CONST_H
#define SERVER_CONST_H

#include <cstdio>
#include <unistd.h>
#include <sys/wait.h>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/fcntl.h>
#include <cerrno>
#include <netinet/in.h>
#include <pthread.h>
#include <string>
#include <map>
#include <memory>
inline volatile sig_atomic_t b_stop = false;

inline void set_nonblock(int fd) {
    int flag = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

enum MSG_IDS {
    MSG_TEST_ID = 0, //测试消息
    MSG_TEST_ID_RSP = 1, //测试回包
};

#define BACKLOG 1048
#define MAXEPOLLLEN 10
#define HEAD_ID_LEN 2
#define HEAD_DATA_LEN 2
#define HEAD_TOTAL_LEN 4
#define MAX_LENGTH (1024 * 2)
#define MAX_SEND_QUEUE_SIZE 1024
#endif //SERVER_CONST_H