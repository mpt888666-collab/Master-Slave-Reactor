//
// Created by mpt on 2026/7/8.
//

#ifndef CHATSERVER_MSGNODE_H
#define CHATSERVER_MSGNODE_H
#include <iostream>
#include "const.h"
#include <cstring>
class LogicSystem;
class MsgNode {
    friend class LogicSystem;
public:

    MsgNode(const MsgNode&) = delete;
    MsgNode& operator=(const MsgNode&) = delete;
    explicit MsgNode(uint16_t max_len) : _cur_len(0), _total_len(max_len){
        _data = new char[max_len + 1]();
        _data[max_len] = '\0';
    }

    ~MsgNode() {
        delete[] _data;
    }

    void Clear() {
        memset(_data, '\0', _total_len);
        _cur_len = 0;
    }

    uint16_t _cur_len;
    uint16_t _total_len;
    char* _data;
};

class RecvNode : public MsgNode {
    friend class LogicSystem;
    friend class LogicWork;
public:
    RecvNode(uint16_t max_len, uint16_t msg_id);

private:
    uint16_t _msg_id;
};

class SendNode : public MsgNode {
    friend class LogicSystem;
    friend class LogicWork;
public:
    SendNode(const char * msg, uint16_t max_len, uint16_t msg_id);
private:
    uint16_t _msg_id;
};

#endif //CHATSERVER_MSGNODE_H