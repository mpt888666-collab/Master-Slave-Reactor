//
// Created by mpt on 2026/7/8.
//

#include "MsgNode.h"
RecvNode::RecvNode(uint16_t max_len, uint16_t msg_id) : MsgNode(max_len), _msg_id(msg_id) {}

SendNode::SendNode(const char *msg, uint16_t max_len, uint16_t msg_id) : MsgNode(max_len + HEAD_TOTAL_LEN), _msg_id(msg_id){
    uint16_t msg_id_host = htons(msg_id);
    uint16_t max_len_host = htons(max_len);

    memcpy(_data, &msg_id_host, HEAD_ID_LEN);
    memcpy(_data + HEAD_ID_LEN, &max_len_host, HEAD_DATA_LEN);
    memcpy(_data + HEAD_TOTAL_LEN, msg, max_len);
}
