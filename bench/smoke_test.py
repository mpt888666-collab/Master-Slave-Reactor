#!/usr/bin/env python3
"""Master-Slave Reactor 服务端冒烟测试。

用法:
    python3 bench/smoke_test.py [host] [port]

会先等待服务端可连接，然后：
  1. 单连接发送一条 msg_id=0 的消息，校验收到 msg_id=1 的 JSON 回包；
  2. 建立 20 条连接、每条发 20 条消息，校验回包数量。
任一步失败以非 0 退出，可直接用于 CI。
"""

import json
import socket
import struct
import sys
import time

MSG_TEST_ID = 0
MSG_TEST_ID_RSP = 1
HEADER = "!HH"
CONNS = 20
MSGS_PER_CONN = 20


def pack(msg_id, body):
    return struct.pack(HEADER, msg_id, len(body)) + body


def recv_packet(sock):
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None, None
        header += chunk

    msg_id, body_len = struct.unpack(HEADER, header)

    body = b""
    while len(body) < body_len:
        chunk = sock.recv(body_len - len(body))
        if not chunk:
            break
        body += chunk

    return msg_id, body


def connect(host, port, timeout=10.0):
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


def wait_for_server(host, port, deadline=15.0):
    end = time.time() + deadline
    last_error = None
    while time.time() < end:
        try:
            return connect(host, port)
        except OSError as exc:
            last_error = exc
            time.sleep(0.3)
    raise SystemExit("server %s:%d not reachable: %s" % (host, port, last_error))


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8090

    sock = wait_for_server(host, port)
    try:
        sock.sendall(pack(MSG_TEST_ID, b"hello"))
        msg_id, body = recv_packet(sock)
    finally:
        sock.close()

    if msg_id != MSG_TEST_ID_RSP:
        raise SystemExit("unexpected reply msg_id: %s" % msg_id)

    payload = json.loads(body.decode("utf-8"))
    if "data" not in payload:
        raise SystemExit("reply payload missing 'data': %s" % payload)

    print("single reply ok: msg_id=%d body=%s" % (msg_id, body.decode("utf-8")))

    socks = []
    try:
        for i in range(CONNS):
            conn = connect(host, port)
            for j in range(MSGS_PER_CONN):
                conn.sendall(pack(MSG_TEST_ID, b"c%d-m%d" % (i, j)))
            socks.append(conn)

        received = 0
        for conn in socks:
            for _ in range(MSGS_PER_CONN):
                msg_id, _ = recv_packet(conn)
                if msg_id == MSG_TEST_ID_RSP:
                    received += 1
    finally:
        for conn in socks:
            conn.close()

    expected = CONNS * MSGS_PER_CONN
    if received != expected:
        raise SystemExit("bulk replies %d/%d" % (received, expected))

    print("bulk replies ok: %d/%d" % (received, expected))
    print("SMOKE: PASS")


if __name__ == "__main__":
    main()