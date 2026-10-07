//
// Created by mpt on 2026/9/20.
//

#include "MainReactor.h"
#include "CServer.h"
#include <arpa/inet.h>
#include "const.h"
MainReactor::MainReactor(int size) : _listen_works(size){
    //以后可以有ini文件，可以监听不同的端口
    for (int i = 0; i < _listen_works.size(); ++i) {
        _threads.emplace_back([this, i]() {
            _listen_works[i] = std::make_shared<CServer>(8090, inet_addr("127.0.0.1"));
            _listen_works[i]->Start();
        });

    }
}

MainReactor::~MainReactor() {
    Stop();
    for (auto& t : _threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto& server : _listen_works) {
        if (server) {
            server->Stop();
        }
    }

    _listen_works.clear();
}

void MainReactor::Run() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);

    std::unique_lock<std::mutex> lock(_mtx);

    std::thread signalThread([this, &set] {
        int sig = SIGINT;
        sigwait(&set, &sig);

        _stop.store(true);
        b_stop = true;
        _cv.notify_all();
    });

    _cv.wait(lock, [this] {
        return _stop.load();
    });

    if (signalThread.joinable()) {
        signalThread.join();
    }
}

void MainReactor::Stop() {
    _stop.store(true);
    b_stop = true;
    _cv.notify_all();
}


