cmake_minimum_required(VERSION 3.28)
project(Server)

set(CMAKE_CXX_STANDARD 20)

add_executable(Server
        main.cpp
        CServer.cpp
        CServer.h
        const.h
        CSession.cpp
        CSession.h
        EventLoop.cpp
        EventLoop.h
        Channel.cpp
        Channel.h
        ThreadLoopPool.cpp
        ThreadLoopPool.h
        MsgNode.cpp
        MsgNode.h
        Singleton.h
        LogicSystem.cpp
        LogicSystem.h
        MainReactor.cpp
        MainReactor.h)

target_compile_definitions(Server PRIVATE _GNU_SOURCE)

find_package(Threads REQUIRED)

target_link_libraries(Server PRIVATE uuid Threads::Threads)