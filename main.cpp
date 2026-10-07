#include "const.h"
#include "MainReactor.h"

#include "LogicSystem.h"
#include "ThreadLoopPool.h"

void signalInit() {
    sigset_t mask;
    sigset_t sa_mask;

    sigemptyset(&mask);
    sigemptyset(&sa_mask);

    sigaddset(&mask, SIGPIPE);
    sigaddset(&sa_mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);

}

void blockSignal() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);
}
int main() {
    blockSignal();
    signalInit();
    {
        MainReactor reactor(2);
        reactor.Run();
    }
    ThreadLoopPool::GetInstance()->Shutdown();
    LogicSystem::GetInstance()->Shutdown();
    std::cout << "All threads finished" << std::endl;
    return 0;
}
