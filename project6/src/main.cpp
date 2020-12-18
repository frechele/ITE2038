#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <random>

#include "dbapi.h"

using std::cin;
using std::cout;
using std::endl;

#define SUCCESSED(cond) ((cond) == 0)
#define FAILED(cond) ((cond) != 0)
#define CHECK(cond, var) (var ? SUCCESSED(cond) : FAILED(cond))

std::atomic<int> counter{ 0 };

void func()
{
    for (int i = 0; i < 1000; ++i)
    {
        int trx_id = trx_begin();

        std::random_device rd;
        std::mt19937 engine(rd());

        std::uniform_int_distribution<int> dist(0, 10000-1);

        int key = dist(engine);
        db_update(1, key, &(std::string("VALUE") + std::to_string(key) + std::to_string(rd()))[0], trx_id);

        ++counter;
        if (counter.load() == 100)
        {
            exit(0);
        }

        trx_commit(trx_id);
    }
}

int main()
{
    init_db(1000, 0, 10, "logfile.data", "logmsg.txt");

    open_table("DATA1");

    // for (int i = 0; i < 100; ++i)
    // {
    //     db_insert(1, i, "AAA");
    // }

    // for (int i = 0; i < 5; ++i)
    // {
    //     std::thread worker(func);
    //     worker.detach();
    // }

    int trx = trx_begin();

    db_update(1, 3, "XYZ", trx);

    trx_commit(trx);

    trx = trx_begin();

    db_update(1, 2, "XXX", trx);

    exit(0);

    shutdown_db();
}
