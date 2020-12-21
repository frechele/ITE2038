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

void func(int key)
{
    for (int i = 0; i < 1000; ++i)
    {
        int trx_id = trx_begin();

        std::string value = "VALUE" + std::to_string(key);
        db_update(1, key, &value[0], trx_id);

        trx_commit(trx_id);
    }
}

int main()
{
    init_db(1000, 0, 10, "logfile.data", "logmsg.txt");

    open_table("DATA1");

    for (int i = 0; i < 100; ++i)
    {
        db_insert(1, i, "AAA");
    }

    for (int i = 0; i < 5; ++i)
    {
        std::thread worker(func, i);
        worker.detach();
    }
}
