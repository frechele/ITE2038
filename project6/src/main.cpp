#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <chrono>
#include <mutex>
#include <thread>

#include "dbapi.h"

using std::cin;
using std::cout;
using std::endl;

#define SUCCESSED(cond) ((cond) == 0)
#define FAILED(cond) ((cond) != 0)
#define CHECK(cond, var) (var ? SUCCESSED(cond) : FAILED(cond))

void func()
{
    int trx_id = trx_begin();

    db_update(1, 3, "XYZ", trx_id);
    trx_commit(trx_id);

    int new_id = trx_begin();
    db_update(1, 2, "XXX", new_id);
    exit(0);
}

int main()
{
    init_db(1000, 0, 0, "logfile.data", "logmsg.txt");

    open_table("DATA1");

    db_insert(1, 2, "AAA");
    db_insert(1, 3, "AAA");

    std::thread worker(func);
    worker.detach();

    while(1);
}
