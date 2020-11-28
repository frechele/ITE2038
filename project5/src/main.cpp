#include <cassert>
#include <iostream>
#include <string>
#include <cstring>

#include <chrono>
#include <thread>

#include "dbapi.h"

using std::cin;
using std::cout;
using std::endl;

#define SUCCESSED(cond) ((cond) == 0)
#define FAILED(cond)   ((cond) != 0)

void test1()
{
    int trx_id = trx_begin();

    assert(trx_commit(trx_id) == trx_id);
}

void test2(int tid)
{
    int trx_id = trx_begin();
    assert(trx_id == 2);

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx_id)));

    assert(trx_commit(trx_id) == trx_id);
}

void test3(int tid)
{
    int trx_id = trx_begin();
    assert(trx_id == 3);

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx_id)));
    assert(SUCCESSED(db_find(tid, 2, value, trx_id)));
    assert(SUCCESSED(db_find(tid, 3, value, trx_id)));

    {
        char tmp[120] = "Hello World! 1";
        assert(SUCCESSED(db_update(tid, 1, tmp, trx_id)));
        assert(SUCCESSED(db_find(tid, 1, value, trx_id)));
        assert(strcmp(tmp, value) == 0);
    }

    {
        char tmp[120] = "Hello World! 2";
        assert(SUCCESSED(db_update(tid, 2, tmp, trx_id)));
        assert(SUCCESSED(db_find(tid, 2, value, trx_id)));
        assert(strcmp(tmp, value) == 0);
    }

    {
        char tmp[120] = "Hello World! 3";
        assert(SUCCESSED(db_update(tid, 3, tmp, trx_id)));
        assert(SUCCESSED(db_find(tid, 3, value, trx_id)));
        assert(strcmp(tmp, value) == 0);
    }

    assert(trx_commit(trx_id) == trx_id);
}

void test4(int tid)
{
    int trx1 = trx_begin();
    assert(trx1 == 4);

    char str_update_by_trx1_1[120] = "UPDATE_BY_TRX1_1";
    char str_update_by_trx1_2[120] = "UPDATE_BY_TRX1_2";
    char value[120];

    std::thread worker2([tid] {
        int trx2 = trx_begin();
        assert(trx2 == 5);

        char value[120];
        char str_update_by_trx2_2[120] = "UPDATE_BY_TRX2_2";

        assert(SUCCESSED(db_find(tid, 1, value, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(FAILED(db_update(tid, 2, str_update_by_trx2_2, trx2)));
        assert(FAILED(db_find(tid, 2, value, trx2)));
        assert(trx_commit(trx2) == 0);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    assert(SUCCESSED(db_update(tid, 2, str_update_by_trx1_2, trx1)));
    assert(SUCCESSED(db_update(tid, 1, str_update_by_trx1_1, trx1)));

    if (worker2.joinable())
        worker2.join();

    assert(SUCCESSED(db_find(tid, 1, value, trx1)));
    assert(strcmp(value, str_update_by_trx1_1) == 0);
    assert(SUCCESSED(db_find(tid, 2, value, trx1)));
    assert(strcmp(value, str_update_by_trx1_2) == 0);

    assert(trx_commit(trx1) == trx1);
}

void test5(int tid)
{
    int trx1 = trx_begin();
    assert(trx1 == 6);

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx1)));

    std::thread worker2([tid] {
        int trx2 = trx_begin();
        assert(trx2 == 7);

        char str_update_by_trx2[120] = "UPDATE_BY_TRX2";
        assert(SUCCESSED(db_update(tid, 1, str_update_by_trx2, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(trx_commit(trx2) == trx2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    assert(FAILED(db_find(tid, 1, value, trx1)));

    if (worker2.joinable())
        worker2.join();

    assert(trx_commit(trx1) == 0);
}

void test6(int tid)
{
    int trx1 = trx_begin();
    assert(trx1 == 8);

    char value[120];
    char str_update_by_trx2[120] = "UPDATE_BY_TRX1";
    assert(SUCCESSED(db_update(tid, 1, str_update_by_trx2, trx1)));

    std::thread worker2([tid] {
        int trx2 = trx_begin();
        assert(trx2 == 9);

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(trx_commit(trx2) == trx2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    assert(SUCCESSED(db_find(tid, 1, value, trx1)));

    if (worker2.joinable())
        worker2.join();

    assert(trx_commit(trx1) == trx1);
}

template <typename Func, typename... Args>
void do_test(const std::string& desc, Func&& func, Args&&... args)
{
    std::cerr << "[test] " << desc << "\n";

    const auto start_point = std::chrono::system_clock::now();

    func(std::forward<Args>(args)...);

    const auto end_point = std::chrono::system_clock::now();

    std::cerr << "time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_point - start_point)
                        .count()
                << " ms\n" << endl;
}

int main()
{
    assert(SUCCESSED(init_db(1000)));

    const int tid = open_table("test.db");
    assert(tid != -1);

    // GENERATE SAMPLE DB
    {
        for (int i = 0; i < 100; ++i)
        {
            std::string value = "INIT_VALUE_" + i;
            assert(SUCCESSED(db_insert(tid, i, &value[0])));
        }
    }

    // TEST CASES
    do_test("only trx begin and commit", test1);
    do_test("trx with find", test2, tid);
    do_test("multiple find and update", test3, tid);
    do_test("deadlock detection (TC in ppt)", test4, tid);
    do_test("deadlock detection (S-X-S)", test5, tid);
    do_test("deadlock detection (X-S-S)", test6, tid);

    assert(SUCCESSED(shutdown_db()));

    cout << "<<< ALL PASSED >>>" << endl;
}
