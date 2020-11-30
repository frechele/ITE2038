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

std::mutex mut;
int trx_count = 0;

int trx_begin_wrap()
{
    std::scoped_lock lock(mut);

    ++trx_count;

    const int ret = trx_begin();
    assert(ret == trx_count);

    return ret;
}

void test1()
{
    int trx_id = trx_begin_wrap();

    assert(trx_commit(trx_id) == trx_id);
}

void test2(int tid)
{
    int trx_id = trx_begin_wrap();

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx_id)));

    assert(trx_commit(trx_id) == trx_id);
}

void test3(int tid)
{
    int trx_id = trx_begin_wrap();

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
    int trx1 = trx_begin_wrap();

    char str_update_by_trx1_1[120] = "UPDATE_BY_TRX1_1";
    char str_update_by_trx1_2[120] = "UPDATE_BY_TRX1_2";
    char value[120];

    std::thread worker2([tid] {
        int trx2 = trx_begin_wrap();

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

    assert(SUCCESSED(db_find(tid, 1, value, trx1)));
    assert(strcmp(value, str_update_by_trx1_1) == 0);
    assert(SUCCESSED(db_find(tid, 2, value, trx1)));
    assert(strcmp(value, str_update_by_trx1_2) == 0);

    assert(trx_commit(trx1) == trx1);

    if (worker2.joinable())
        worker2.join();
}

void test5(int tid)
{
    int trx1 = trx_begin_wrap();

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx1)));

    std::thread worker2([tid] {
        int trx2 = trx_begin_wrap();

        char str_update_by_trx2[120] = "UPDATE_BY_TRX2";
        assert(SUCCESSED(db_update(tid, 1, str_update_by_trx2, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(trx_commit(trx2) == trx2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    assert(SUCCESSED(db_find(tid, 1, value, trx1)));

    assert(trx_commit(trx1) == trx1);

    if (worker2.joinable())
        worker2.join();
}

void test6(int tid)
{
    int trx1 = trx_begin_wrap();

    char value[120];
    char str_update_by_trx2[120] = "UPDATE_BY_TRX1";
    assert(SUCCESSED(db_update(tid, 1, str_update_by_trx2, trx1)));

    std::thread worker2([tid] {
        int trx2 = trx_begin_wrap();

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(trx_commit(trx2) == trx2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    assert(SUCCESSED(db_find(tid, 1, value, trx1)));

    assert(trx_commit(trx1) == trx1);

    if (worker2.joinable())
        worker2.join();
}

void test7(int tid)
{
    int trx1 = trx_begin_wrap();

    char value[120];

    char str_update_origin[120] = "THIS_IS_ORIGIN";

    {
        int read_trx = trx_begin_wrap();
        assert(SUCCESSED(db_update(tid, 3, str_update_origin, read_trx)));
        assert(SUCCESSED(db_update(tid, 4, str_update_origin, read_trx)));

        assert(trx_commit(read_trx) == read_trx);
    }

    char str_update_will_be_rollbacked[120] = "WILL_BE_ROLLBACKED";

    std::thread worker2([tid, &str_update_will_be_rollbacked] {
        int trx2 = trx_begin_wrap();

        assert(
            SUCCESSED(db_update(tid, 3, str_update_will_be_rollbacked, trx2)));
        assert(
            SUCCESSED(db_update(tid, 4, str_update_will_be_rollbacked, trx2)));

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx2)));

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        assert(FAILED(db_update(tid, 2, str_update_will_be_rollbacked, trx2)));
        assert(FAILED(db_find(tid, 2, value, trx2)));

        assert(trx_commit(trx2) == 0);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    char str_update_will_be_not_rollbacked[120] = "WILL_BE_NOT_ROLLBACKED";
    assert(
        SUCCESSED(db_update(tid, 2, str_update_will_be_not_rollbacked, trx1)));
    assert(
        SUCCESSED(db_update(tid, 1, str_update_will_be_not_rollbacked, trx1)));

    assert(trx_commit(trx1) == trx1);

    if (worker2.joinable())
        worker2.join();

    {
        int read_trx = trx_begin_wrap();
        assert(SUCCESSED(db_find(tid, 3, value, read_trx)));

        assert(trx_commit(read_trx) == read_trx);

        assert(strcmp(str_update_origin, value) == 0);
        assert(strcmp(value, str_update_will_be_rollbacked) != 0);
    }

    {
        int read_trx = trx_begin_wrap();
        assert(SUCCESSED(db_find(tid, 4, value, read_trx)));

        assert(trx_commit(read_trx) == read_trx);

        assert(strcmp(str_update_origin, value) == 0);
        assert(strcmp(value, str_update_will_be_rollbacked) != 0);
    }
}

void test8(int tid)
{
    char str_update2[120] = "UPDATE_BY_TRX1_2";

    std::thread worker1([tid, &str_update2] {
        int trx = trx_begin_wrap();

        char str_update[120] = "UPDATE_BY_TRX1";
        assert(SUCCESSED(db_update(tid, 1, str_update, trx)));

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx)));

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        assert(SUCCESSED(db_update(tid, 1, str_update2, trx)));

        assert(trx_commit(trx) == trx);
    });

    std::thread worker2([tid] {
        int trx = trx_begin_wrap();

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx)));

        assert(trx_commit(trx) == trx);
    });

    std::thread worker3([tid] {
        int trx = trx_begin_wrap();

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx)));

        assert(trx_commit(trx) == trx);
    });

    if (worker1.joinable())
        worker1.join();
    if (worker2.joinable())
        worker2.join();
    if (worker3.joinable())
        worker3.join();

    int trx = trx_begin_wrap();

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx)));

    assert(trx_commit(trx) == trx);

    assert(strcmp(value, str_update2) == 0);
}

void test9(int tid)
{
    char str_update2[120] = "UPDATE_BY_TRX2_TEST9";

    std::thread worker1([tid] {
        int trx = trx_begin_wrap();

        char value[120];
        assert(SUCCESSED(db_find(tid, 1, value, trx)));

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        char str_update[120] = "WILL_BE_ROLLBACKED";
        assert(FAILED(db_update(tid, 1, str_update, trx)));

        assert(trx_commit(trx) == 0);
    });

    std::thread worker2([tid, &str_update2] {
        int trx = trx_begin_wrap();

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        assert(SUCCESSED(db_update(tid, 1, str_update2, trx)));

        assert(trx_commit(trx) == trx);
    });

    if (worker1.joinable())
        worker1.join();
    if (worker2.joinable())
        worker2.join();

    int trx = trx_begin_wrap();

    char value[120];
    assert(SUCCESSED(db_find(tid, 1, value, trx)));

    assert(trx_commit(trx) == trx);

    assert(strcmp(value, str_update2) == 0);
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
              << " ms\n"
              << endl;
}

int main()
{
    char log_file[] = "log.data";
    char logmsg_file[] = "log.txt";

    assert(SUCCESSED(init_db(10, 1, 100, log_file, logmsg_file)));

    const int tid = open_table("DATA1");
    assert(tid == 1);

    // GENERATE SAMPLE DB
    {
        for (int i = 0; i < 10000; ++i)
        {
            std::string value = "INIT_VALUE_" + i;
            assert(SUCCESSED(db_insert(tid, i, &value[0])));
        }
    }

    // TEST CASES
    do_test("only trx begin and commit", test1);
    do_test("trx with find", test2, tid);
    do_test("multiple find and update", test3, tid);
    do_test("deadlock detection (TC in ppt) - deadlock", test4, tid);
    do_test("deadlock detection (S-X-S) - no deadlock", test5, tid);
    do_test("deadlock detection (X-S-S) - no deadlock", test6, tid);
    do_test("rollback", test7, tid);
    do_test("deadlock detection (X-S-S-S-X) - no deadlock", test8, tid);
    do_test("deadlock detection (S-X-X) - deadlock", test9, tid);

    assert(SUCCESSED(shutdown_db()));

    cout << "<<< ALL PASSED >>>" << endl;
}
