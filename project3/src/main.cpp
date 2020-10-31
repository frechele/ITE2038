#include <cassert>
#include <fstream>
#include <iostream>
#include <string>

// for performance test
#include <algorithm>
#include <chrono>
#include <random>
#include <vector>

#include "common.h"
#include "dbapi.h"
#include "buffer.h"

#include "bpt.h"

using std::cin;
using std::cout;
using std::endl;

#define PERF_TEST

int main()
{
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(0);

    if (FAILED(init_db(3)))
    {
#ifndef PERF_TEST
        cout << "ERROR: cannot init db" << endl;
#endif
        return -1;
    }

#ifndef PERF_TEST
    bool is_running = true;

    std::istream& in_stream = cin;
    // std::istream&& in_stream = std::ifstream("input.txt");

    while (!in_stream.eof() && is_running)
    {
        std::string cmd;
        in_stream >> cmd;

        int table_id;

        if (cmd == "open")
        {
            std::string arg;
            in_stream >> arg;

            table_id = open_table(&arg[0]);
            if (table_id == -1)
            {
                cout << "cannot open table " << arg << '.' << endl;
            }
            else
            {
                cout << "open table " << arg << " (id : " << table_id << ")."
                     << endl;
            }
        }
        else if (cmd == "close")
        {
            cin >> table_id;

            if (SUCCESSED(close_table(table_id)))
            {
                cout << "close table successed." << endl;
            }
            else
            {
                cout << "close table failed." << endl;
            }
        }
        else if (cmd == "quit")
        {
            cout << "dbms will be shutdown." << endl;
            is_running = false;
        }
        else if (cmd == "insert")
        {
            int64_t key;
            std::string value;
            in_stream >> table_id >> key;
            in_stream.get();
            std::getline(in_stream, value);

            if (SUCCESSED(db_insert(table_id, key, &value[0])))
            {
                cout << "insert " << key << " successed." << endl;
            }
            else
            {
                cout << "insert " << key << " failed." << endl;
            }
        }
        else if (cmd == "find")
        {
            int64_t key;
            in_stream >> table_id >> key;

            char value[120];
            if (SUCCESSED(db_find(table_id, key, value)))
            {
                cout << key << " : " << value << endl;
            }
            else
            {
                cout << key << " is not found." << endl;
            }
        }
        else if (cmd == "delete")
        {
            int64_t key;
            in_stream >> table_id >> key;

            if (SUCCESSED(db_delete(table_id, key)))
            {
                cout << "delete " << key << " successed." << endl;
            }
            else
            {
                cout << "delete " << key << " failed." << endl;
            }
        }
        else if (cmd == "sep")
        {
            cout << "sep" << endl;
        }
    }
#else
    const int N = 1'000'000;

    //std::random_device rd;
    std::mt19937 engine(123456);

    const int tid = open_table("/mnt/ssd/dbms/ramdisk/test1.db");

    {
        std::vector<int> keys(N);
        std::iota(begin(keys), end(keys), 1);
        std::shuffle(begin(keys), end(keys), engine);

        std::vector<std::string> values(N);
        std::transform(begin(keys), end(keys), begin(values),
                    [](int k) { return std::to_string(k); });

        const auto start_point = std::chrono::system_clock::now();

        for (int i = 0; i < N; ++i) {
            assert(db_insert(tid, keys[i], &values[i][0]) == SUCCESS);

            if (!BufMgr().check_all_unpinned())
            {
                BufMgr().dump_frame_stat();
                abort();
            }
        }

        const auto end_point = std::chrono::system_clock::now();

        std::cerr << "time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_point - start_point)
                        .count()
                << " ms" << endl;
    }

    {
        const int delete_N = N;

        std::vector<int> keys(delete_N);
        std::iota(begin(keys), end(keys), 1);
        std::shuffle(begin(keys), end(keys), engine);

        const auto start_point = std::chrono::system_clock::now();

        for (int i = 0; i < delete_N; ++i)
        {
            assert(db_delete(tid, keys[i]) == SUCCESS);

            if (!BufMgr().check_all_unpinned())
            {
                BufMgr().dump_frame_stat();
                abort();
            }
        }

        const auto end_point = std::chrono::system_clock::now();

        std::cerr << "time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_point - start_point)
                        .count()
                << " ms" << endl;
    }

#endif
    shutdown_db();
}
