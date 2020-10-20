#include <fstream>
#include <iostream>
#include <string>

#include "common.h"
#include "dbapi.h"

using std::cin;
using std::cout;
using std::endl;

int main()
{
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
                cout << "open table " << arg << "(id : " << table_id << ")." << endl;
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
    }
}
