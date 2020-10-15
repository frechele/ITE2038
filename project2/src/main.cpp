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

        if (cmd == "open")
        {
            std::string arg;
            in_stream >> arg;

            if (open_table(&arg[0]) == -1)
            {
                cout << "cannot open table " << arg << '.' << endl;
            }
            else
            {
                cout << "open table " << arg << '.' << endl;
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
            in_stream >> key;
            in_stream.get();
            std::getline(in_stream, value);

            if (SUCCESSED(db_insert(key, &value[0])))
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
            in_stream >> key;

            char value[120];
            if (SUCCESSED(db_find(key, value)))
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
            in_stream >> key;

            if (SUCCESSED(db_delete(key)))
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
