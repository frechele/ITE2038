#include <iostream>
#include <string>

#include "common.h"
#include "dbapi.h"

using std::cout;
using std::cin;
using std::endl;

int main([[maybe_unused]] int argc, [[maybe_unused]] char** argv) {
    bool is_running = true;

    while (!cin.eof() && is_running) {
        std::string cmd;
        cin >> cmd;

        if (cmd == "open") {
            std::string arg;
            cin >> arg;

            open_table(&arg[0]);
        } else if (cmd == "quit") {
            cout << "dbms will be shutdown." << endl;

            is_running = false;
        } else if (cmd == "insert") {
            int64_t key;
            std::string value;
            cin >> key;
            std::getline(cin, value);

            if (SUCCESSED(db_insert(key, &value[0]))) {
                cout << "insert " << key << " successed." << endl;
            } else {
                cout << "insert " << key << " failed." << endl;
            }
        } else if (cmd == "find") {
            int64_t key;
            cin >> key;

            char value[120];
            if (SUCCESSED(db_find(key, value))) {
                cout << key << " is " << value << '.' << endl;
            } else {
                cout << key << " is not found." << endl;
            }
        } else if (cmd == "delete") {
            int64_t key;
            cin >> key;

            if (SUCCESSED(db_delete(key))) {
                cout << "delete " << key << " successed." << endl;
            } else {
                cout << "delete " << key << " failed." << endl;
            }
        }
    }
}
