#ifndef DBAPI_H_
#define DBAPI_H_

#include <cstdint>

extern "C"
{
int open_table(char* pathname);

int db_insert(int64_t key, char* value);

int db_find(int64_t key, char* ret_val);

int db_delete(int64_t key);
}

#endif  // DBAPI_H_
