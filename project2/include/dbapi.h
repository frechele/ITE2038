#ifndef __DBAPI_H__
#define __DBAPI_H__

#include <stdint.h>

int open_table(char* pathname);

int db_insert(int64_t key, char* value);

int db_fild(int64_t key, char* ret_val);

int db_delete(int64_t key);

#endif  // __DBAPI_H__
