#include "dbapi.h"

#include "common.h"
#include "file.h"

int open_table(char* pathname)
{
    static int TABLE_COUNT = 0;

    if (!FileManager::get().open(pathname))
        return -1;

    return TABLE_COUNT++;
}

int db_insert(int64_t key, char* value)
{
    if (!FileManager::get().is_open())
        return FAIL;

    return FAIL;
}

int db_find(int64_t key, char* ret_val)
{
    if (!FileManager::get().is_open())
        return FAIL;

    return FAIL;
}

int db_delete(int64_t key)
{
    if (!FileManager::get().is_open())
        return FAIL;

    return FAIL;
}
