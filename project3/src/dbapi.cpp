#include "dbapi.h"

#include "common.h"
#include "table.h"

#include <cstring>

#include <iostream>

int init_db(int num_buf)
{
    CHECK_FAILURE2(TableManager::initialize(num_buf), FAIL);

    return SUCCESS;
}

int shutdown_db()
{
    CHECK_FAILURE2(TableManager::shutdown(), FAIL);

    return SUCCESS;
}

int open_table(char* pathname)
{
    if (auto table_id = TblMgr().open_table(pathname); table_id.has_value())
        return table_id.value();

    return -1;
}

int close_table(int table_id)
{
    return TblMgr().close_table(table_id) ? SUCCESS : FAIL;
}

int db_insert(int table_id, int64_t key, char* value)
{
    auto table = TblMgr().get_table(table_id);

    if (!table.has_value())
        return FAIL;

    page_data_t record;
    record.key = key;
    strncpy(record.value, value, PAGE_DATA_VALUE_SIZE);

    return table.value()->insert(record) ? SUCCESS : FAIL;
}

int db_find(int table_id, int64_t key, char* ret_val)
{
    auto table = TblMgr().get_table(table_id);

    if (!table.has_value())
        return FAIL;

    auto res = table.value()->find(key);
    if (!res)
        return FAIL;

    strncpy(ret_val, res.value().value, PAGE_DATA_VALUE_SIZE);
    return SUCCESS;
}

int db_delete(int table_id, int64_t key)
{
    auto table = TblMgr().get_table(table_id);

    if (!table.has_value())
        return FAIL;

    return table.value()->remove(key) ? SUCCESS : FAIL;
}
