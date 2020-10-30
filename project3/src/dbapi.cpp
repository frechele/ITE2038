#include "dbapi.h"

#include "bpt.h"
#include "common.h"
#include "dbms.h"

#include <cstring>

#include <iostream>

int init_db(int num_buf)
{
    CHECK_FAILURE2(DBMS::get().initialize(num_buf), FAIL);

    return SUCCESS;
}

int shutdown_db()
{
    CHECK_FAILURE2(DBMS::get().shutdown(), FAIL);

    return SUCCESS;
}

int open_table(char* pathname)
{
    if (int table_id = BPTree::open_table(pathname); table_id != -1)
        return table_id;

    return -1;
}

int close_table(int table_id)
{
    return BPTree::close_table(table_id) ? SUCCESS : FAIL;
}

int db_insert(int table_id, int64_t key, char* value)
{
    if (!BPTree::is_open(table_id))
        return FAIL;

    page_data_t record;
    record.key = key;
    strncpy(record.value, value, PAGE_DATA_VALUE_SIZE);

    return BPTree::insert(table_id, record) ? SUCCESS : FAIL;
}

int db_find(int table_id, int64_t key, char* ret_val)
{
    if (!BPTree::is_open(table_id))
        return FAIL;

    auto res = BPTree::find(table_id, key);
    if (!res)
        return FAIL;

    strncpy(ret_val, res.value().value, PAGE_DATA_VALUE_SIZE);
    return SUCCESS;
}

int db_delete(int table_id, int64_t key)
{
    if (!BPTree::is_open(table_id))
        return FAIL;

    return BPTree::remove(table_id, key) ? SUCCESS : FAIL;
}
