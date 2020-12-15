#include "dbapi.h"

#include "common.h"
#include "lock.h"
#include "log.h"
#include "table.h"
#include "xact.h"

#include <cstring>

#include <iostream>

int init_db(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path)
{
    CHECK_FAILURE2(LockManager::initialize(), FAIL);
    CHECK_FAILURE2(LogManager::initialize(std::string(log_path), std::string(logmsg_path)), FAIL);
    CHECK_FAILURE2(XactManager::initialize(), FAIL);
    CHECK_FAILURE2(TableManager::initialize(num_buf), FAIL);

    return SUCCESS;
}

int shutdown_db()
{
    CHECK_FAILURE2(TableManager::shutdown(), FAIL);
    CHECK_FAILURE2(XactManager::shutdown(), FAIL);
    CHECK_FAILURE2(LogManager::shutdown(), FAIL);
    CHECK_FAILURE2(LockManager::shutdown(), FAIL);

    return SUCCESS;
}

int open_table(char* pathname)
{
    CHECK_FAILURE2(TableManager::is_initialized(), -1);

    if (auto table_id = TblMgr().open_table(pathname); table_id.has_value())
        return table_id.value();

    return -1;
}

int close_table(int table_id)
{
    CHECK_FAILURE2(TableManager::is_initialized(), FAIL);

    return TblMgr().close_table(table_id) ? SUCCESS : FAIL;
}

int db_insert(int table_id, int64_t key, char* value)
{
    CHECK_FAILURE2(TableManager::is_initialized(), FAIL);

    auto table = TblMgr().get_table(table_id);
    CHECK_FAILURE2(table.has_value(), FAIL);

    page_data_t record;
    record.key = key;
    strncpy(record.value, value, PAGE_DATA_VALUE_SIZE);

    CHECK_FAILURE2(table.value()->insert(record), FAIL);

    return SUCCESS;
}

int db_find(int table_id, int64_t key, char* ret_val, int trx_id)
{
    CHECK_FAILURE2(TableManager::is_initialized(), FAIL);

    auto table = TblMgr().get_table(table_id);
    CHECK_FAILURE2(table.has_value(), FAIL);

    Xact* xact = XactMgr().get(trx_id);
    CHECK_FAILURE2(xact != nullptr, FAIL);

    auto res = table.value()->find(key, xact);
    CHECK_FAILURE2(res.has_value(), FAIL);

    strncpy(ret_val, res.value().value, PAGE_DATA_VALUE_SIZE);

    return SUCCESS;
}

int db_delete(int table_id, int64_t key)
{
    CHECK_FAILURE2(TableManager::is_initialized(), FAIL);

    auto table = TblMgr().get_table(table_id);
    CHECK_FAILURE2(table.has_value(), FAIL);

    CHECK_FAILURE2(table.value()->remove(key), FAIL);

    return SUCCESS;
}

int db_update(int table_id, int64_t key, char* value, int trx_id)
{
    CHECK_FAILURE2(TableManager::is_initialized(), FAIL);

    auto table = TblMgr().get_table(table_id);
    CHECK_FAILURE2(table.has_value(), FAIL);

    Xact* xact = XactMgr().get(trx_id);
    CHECK_FAILURE2(xact != nullptr, FAIL);

    CHECK_FAILURE2(table.value()->update(key, value, xact), FAIL);

    return SUCCESS;
}

int trx_begin()
{
    CHECK_FAILURE2(TableManager::is_initialized(), 0);

    Xact* xact = XactMgr().begin();
    CHECK_FAILURE2(xact != nullptr, 0);

    return xact->id();
}

int trx_commit(int trx_id)
{
    CHECK_FAILURE2(TableManager::is_initialized(), 0);

    Xact* xact = XactMgr().get(trx_id);
    CHECK_FAILURE2(xact != nullptr, 0);

    CHECK_FAILURE2(XactMgr().commit(xact), 0);

    return trx_id;
}

int trx_abort(int trx_id)
{
    CHECK_FAILURE2(TableManager::is_initialized(), 0);

    Xact* xact = XactMgr().get(trx_id);
    CHECK_FAILURE2(xact != nullptr, 0);

    CHECK_FAILURE2(XactMgr().abort(xact), 0);

    return trx_id;
}
