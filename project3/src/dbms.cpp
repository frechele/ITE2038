#include "dbms.h"

#include "buffer.h"
#include "common.h"
#include "file.h"

#include <cassert>

DBMS::~DBMS()
{
    (void)shutdown();
}

DBMS& DBMS::get()
{
    static DBMS instance;

    return instance;
}

bool DBMS::initialize(int num_buf)
{
    assert(buf_mgr_ == nullptr);
    assert(tbl_mgr_ == nullptr);

    buf_mgr_ = new (std::nothrow) BufferManager();
    CHECK_FAILURE(buf_mgr_ != nullptr);    

    tbl_mgr_ = new (std::nothrow) TableManager();
    CHECK_FAILURE(tbl_mgr_ != nullptr);

    return buf_mgr_->initialize(num_buf);
}

bool DBMS::shutdown()
{
    if (buf_mgr_ == nullptr)
        return true;

    CHECK_FAILURE(buf_mgr_->shutdown());

    delete buf_mgr_;
    buf_mgr_ = nullptr;

    delete tbl_mgr_;
    tbl_mgr_ = nullptr;

    return true;
}

BufferManager& DBMS::get_buffer_manager()
{
    return *buf_mgr_;
}

TableManager& DBMS::get_table_manager()
{
    return *tbl_mgr_;
}
