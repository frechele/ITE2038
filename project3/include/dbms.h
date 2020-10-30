#ifndef DBMS_H_
#define DBMS_H_

class BufferManager;
class TableManager;

class DBMS
{
 public:
    static DBMS& get();

    bool initialize(int num_buf);
    bool shutdown();

    BufferManager& get_buffer_manager();
    TableManager& get_table_manager();

 private:
    DBMS() = default;
    ~DBMS();

 private:
    BufferManager* buf_mgr_{ nullptr };
    TableManager* tbl_mgr_{ nullptr };
};

inline BufferManager& BufMgr()
{
   return DBMS::get().get_buffer_manager();
}

inline TableManager& TblMgr()
{
   return DBMS::get().get_table_manager();
}

#endif  // DBMS_H_
