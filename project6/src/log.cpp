#include "log.h"

#include "common.h"

#include <fcntl.h>
#include <memory.h>
#include <unistd.h>
#include <cassert>

Log Log::create_begin(xact_id xid, lsn_t lsn)
{
    Log log;

    log.type_ = LogType::BEGIN;
    log.size_ = sizeof(int) + sizeof(lsn_t) + sizeof(lsn_t) + sizeof(xact_id) +
                sizeof(LogType);

    log.xid_ = xid;
    log.lsn_ = lsn;
    log.last_lsn_ = NULL_LSN;

    return log;
}

Log Log::create_commit(xact_id xid, lsn_t lsn, lsn_t last_lsn)
{
    Log log;

    log.type_ = LogType::COMMIT;
    log.size_ = sizeof(int) + sizeof(lsn_t) + sizeof(lsn_t) + sizeof(xact_id) +
                sizeof(LogType);

    log.xid_ = xid;
    log.lsn_ = lsn;
    log.last_lsn_ = last_lsn;

    return log;
}

Log Log::create_update(xact_id xid, lsn_t lsn, lsn_t last_lsn,
                       const HierarchyID& hid, int length, const void* old_data,
                       const void* new_data)
{
    Log log;

    log.type_ = LogType::UPDATE;
    log.size_ = sizeof(int) + sizeof(lsn_t) + sizeof(lsn_t) + sizeof(xact_id) +
                sizeof(LogType) + sizeof(table_id_t) + sizeof(pagenum_t) +
                sizeof(int) + sizeof(int) + length + length;

    log.xid_ = xid;
    log.lsn_ = lsn;
    log.last_lsn_ = last_lsn;

    log.tid_ = hid.table_id;
    log.pid_ = hid.pagenum;
    log.offset_ = PAGE_HEADER_SIZE + hid.offset * PAGE_DATA_SIZE + 8;
    log.length_ = length;
    memcpy(log.old_data_, old_data, length);
    memcpy(log.new_data_, new_data, length);

    return log;
}

Log Log::create_rollback(xact_id xid, lsn_t lsn, lsn_t last_lsn)
{
    Log log;

    log.type_ = LogType::ROLLBACK;
    log.size_ = sizeof(int) + sizeof(lsn_t) + sizeof(lsn_t) + sizeof(xact_id) +
                sizeof(LogType);

    log.xid_ = xid;
    log.lsn_ = lsn;
    log.last_lsn_ = last_lsn;

    return log;
}

Log Log::create_compensate(xact_id xid, lsn_t lsn, lsn_t last_lsn,
                           const HierarchyID& hid, int length,
                           const void* old_data, const void* new_data,
                           lsn_t next_undo_lsn)
{
    Log log;

    log.type_ = LogType::COMPENSATE;
    log.size_ = sizeof(int) + sizeof(lsn_t) + sizeof(lsn_t) + sizeof(xact_id) +
                sizeof(LogType) + sizeof(table_id_t) + sizeof(pagenum_t) +
                sizeof(int) + sizeof(int) + length + length + sizeof(lsn_t);

    log.xid_ = xid;
    log.lsn_ = lsn;
    log.last_lsn_ = last_lsn;

    log.tid_ = hid.table_id;
    log.pid_ = hid.pagenum;
    log.offset_ = PAGE_HEADER_SIZE + hid.offset * PAGE_DATA_SIZE + 8;
    log.length_ = length;
    memcpy(log.old_data_, old_data, length);
    memcpy(log.new_data_, new_data, length);

    log.next_undo_lsn_ = next_undo_lsn;

    return log;
}

LogType Log::type() const
{
    return type_;
}

xact_id Log::xid() const
{
    return xid_;
}

lsn_t Log::lsn() const
{
    return lsn_;
}

lsn_t Log::last_lsn() const
{
    return last_lsn_;
}

int Log::size() const
{
    return size_;
}

table_id_t Log::table_id() const
{
    return tid_;
}

pagenum_t Log::pagenum() const
{
    return pid_;
}

int Log::offset() const
{
    return offset_;
}

int Log::length() const
{
    return length_;
}

const void* Log::old_data() const
{
    return old_data_;
}

const void* Log::new_data() const
{
    return new_data_;
}

lsn_t Log::next_undo_lsn() const
{
    return next_undo_lsn_;
}

bool LogManager::initialize(const std::string& log_path)
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) LogManager;
    CHECK_FAILURE(instance_ != nullptr);

    const bool create_new = (access(log_path.c_str(), F_OK) == -1);

    CHECK_FAILURE(
        (instance_->f_log_ = open(
             log_path.c_str(), O_RDWR | O_CREAT | O_DSYNC,
             S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) != -1);

    if (!create_new)
    {
        CHECK_FAILURE(pread(instance_->f_log_, &instance_->header_,
                            sizeof(log_file_header), 0) > 0);
    }

    return true;
}

bool LogManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    close(instance_->f_log_);
    instance_->f_log_ = -1;

    delete instance_;
    instance_ = nullptr;

    return true;
}

LogManager& LogManager::get_instance()
{
    return *instance_;
}

lsn_t LogManager::log_begin(xact_id xid)
{
    return logging([&](lsn_t lsn) { append_log(Log::create_begin(xid, lsn)); });
}

lsn_t LogManager::log_commit(xact_id xid, lsn_t last_lsn)
{
    return logging([&](lsn_t lsn) {
        append_log(Log::create_commit(xid, lsn, last_lsn));

        assert(force());
    });
}

lsn_t LogManager::log_update(xact_id xid, lsn_t last_lsn,
                             const HierarchyID& hid, int length,
                             page_data_t old_data, page_data_t new_data)
{
    return logging([&](lsn_t lsn) {
        append_log(Log::create_update(xid, lsn, last_lsn, std::move(hid),
                                      length, old_data.value, new_data.value));
    });
}

lsn_t LogManager::log_rollback(xact_id xid, lsn_t last_lsn)
{
    return logging([&](lsn_t lsn) {
        append_log(Log::create_rollback(xid, lsn, last_lsn));
    });
}

lsn_t LogManager::log_compensate(xact_id xid, lsn_t last_lsn,
                                 const HierarchyID& hid, int length,
                                 const void* old_data, const void* new_data,
                                 lsn_t next_undo_lsn)
{
    return logging([&](lsn_t lsn) {
        append_log(Log::create_compensate(xid, lsn, last_lsn, hid, length,
                                          old_data, new_data, next_undo_lsn),
                   false);
    });
}

const std::list<std::unique_ptr<Log>>& LogManager::get(xact_id xid) const
{
    std::scoped_lock lock(mutex_);

    return log_per_xact_.at(xid);
}

void LogManager::remove(xact_id xid)
{
    std::scoped_lock lock(mutex_);

    log_per_xact_.erase(xid);
}

bool LogManager::find_page(table_id_t tid, pagenum_t pid) const
{
    std::scoped_lock lock(mutex_);

    for (auto& log : log_)
    {
        if (Log::HasRecord(log->type()))
        {
            if (log->table_id() == tid && log->pagenum() == pid)
                return true;
        }
    }

    return false;
}

bool LogManager::force()
{
    std::scoped_lock lock(mutex_);

    CHECK_FAILURE(pwrite(f_log_, &header_, sizeof(log_file_header), 0) != -1);

    for (const auto& log : log_)
    {
        const std::size_t size = log->size();

        CHECK_FAILURE(pwrite(f_log_, log.get(), size,
                             log->lsn() + sizeof(log_file_header)) != -1);
    }

    fsync(f_log_);

    log_.clear();

    return true;
}

lsn_t LogManager::base_lsn() const
{
    return header_.base_lsn;
}

lsn_t LogManager::next_lsn() const
{
    return header_.next_lsn;
}

void LogManager::truncate_log()
{
    ftruncate(f_log_, sizeof(log_file_header));
}

Log LogManager::read_log(lsn_t lsn) const
{
    return read_log_offset(lsn - header_.base_lsn + sizeof(log_file_header));
}

Log LogManager::read_log_offset(lsn_t offset) const
{
    int size;
    pread(f_log_, &size, 4, offset);

    Log result;
    pread(f_log_, &result, size, offset);

    return result;
}
