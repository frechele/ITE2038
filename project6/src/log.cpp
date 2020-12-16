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
    log.offset_ = PAGE_HEADER_SIZE + hid.offset * length;
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

bool LogManager::initialize(const std::string& log_path,
                            const std::string& logmsg_path)
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) LogManager;
    CHECK_FAILURE(instance_ != nullptr);

    instance_->f_logmsg_.open(logmsg_path, std::ios_base::app);
    CHECK_FAILURE(instance_->f_logmsg_.is_open());

    CHECK_FAILURE(
        (instance_->f_log_ = open(
             log_path.c_str(), O_RDWR | O_CREAT | O_DSYNC,
             S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) != -1);

    return true;
}

bool LogManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    close(instance_->f_log_);
    instance_->f_log_ = -1;

    instance_->f_logmsg_.close();

    delete instance_;
    instance_ = nullptr;

    return true;
}

LogManager& LogManager::get_instance()
{
    return *instance_;
}

void LogManager::log_begin(Xact* xact)
{
    logging(xact, [&](std::size_t lsn) {
        auto log = Log::create_begin(xact->id(), lsn);
        append_log(log);

        return log.size();
    });
}

void LogManager::log_commit(Xact* xact)
{
    logging(xact, [&](std::size_t lsn) {
        auto log = Log::create_commit(xact->id(), lsn, xact->last_lsn());
        append_log(log);

        assert(force());

        return log.size();
    });
}

void LogManager::log_update(Xact* xact, const HierarchyID& hid, int length,
                            page_data_t old_data, page_data_t new_data)
{
    logging(xact, [&](int lsn) {
        auto log =
            Log::create_update(xact->id(), lsn, xact->last_lsn(), hid, length,
                               old_data.value, new_data.value);
        append_log(log);

        return log.size();
    });
}

void LogManager::log_rollback(Xact* xact)
{
    logging(xact, [&](int lsn) {
        auto log = Log::create_rollback(xact->id(), lsn, xact->last_lsn());
        append_log(log);

        return log.size();
    });
}

const std::list<std::unique_ptr<Log>>& LogManager::get(Xact* xact) const
{
    std::scoped_lock lock(mutex_);

    return log_per_xact_.at(xact->id());
}

void LogManager::remove(Xact* xact)
{
    std::scoped_lock lock(mutex_);

    log_per_xact_.erase(xact->id());
}

lsn_t LogManager::flushed_lsn() const
{
    return flushed_lsn_.load();
}

bool LogManager::find_pagenum(pagenum_t pid) const
{
    std::scoped_lock lock(mutex_);

    for (auto& log : log_)
    {
        if (Log::HasRecord(log->type()))
        {
            if (log->pagenum() == pid)
                return true;
        }
    }

    return false;
}

bool LogManager::force()
{
    std::scoped_lock lock(mutex_);

    int flushed = flushed_lsn();
    for (const auto& log : log_)
    {
        const std::size_t size = log->size();

        CHECK_FAILURE(pwrite(f_log_, log.get(), size, flushed) != -1);
        flushed += size;
    }

    fsync(f_log_);

    log_.clear();
    flushed_lsn_ = flushed;

    return true;
}
