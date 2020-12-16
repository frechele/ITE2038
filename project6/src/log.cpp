#include "log.h"

#include "common.h"

#include <fcntl.h>
#include <memory.h>
#include <unistd.h>
#include <cassert>

Log::Log(xact_id xid, LogType type, lsn_t lsn, lsn_t last_lsn, int size)
    : size_(size), lsn_(lsn), last_lsn_(last_lsn), xid_(xid), type_(type)
{
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

bool LogManager::initialize(const std::string& log_path)
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) LogManager;
    CHECK_FAILURE(instance_ != nullptr);

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
        append_log(LogBegin(xact->id(), lsn, NULL_LSN));
    });
}

void LogManager::log_commit(Xact* xact)
{
    logging(xact, [&](std::size_t lsn) {
        append_log(LogCommit(xact->id(), lsn, xact->last_lsn()));

        assert(force());
    });
}

void LogManager::log_update(Xact* xact, const HierarchyID& hid, int length,
                            page_data_t old_data, page_data_t new_data)
{
    logging(xact, [&](int lsn) {
        append_log(LogUpdate(xact->id(), lsn, xact->last_lsn(), std::move(hid),
                             length, old_data.value, new_data.value));
    });
}

void LogManager::log_rollback(Xact* xact)
{
    logging(xact, [&](int lsn) {
        append_log(LogRollback(xact->id(), lsn, xact->last_lsn()));
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
            if (static_cast<const LogUpdate*>(log.get())->pagenum() == pid)
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
