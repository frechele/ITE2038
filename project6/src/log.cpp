#include "log.h"

#include "common.h"

#include <fcntl.h>
#include <memory.h>
#include <unistd.h>
#include <cassert>

Log::Log(xact_id xid, LogType type, std::size_t lsn, std::size_t last_lsn)
    : xid_(xid), type_(type), lsn_(lsn), last_lsn_(last_lsn)
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

std::size_t Log::lsn() const
{
    return lsn_;
}

std::size_t Log::last_lsn() const
{
    return last_lsn_;
}

std::size_t Log::size() const
{
    return sizeof(xid_) + sizeof(type_) + sizeof(lsn_) + sizeof(last_lsn_);
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

void LogManager::log_update(Xact* xact, HierarchyID hid, std::size_t length,
                            page_data_t old_data, page_data_t new_data)
{
    logging(xact, [&](std::size_t lsn) {
        append_log(LogUpdate(xact->id(), lsn, xact->last_lsn(), std::move(hid),
                             length, std::move(old_data), std::move(new_data)));
    });
}

void LogManager::log_rollback(Xact* xact)
{
    logging(xact, [&](std::size_t lsn) {
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

std::size_t LogManager::flushed_lsn() const
{
    return flushed_lsn_.load();
}

bool LogManager::find_pagenum(pagenum_t pid) const
{
    std::scoped_lock lock(mutex_);

    const std::size_t size = log_.size();
    for (std::size_t lsn = flushed_lsn(); lsn < size;)
    {
        const Log* log = read_buffer(lsn);

        if (Log::HasRecord(log->type()))
        {
            if (static_cast<const LogUpdate*>(log)->hid().pagenum == pid)
                return true;
        }

        lsn += log->size();
    }

    return false;
}

bool LogManager::force()
{
    std::scoped_lock lock(mutex_);

    const std::size_t size = log_.size();
    pwrite(f_log_, log_.data(), size, flushed_lsn());

    fsync(f_log_);
    log_.clear();
    flushed_lsn_ += size;

    return true;
}

Log* LogManager::read_buffer(std::size_t lsn)
{
    return const_cast<Log*>(std::as_const(*this).read_buffer(lsn));
}

const Log* LogManager::read_buffer(std::size_t lsn) const
{
    return reinterpret_cast<const Log*>(&log_[lsn - flushed_lsn()]);
}
