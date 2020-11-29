#include "log.h"

#include "common.h"

Log::Log(xact_id xid, std::size_t lsn, std::size_t last_lsn)
    : xid_(xid), lsn_(lsn), last_lsn_(last_lsn)
{
}

LogType Log::type() const
{
    return LogType::INVALID;
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

bool LogManager::initialize()
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) LogManager;
    CHECK_FAILURE(instance_ != nullptr);

    // just for making one-based index
    instance_->log_.emplace_back(nullptr);

    return true;
}

bool LogManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    delete instance_;
    instance_ = nullptr;

    return true;
}

LogManager& LogManager::get_instance()
{
    return *instance_;
}

void LogManager::remove(xact_id xid)
{
    std::scoped_lock lock(mutex_);

    log_per_xact_.erase(xid);
}

const std::list<Log*>& LogManager::get(xact_id xid)
{
    std::scoped_lock lock(mutex_);

    return log_per_xact_[xid];
}
