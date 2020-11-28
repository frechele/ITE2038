#ifndef LOG_H_
#define LOG_H_

#include "file.h"
#include "types.h"

#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

enum class LogType
{
    INVALID,
    UPDATE,
    COMMIT,
    ABORT
};

class Log
{
 public:
    Log(xact_id xid, std::size_t lsn, std::size_t last_lsn);

    Log(const Log&) = delete;
    Log& operator==(const Log&) = delete;

    virtual LogType type() const;
    xact_id xid() const;
    std::size_t lsn() const;
    std::size_t last_lsn() const;

 private:
    xact_id xid_;
    std::size_t lsn_;
    std::size_t last_lsn_;
};

template <LogType LogT>
class LogWithoutRecordBase : public Log
{
 public:
    LogWithoutRecordBase(xact_id xid, std::size_t lsn, std::size_t last_lsn)
        : Log(xid, lsn, last_lsn)
    {
    }

    LogType type() const override
    {
        return LogT;
    }
};

template <LogType LogT>
class LogWithRecordBase : public LogWithoutRecordBase<LogT>
{
 public:
    LogWithRecordBase(xact_id xid, std::size_t lsn, std::size_t last_lsn,
                      pagenum_t pid, int offset, page_data_t old_data,
                      page_data_t new_data)
        : LogWithoutRecordBase<LogT>(xid, lsn, last_lsn),
          pid_(pid),
          offset_(offset),
          old_data_(std::move(old_data)),
          new_data_(std::move(new_data))
    {
    }

    pagenum_t pid() const
    {
        return pid_;
    }

    int offset() const
    {
        return offset_;
    }

    const page_data_t& old_data() const
    {
        return old_data;
    }

    const page_data_t& new_data() const
    {
        return new_data;
    }

 private:
    pagenum_t pid_;
    int offset_;
    page_data_t old_data_, new_data_;
};

using LogCommit = LogWithoutRecordBase<LogType::COMMIT>;
using LogAbort = LogWithoutRecordBase<LogType::ABORT>;
using LogUpdate = LogWithRecordBase<LogType::UPDATE>;

class LogManager final
{
 public:
    [[nodiscard]] static bool initialize();
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LogManager& get_instance();

    template <class LogT, typename... Args>
    void log(xact_id xid, Args&&... args);

    void remove(xact_id xid);

    const std::list<Log*>& get(xact_id xid) const;

 private:
    mutable std::mutex mutex_;
    std::deque<std::unique_ptr<Log>> log_;
    std::unordered_map<xact_id, std::list<Log*>> log_per_xact_;

    inline static LogManager* instance_{ nullptr };
};

inline LogManager& LogMgr()
{
    return LogManager::get_instance();
}

template <class LogT, typename... Args>
void LogManager::log(xact_id xid, Args&&... args)
{
    std::scoped_lock lock(mutex_);

    const std::size_t last_lsn =
        (log_per_xact_.find(xid) == log_per_xact_.end())
            ? 0
            : log_per_xact_[xid].back()->lsn();

    auto log = std::make_unique<LogT>(xid, log_.size(), last_lsn,
                                      std::forward<Args>(args)...);

    if (last_lsn == 0)
    {
        log_per_xact_[xid] = std::list<Log*>();
    }

    log_per_xact_[xid].emplace_back(log.get());
    log_.emplace_back(std::move(log));
}

#endif  // LOG_H_
