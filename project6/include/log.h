#ifndef LOG_H_
#define LOG_H_

#include "file.h"
#include "types.h"

#include <deque>
#include <fstream>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>

enum class LogType
{
    INVALID = -1,
    BEGIN,
    UPDATE,
    COMMIT,
    ROLLBACK,
    COMPENSATE,
    CONSIDER_REDO
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
                      HierarchyID hid, std::size_t length, page_data_t old_data,
                      page_data_t new_data)
        : LogWithoutRecordBase<LogT>(xid, lsn, last_lsn),
          hid_(std::move(hid)),
          length_(length),
          old_data_(std::move(old_data)),
          new_data_(std::move(new_data))
    {
    }

    HierarchyID hid() const
    {
        return hid_;
    }

    std::size_t length() const
    {
        return length_;
    }

    const page_data_t& old_data() const
    {
        return old_data_;
    }

    const page_data_t& new_data() const
    {
        return new_data_;
    }

 private:
    HierarchyID hid_;
    std::size_t length_;
    page_data_t old_data_, new_data_;
};

using LogBegin = LogWithoutRecordBase<LogType::BEGIN>;
inline std::ostream& operator<<(std::ostream& os, const LogBegin& log)
{
    os << "LSN " << log.lsn() << " [BEGIN] Transaction id " << log.xid();
    return os;
}

using LogCommit = LogWithoutRecordBase<LogType::COMMIT>;
inline std::ostream& operator<<(std::ostream& os, const LogCommit& log)
{
    os << "LSN " << log.lsn() << " [COMMIT] Transaction id " << log.xid();
    return os;
}

using LogRollback = LogWithoutRecordBase<LogType::ROLLBACK>;
inline std::ostream& operator<<(std::ostream& os, const LogRollback& log)
{
    os << "LSN " << log.lsn() << " [ROLLBACK] Transaction id " << log.xid();
    return os;
}

using LogUpdate = LogWithRecordBase<LogType::UPDATE>;
inline std::ostream& operator<<(std::ostream& os, const LogUpdate& log)
{
    os << "LSN " << log.lsn() << " [UPDATE] Transaction id " << log.xid();
    return os;
}

using LogConsiderRedo = LogWithoutRecordBase<LogType::CONSIDER_REDO>;
inline std::ostream& operator<<(std::ostream& os, const LogConsiderRedo& log)
{
    os << "LSN " << log.lsn() << " [CONSIDER-REDO] Transaction id "
       << log.xid();
    return os;
}

class LogCompensate : public LogWithRecordBase<LogType::COMPENSATE>
{
 public:
    LogCompensate(xact_id xid, std::size_t lsn, std::size_t last_lsn,
                  HierarchyID hid, std::size_t length, page_data_t old_data,
                  page_data_t new_data, std::size_t next_undo_lsn)
        : LogWithRecordBase<LogType::COMPENSATE>(
              xid, lsn, last_lsn, std::move(hid), length, std::move(old_data),
              std::move(new_data)),
          next_undo_lsn_(next_undo_lsn)
    {
    }

    std::size_t next_undo_lsn() const
    {
        return next_undo_lsn_;
    }

 private:
    std::size_t next_undo_lsn_;
};
inline std::ostream& operator<<(std::ostream& os, const LogCompensate& log)
{
    os << "LSN " << log.lsn() << " [CLR] next undo lsn " << log.next_undo_lsn();
    return os;
}

class LogManager final
{
 public:
    [[nodiscard]] static bool initialize(const std::string& logmsg_path);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LogManager& get_instance();

    template <class LogT, typename... Args>
    void log(xact_id xid, Args&&... args);

    void remove(xact_id xid);

    const std::list<Log*>& get(xact_id xid);

 private:
    LogManager() = default;

 private:
    mutable std::mutex mutex_;
    std::deque<std::unique_ptr<Log>> log_;
    std::unordered_map<xact_id, std::list<Log*>> log_per_xact_;

    std::ofstream f_logmsg_;

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

    std::size_t last_lsn = 0;
    if (!log_per_xact_[xid].empty())
        last_lsn = log_per_xact_[xid].back()->lsn();

    log_.emplace_back(std::make_unique<LogT>(xid, log_.size(), last_lsn,
                                             std::forward<Args>(args)...));
    log_per_xact_[xid].emplace_back(log_.back().get());
    f_logmsg_ << *log_.back() << std::endl;
}

#endif  // LOG_H_
