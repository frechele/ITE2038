#ifndef LOG_H_
#define LOG_H_

#include "file.h"
#include "types.h"
#include "xact.h"

#include <atomic>
#include <fstream>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <vector>

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

constexpr std::size_t NULL_LSN = 0;

class Log
{
 public:
    [[nodiscard]] static constexpr bool HasRecord(LogType type)
    {
        switch (type)
        {
            case LogType::UPDATE:
            case LogType::COMPENSATE:
                return true;

            default:
                return false;
        }
    }

 public:
    Log() = default;
    Log(xact_id xid, LogType type, std::size_t lsn, std::size_t last_lsn);

    [[nodiscard]] LogType type() const;
    [[nodiscard]] xact_id xid() const;
    [[nodiscard]] std::size_t lsn() const;
    [[nodiscard]] std::size_t last_lsn() const;

    [[nodiscard]] virtual std::size_t size() const;

 private:
    xact_id xid_{ 0 };
    LogType type_{ LogType::INVALID };
    std::size_t lsn_{ NULL_LSN };
    std::size_t last_lsn_{ NULL_LSN };
};

template <LogType LogT>
class LogWithoutRecordBase : public Log
{
 public:
    LogWithoutRecordBase() = default;

    LogWithoutRecordBase(xact_id xid, std::size_t lsn, std::size_t last_lsn)
        : Log(xid, LogT, lsn, last_lsn)
    {
    }
};

template <LogType LogT>
class LogWithRecordBase : public LogWithoutRecordBase<LogT>
{
 public:
    LogWithRecordBase() = default;

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

    [[nodiscard]] HierarchyID hid() const
    {
        return hid_;
    }

    [[nodiscard]] std::size_t length() const
    {
        return length_;
    }

    [[nodiscard]] const page_data_t& old_data() const
    {
        return old_data_;
    }

    [[nodiscard]] const page_data_t& new_data() const
    {
        return new_data_;
    }

    [[nodiscard]] std::size_t size() const override
    {
        return Log::size() + sizeof(hid_) + length_ + length_;
    }

 private:
    HierarchyID hid_;
    std::size_t length_;
    page_data_t old_data_, new_data_;
};

using LogBegin = LogWithoutRecordBase<LogType::BEGIN>;
inline std::ostream& operator<<(std::ostream& os, const LogBegin& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [BEGIN] Transaction id "
       << log.xid();
    return os;
}

using LogCommit = LogWithoutRecordBase<LogType::COMMIT>;
inline std::ostream& operator<<(std::ostream& os, const LogCommit& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [COMMIT] Transaction id "
       << log.xid();
    return os;
}

using LogRollback = LogWithoutRecordBase<LogType::ROLLBACK>;
inline std::ostream& operator<<(std::ostream& os, const LogRollback& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [ROLLBACK] Transaction id "
       << log.xid();
    return os;
}

using LogUpdate = LogWithRecordBase<LogType::UPDATE>;
inline std::ostream& operator<<(std::ostream& os, const LogUpdate& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [UPDATE] Transaction id "
       << log.xid();
    return os;
}

using LogConsiderRedo = LogWithoutRecordBase<LogType::CONSIDER_REDO>;
inline std::ostream& operator<<(std::ostream& os, const LogConsiderRedo& log)
{
    os << "LSN " << log.lsn() + sizeof(log)
       << " [CONSIDER-REDO] Transaction id " << log.xid();
    return os;
}

class LogCompensate : public LogWithRecordBase<LogType::COMPENSATE>
{
 public:
    LogCompensate() = default;

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

    std::size_t size() const override
    {
        return LogWithRecordBase<LogType::COMPENSATE>::size() +
               sizeof(next_undo_lsn_);
    }

 private:
    std::size_t next_undo_lsn_;
};
inline std::ostream& operator<<(std::ostream& os, const LogCompensate& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [CLR] next undo lsn "
       << log.next_undo_lsn();
    return os;
}

class LogManager final
{
 public:
    [[nodiscard]] static bool initialize(const std::string& log_path,
                                         const std::string& logmsg_path);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LogManager& get_instance();

    void log_begin(Xact* xact);
    void log_commit(Xact* xact);
    void log_update(Xact* xact, HierarchyID hid, std::size_t lenght,
                    page_data_t old_data, page_data_t new_data);
    void log_rollback(Xact* xact);

    const std::list<std::unique_ptr<Log>>& get(Xact* xact) const;
    void remove(Xact* xact);

    [[nodiscard]] std::size_t flushed_lsn() const;

    [[nodiscard]] bool find_pagenum(pagenum_t pid) const;

    [[nodiscard]] bool force();

 private:
    LogManager() = default;

    template <typename Func>
    void logging(Xact* xact, Func&& func);
    template <typename LogT>
    void append_log(const LogT& log);

    Log* read_buffer(std::size_t lsn);
    const Log* read_buffer(std::size_t lsn) const;

 private:
    mutable std::recursive_mutex mutex_;

    std::vector<char> log_;
    std::unordered_map<xact_id, std::list<std::unique_ptr<Log>>> log_per_xact_;
    std::atomic<std::size_t> flushed_lsn_{ 0 };

    int f_log_{ -1 };
    std::ofstream f_logmsg_;

    inline static LogManager* instance_{ nullptr };
};

inline LogManager& LogMgr()
{
    return LogManager::get_instance();
}

template <typename Func>
void LogManager::logging(Xact* xact, Func&& func)
{
    std::scoped_lock lock(mutex_);

    const std::size_t lsn = flushed_lsn_.load() + log_.size();

    func(lsn);

    xact->last_lsn(lsn);
}

template <typename LogT>
void LogManager::append_log(const LogT& log)
{
    const std::size_t orig_size = log_.size();
    const std::size_t size = log.size();
    log_.resize(orig_size + size);

    memcpy(&log_[orig_size], &log, size);

    log_per_xact_[log.xid()].emplace_back(std::make_unique<LogT>(log));
}

#endif  // LOG_H_
