#ifndef LOG_H_
#define LOG_H_

#include "file.h"
#include "types.h"
#include "xact.h"

#include <memory.h>
#include <atomic>
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
using lsn_t = std::uint64_t;

#pragma pack(1)
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
    Log(xact_id xid, LogType type, lsn_t lsn, lsn_t last_lsn, int size);

    [[nodiscard]] LogType type() const;
    [[nodiscard]] xact_id xid() const;
    [[nodiscard]] lsn_t lsn() const;
    [[nodiscard]] lsn_t last_lsn() const;

    [[nodiscard]] int size() const;

 private:
    int size_{ 28 };
    lsn_t lsn_{ NULL_LSN };
    lsn_t last_lsn_{ NULL_LSN };
    xact_id xid_{ 0 };
    LogType type_{ LogType::INVALID };
};

template <LogType LogT>
class LogWithoutRecordBase : public Log
{
 public:
    LogWithoutRecordBase() = default;

    LogWithoutRecordBase(xact_id xid, lsn_t lsn, lsn_t last_lsn)
        : Log(xid, LogT, lsn, last_lsn, 28)
    {
    }
};

template <LogType LogT>
class LogWithRecordBase : public Log
{
 public:
    LogWithRecordBase() = default;

    LogWithRecordBase(xact_id xid, lsn_t lsn, lsn_t last_lsn,
                      const HierarchyID& hid, int length, const void* old_data,
                      const void* new_data, int size = 288)
        : Log(xid, LogT, lsn, last_lsn, size),
          tid_(hid.table_id),
          pid_(hid.pagenum),
          offset_(hid.offset * PAGE_DATA_SIZE + PAGE_HEADER_SIZE + 8),
          length_(length)
    {
        memcpy(old_data_, old_data, length);
        memcpy(new_data_, new_data, length);
    }

    [[nodiscard]] table_id_t table_id() const
    {
        return tid_;
    }

    [[nodiscard]] pagenum_t pagenum() const
    {
        return pid_;
    }

    [[nodiscard]] int offset() const
    {
        return offset_;
    }

    [[nodiscard]] int length() const
    {
        return length_;
    }

    [[nodiscard]] const void* old_data() const
    {
        return old_data_;
    }

    [[nodiscard]] const void* new_data() const
    {
        return new_data_;
    }

 private:
    table_id_t tid_;
    pagenum_t pid_;
    int offset_;
    int length_;
    char old_data_[PAGE_DATA_VALUE_SIZE];
    char new_data_[PAGE_DATA_VALUE_SIZE];
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

class LogCompensate final : public LogWithRecordBase<LogType::COMPENSATE>
{
 public:
    LogCompensate() = default;
    LogCompensate(xact_id xid, lsn_t lsn, lsn_t last_lsn,
                  const HierarchyID& hid, int length, const void* old_data,
                  const void* new_data, lsn_t next_undo_lsn)
        : LogWithRecordBase<LogType::COMPENSATE>(xid, lsn, last_lsn,
                                                 std::move(hid), length,
                                                 old_data, new_data, 296)
    {
    }

    [[nodiscard]] lsn_t next_undo_lsn() const
    {
        return next_undo_lsn_;
    }

 private:
    lsn_t next_undo_lsn_;
};
inline std::ostream& operator<<(std::ostream& os, const LogCompensate& log)
{
    os << "LSN " << log.lsn() + sizeof(log) << " [CLR] next undo lsn "
       << log.next_undo_lsn();
    return os;
}
#pragma pack()

inline std::ostream& operator<<(std::ostream& os, const Log& log)
{
    switch (log.type())
    {
        case LogType::BEGIN:
            return operator<<(os, static_cast<const LogBegin&>(log));

        case LogType::COMMIT:
            return operator<<(os, static_cast<const LogCommit&>(log));

        case LogType::UPDATE:
            return operator<<(os, static_cast<const LogUpdate&>(log));

        case LogType::ROLLBACK:
            return operator<<(os, static_cast<const LogRollback&>(log));

        case LogType::COMPENSATE:
            return operator<<(os, static_cast<const LogCompensate&>(log));

        case LogType::CONSIDER_REDO:
            return operator<<(os, static_cast<const LogConsiderRedo&>(log));

        default:
            return os;
    }
}

class LogManager final
{
 public:
    [[nodiscard]] static bool initialize(const std::string& log_path);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LogManager& get_instance();

    void log_begin(Xact* xact);
    void log_commit(Xact* xact);
    void log_update(Xact* xact, const HierarchyID& hid, int lenght,
                    page_data_t old_data, page_data_t new_data);
    void log_rollback(Xact* xact);

    const std::list<std::unique_ptr<Log>>& get(Xact* xact) const;
    void remove(Xact* xact);

    [[nodiscard]] lsn_t flushed_lsn() const;

    [[nodiscard]] bool find_pagenum(pagenum_t pid) const;

    [[nodiscard]] bool force();

 private:
    LogManager() = default;

    template <typename Func>
    void logging(Xact* xact, Func&& func);
    template <typename LogT>
    void append_log(const LogT& log);

 private:
    mutable std::recursive_mutex mutex_;

    std::vector<std::unique_ptr<Log>> log_;
    std::unordered_map<xact_id, std::list<std::unique_ptr<Log>>> log_per_xact_;

    std::atomic<lsn_t> flushed_lsn_{ 0 };
    lsn_t lsn_{ 0 };

    int f_log_{ -1 };

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

    func(lsn_);

    xact->last_lsn(lsn_);
}

template <typename LogT>
void LogManager::append_log(const LogT& log)
{
    log_.emplace_back(std::make_unique<LogT>(log));
    log_per_xact_[log.xid()].emplace_back(std::make_unique<LogT>(log));

    lsn_ += log.size();
}

#endif  // LOG_H_
