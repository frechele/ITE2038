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
using lsn_t = std::uint64_t;

#pragma pack(push, 1)

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

    [[nodiscard]] static Log create_begin(xact_id xid, lsn_t lsn);
    [[nodiscard]] static Log create_commit(xact_id xid, lsn_t lsn,
                                           lsn_t last_lsn);
    [[nodiscard]] static Log create_update(xact_id xid, lsn_t lsn,
                                           lsn_t last_lsn,
                                           const HierarchyID& hid, int length,
                                           const void* old_data,
                                           const void* new_data);
    [[nodiscard]] static Log create_rollback(xact_id xid, lsn_t lsn,
                                             lsn_t last_lsn);

 public:
    [[nodiscard]] LogType type() const;
    [[nodiscard]] xact_id xid() const;
    [[nodiscard]] lsn_t lsn() const;
    [[nodiscard]] lsn_t last_lsn() const;
    [[nodiscard]] int size() const;

    [[nodiscard]] table_id_t table_id() const;
    [[nodiscard]] pagenum_t pagenum() const;
    [[nodiscard]] int offset() const;
    [[nodiscard]] int length() const;
    [[nodiscard]] const void* old_data() const;
    [[nodiscard]] const void* new_data() const;

    [[nodiscard]] lsn_t next_undo_lsn() const;

 private:
    int size_{ 28 };
    lsn_t lsn_{ NULL_LSN };
    lsn_t last_lsn_{ NULL_LSN };
    xact_id xid_{ 0 };
    LogType type_{ LogType::INVALID };

    table_id_t tid_;
    pagenum_t pid_;
    int offset_;
    int length_;
    char old_data_[PAGE_DATA_VALUE_SIZE], new_data_[PAGE_DATA_VALUE_SIZE];

    lsn_t next_undo_lsn_;
};

#pragma pack(pop)

inline std::ostream& operator<<(std::ostream& os, const Log& log)
{
    switch (log.type())
    {
        case LogType::BEGIN:
            os << "LSN " << log.lsn() + log.size()
               << " [BEGIN] Transaction id " << log.xid();
            break;

        case LogType::COMMIT:
            os << "LSN " << log.lsn() + log.size()
               << " [COMMIT] Transaction id " << log.xid();
            break;

        case LogType::UPDATE:
            os << "LSN " << log.lsn() + log.size()
               << " [UPDATE] Transaction id " << log.xid();
            break;

        case LogType::ROLLBACK:
            os << "LSN " << log.lsn() + log.size()
               << " [ROLLBACK] Transaction id " << log.xid();
            break;

        case LogType::COMPENSATE:
            os << "LSN " << log.lsn() + log.size() << " [CLR] next undo lsn "
               << log.next_undo_lsn();
            break;

        case LogType::CONSIDER_REDO:
            os << "LSN " << log.lsn() + log.size()
               << " [CONSIDER-REDO] Transaction id " << log.xid();
            break;

        default:
            break;
    }

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

    const int log_size = func(lsn_);

    xact->last_lsn(lsn_);

    lsn_ += log_size;
}

template <typename LogT>
void LogManager::append_log(const LogT& log)
{
    log_.emplace_back(std::make_unique<LogT>(log));
    log_per_xact_[log.xid()].emplace_back(std::make_unique<LogT>(log));
}

#endif  // LOG_H_
