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
    COMPENSATE
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
    [[nodiscard]] static Log create_compensate(xact_id xid, lsn_t lsn,
                                               lsn_t last_lsn,
                                               const HierarchyID& hid,
                                               int length, const void* old_data,
                                               const void* new_data,
                                               lsn_t next_undo_lsn);

 public:
    Log() = default;

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
    char old_data_[PAGE_DATA_VALUE_SIZE];
    char new_data_[PAGE_DATA_VALUE_SIZE];
    lsn_t next_undo_lsn_;
};
#pragma pack(pop)

struct log_file_header final
{
    lsn_t base_lsn;
    lsn_t next_lsn;
};

class LogManager final
{
 public:
    [[nodiscard]] static bool initialize(const std::string& log_path);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LogManager& get_instance();

    lsn_t log_begin(Xact* xact);
    lsn_t log_commit(Xact* xact);
    lsn_t log_update(Xact* xact, const HierarchyID& hid, int length,
                     page_data_t old_data, page_data_t new_data);
    lsn_t log_rollback(Xact* xact);
    lsn_t log_rollback(xact_id xid, lsn_t last_lsn);
    lsn_t log_compensate(xact_id xid, lsn_t last_lsn, const HierarchyID& hid, int length,
                         const void* old_data, const void* new_data, lsn_t next_undo_lsn);

    const std::list<std::unique_ptr<Log>>& get(Xact* xact) const;
    void remove(Xact* xact);

    [[nodiscard]] lsn_t flushed_lsn() const;

    [[nodiscard]] bool find_page(table_id_t tid, pagenum_t pid) const;

    [[nodiscard]] bool force();

    [[nodiscard]] lsn_t base_lsn() const;
    [[nodiscard]] lsn_t next_lsn() const;

    void truncate_log();
    [[nodiscard]] Log read_log(lsn_t lsn) const;

 private:
    LogManager() = default;

    template <typename Func>
    [[nodiscard]] lsn_t logging(Xact* xact, Func&& func);
    template <typename LogT>
    void append_log(const LogT& log, bool add_to_search = true);

    [[nodiscard]] Log read_log_offset(lsn_t offset) const;

 private:
    mutable std::recursive_mutex mutex_;

    std::vector<std::unique_ptr<Log>> log_;
    std::unordered_map<xact_id, std::list<std::unique_ptr<Log>>> log_per_xact_;

    std::atomic<lsn_t> flushed_lsn_{ 0 };
    log_file_header header_;

    int f_log_{ -1 };

    inline static LogManager* instance_{ nullptr };
};

inline LogManager& LogMgr()
{
    return LogManager::get_instance();
}

template <typename Func>
lsn_t LogManager::logging(Xact* xact, Func&& func)
{
    std::scoped_lock lock(mutex_);

    const lsn_t cur_lsn = header_.next_lsn;

    func(cur_lsn);

    xact->last_lsn(cur_lsn);
    return cur_lsn;
}

template <typename LogT>
void LogManager::append_log(const LogT& log, bool add_to_search)
{
    log_.emplace_back(std::make_unique<LogT>(log));

    if (add_to_search)
        log_per_xact_[log.xid()].emplace_back(std::make_unique<LogT>(log));

    header_.next_lsn += log.size();
}

#endif  // LOG_H_
