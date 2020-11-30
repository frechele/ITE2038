#ifndef LOCK_H_
#define LOCK_H_

#include "types.h"

#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>

enum class LockType
{
    NONE,
    SHARED,
    EXCLUSIVE
};

struct HashTableEntry;

class Lock final
{
 public:
    Lock() = default;
    Lock(xact_id xid, LockType type, HashTableEntry* sentinel);

    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    xact_id xid() const;
    LockType type() const;

    HashTableEntry* sentinel() const;

    void wait(std::unique_lock<std::mutex>& lock);
    void notify();

 private:
    xact_id xid_{ 0 };
    LockType type_{ LockType::NONE };

    HashTableEntry* sentinel_{ nullptr };

    std::condition_variable cond_;
};

struct HashTableEntry final
{
    HierarchyID hid;
    LockType status{ LockType::NONE };

    std::list<Lock*> run;
    std::list<Lock*> wait;
};

struct WFGNode final
{
    bool visited{ false };
    bool exploring{ false };
    std::list<xact_id> In, Out;
};

using WaitForGraph = std::unordered_map<xact_id, WFGNode>;

class LockManager final
{
 public:
    [[nodiscard]] static bool initialize();
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static LockManager& get_instance();

    [[nodiscard]] Lock* acquire(HierarchyID hid, xact_id xid, LockType type);
    [[nodiscard]] bool release(Lock* lock_obj);

 private:
    void clear_all_entries();

    [[nodiscard]] WaitForGraph build_wait_for_graph() const;
    [[nodiscard]] bool check_cycle(WaitForGraph& graph, WFGNode& node) const;

 private:
    std::mutex mutex_;

    std::unordered_map<HierarchyID, HashTableEntry*> entries_;

    inline static LockManager* instance_{ nullptr };
};

inline LockManager& LockMgr()
{
    return LockManager::get_instance();
}

#endif  // LOCK_H_
