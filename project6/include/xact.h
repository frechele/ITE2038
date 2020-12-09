#ifndef XACT_H_
#define XACT_H_

#include "lock.h"
#include "types.h"

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

class Xact final
{
 public:
    Xact(xact_id id);

    Xact(const Xact&) = delete;
    Xact& operator=(const Xact&) = delete;

    [[nodiscard]] xact_id id() const;

    [[nodiscard]] LockAcquireResult add_lock(HierarchyID hid, LockType type,
                                             Lock** lock_obj = nullptr);
    [[nodiscard]] bool release_all_locks();

    [[nodiscard]] bool undo();

    void lock();
    void unlock();

    void wait(std::condition_variable& cv);

 private:
    std::mutex mutex_;
    xact_id id_;

    std::list<Lock*> locks_;
};

class XactManager final
{
 public:
    [[nodiscard]] static bool initialize();
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static XactManager& get_instance();

    [[nodiscard]] Xact* begin();
    [[nodiscard]] bool commit(Xact* xact);
    [[nodiscard]] bool abort(Xact* xact);

    [[nodiscard]] Xact* get(xact_id id) const;

    void acquire_xact_lock(Xact* xact);

 private:
    XactManager() = default;

 private:
    mutable std::mutex mutex_;

    int global_xact_counter_{ 0 };
    std::unordered_map<xact_id, Xact*> xacts_;

    inline static XactManager* instance_{ nullptr };
};

inline XactManager& XactMgr()
{
    return XactManager::get_instance();
}

#endif  // XACT_H_
