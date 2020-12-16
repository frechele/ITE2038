#include "xact.h"

#include "buffer.h"
#include "common.h"
#include "log.h"

#include <memory.h>
#include <algorithm>
#include <cassert>
#include <new>

Xact::Xact(xact_id id) : id_(id)
{
}

xact_id Xact::id() const
{
    return id_;
}

LockAcquireResult Xact::add_lock(HierarchyID hid, LockType type,
                                 Lock** lock_obj)
{
    if (auto it = std::find_if(begin(locks_), end(locks_),
                               [&](const Lock* lock) -> bool {
                                   return (static_cast<int>(lock->type()) >=
                                               static_cast<int>(type) &&
                                           lock->sentinel()->hid == hid);
                               });
        it != end(locks_))
    {
        if (lock_obj != nullptr)
            *lock_obj = *it;

        return LockAcquireResult::ACQUIRED;
    }

    auto [lk, result] = LockMgr().acquire(hid, this, type);
    if (result == LockAcquireResult::FAIL ||
        result == LockAcquireResult::DEADLOCK)
    {
        return result;
    }

    locks_.emplace_back(lk);

    if (lock_obj != nullptr)
        *lock_obj = lk;

    return result;
}

bool Xact::release_all_locks()
{
    for (auto& lk : locks_)
    {
        CHECK_FAILURE(LockMgr().release(lk));
    }

    locks_.clear();

    return true;
}

bool Xact::undo()
{
    const auto& logs = LogMgr().get(this);

    const auto end_it = logs.rend();
    for (auto it = logs.rbegin(); it != end_it; ++it)
    {
        const LogType type = (*it)->type();

        if (type == LogType::UPDATE)
        {
            const auto log = (*it).get();
            const HierarchyID hid(
                log->table_id(), log->pagenum(),
                (log->offset() - PAGE_HEADER_SIZE) / log->length());

            // table must be avaiable
            Table* table = TblMgr().get_table(hid.table_id).value();
            CHECK_FAILURE(buffer(
                [&](Page& page) {
                    memcpy(page.data()[hid.offset].value, log->old_data(),
                           log->length());
                },
                *table, hid.pagenum, false));
        }
    }

    return true;
}

void Xact::lock()
{
    mutex_.lock();
}

void Xact::unlock()
{
    mutex_.unlock();
}

void Xact::wait(std::condition_variable& cv)
{
    std::unique_lock<std::mutex> lock(mutex_, std::adopt_lock);

    cv.wait(lock);
}

void Xact::last_lsn(std::size_t lsn)
{
    last_lsn_ = lsn;
}

std::size_t Xact::last_lsn() const
{
    return last_lsn_;
}

bool XactManager::initialize()
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) XactManager;
    CHECK_FAILURE(instance_ != nullptr);

    return true;
}

bool XactManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    delete instance_;
    instance_ = nullptr;

    return true;
}

XactManager& XactManager::get_instance()
{
    return *instance_;
}

Xact* XactManager::begin()
{
    std::scoped_lock lock(mutex_);

    const xact_id id = global_xact_counter_ + 1;
    Xact* xact = new (std::nothrow) Xact(id);
    CHECK_FAILURE2(xact != nullptr, nullptr);

    xacts_.insert_or_assign(id, xact);

    ++global_xact_counter_;

    LogMgr().log_begin(xact);

    return xact;
}

bool XactManager::commit(Xact* xact)
{
    CHECK_FAILURE(xact->release_all_locks());

    LogMgr().log_commit(xact);
    LogMgr().remove(xact);

    std::scoped_lock lock(mutex_);

    delete xact;
    xacts_.erase(xact->id());

    return true;
}

bool XactManager::abort(Xact* xact)
{
    CHECK_FAILURE(xact->undo());
    CHECK_FAILURE(xact->release_all_locks());

    LogMgr().log_rollback(xact);
    LogMgr().remove(xact);

    std::scoped_lock lock(mutex_);

    delete xact;
    xacts_.erase(xact->id());

    return true;
}

Xact* XactManager::get(xact_id id) const
{
    std::scoped_lock lock(mutex_);

    auto it = xacts_.find(id);
    CHECK_FAILURE2(it != xacts_.end(), nullptr);

    return it->second;
}

void XactManager::acquire_xact_lock(Xact* xact)
{
    std::scoped_lock lock(mutex_);

    xact->lock();
}
