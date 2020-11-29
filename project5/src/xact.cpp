#include "xact.h"

#include "buffer.h"
#include "common.h"
#include "log.h"

#include <cassert>
#include <new>

Xact::Xact(xact_id id) : id_(id)
{
}

xact_id Xact::id() const
{
    return id_;
}

bool Xact::add_lock(HierarchyID hid, LockType type)
{
    Lock* lk = LockMgr().acquire(hid, id_, type);
    if (lk == nullptr)
    {
        assert(XactMgr().abort(this));
        return false;
    }

    std::scoped_lock lock(mutex_);
    locks_.emplace_back(lk);

    return true;
}

bool Xact::release_all_locks([[maybe_unused]] bool abort)
{
    std::scoped_lock lock(mutex_);

    for (auto& lk : locks_)
    {
        CHECK_FAILURE(LockMgr().release(lk));
    }

    locks_.clear();

    return true;
}

bool Xact::undo()
{
    std::scoped_lock lock(mutex_);

    const auto& logs = LogMgr().get(id_);

    const auto end_it = logs.rend();
    for (auto it = logs.rbegin(); it != end_it; ++it)
    {
        const LogType type = (*it)->type();

        if (type == LogType::UPDATE)
        {
            const auto log = static_cast<LogUpdate*>(*it);
            const HierarchyID hid = log->hid();

            // table must be avaiable
            Table* table = TblMgr().get_table(hid.table_id).value();
            CHECK_FAILURE(buffer([&](Page& page) {
                page.data()[hid.offset] = log->old_data();
            }, *table, hid.pagenum));
        }
    }

    return true;
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

    return xact;
}

bool XactManager::commit(Xact* xact)
{
    std::scoped_lock lock(mutex_);

    auto it = xacts_.find(xact->id());
    CHECK_FAILURE(it != xacts_.end());

    CHECK_FAILURE(xact->release_all_locks());

    LogMgr().log<LogCommit>(xact->id());
    LogMgr().remove(xact->id());

    delete xact;
    xacts_.erase(it);

    return true;
}

bool XactManager::abort(Xact* xact)
{
    std::scoped_lock lock(mutex_);
    
    auto it = xacts_.find(xact->id());
    CHECK_FAILURE(it != xacts_.end());

    CHECK_FAILURE(xact->undo());
    CHECK_FAILURE(xact->release_all_locks(true));

    LogMgr().log<LogAbort>(xact->id());
    LogMgr().remove(xact->id());

    delete xact;
    xacts_.erase(it);

    return true;
}

Xact* XactManager::get(xact_id id) const
{
    std::scoped_lock lock(mutex_);

    auto it = xacts_.find(id);
    CHECK_FAILURE2(it != xacts_.end(), nullptr);

    return it->second;
}
