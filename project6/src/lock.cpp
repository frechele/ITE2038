#include "lock.h"

#include "common.h"

#include <algorithm>
#include <cassert>
#include <new>

Lock::Lock(xact_id xid, LockType type, HashTableEntry* sentinel)
    : xid_(xid), type_(type), sentinel_(sentinel)
{
}

xact_id Lock::xid() const
{
    return xid_;
}

LockType Lock::type() const
{
    return type_;
}

HashTableEntry* Lock::sentinel() const
{
    return sentinel_;
}

void Lock::wait(std::unique_lock<std::mutex>& lock)
{
    cond_.wait(lock);
}

void Lock::notify()
{
    cond_.notify_all();
}

bool LockManager::initialize()
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) LockManager;
    CHECK_FAILURE(instance_ != nullptr);

    return true;
}

bool LockManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    instance_->clear_all_entries();

    delete instance_;
    instance_ = nullptr;

    return true;
}

LockManager& LockManager::get_instance()
{
    assert(instance_ != nullptr);

    return *instance_;
}

Lock* LockManager::acquire(HierarchyID hid, xact_id xid, LockType type)
{
    assert(type != LockType::NONE);

    std::unique_lock lock(mutex_);

    HashTableEntry* entry;

    auto it = entries_.find(hid);
    if (it == end(entries_))
    {
        entry = new (std::nothrow) HashTableEntry;
        CHECK_FAILURE2(entry != nullptr, nullptr);

        entry->hid = hid;

        entries_.insert_or_assign(hid, entry);
    }
    else
    {
        entry = it->second;
    }

    Lock* lock_obj = new Lock(xid, type, entry);

    bool acquire =
        (entry->status == LockType::NONE || entry->run.empty()) ||
        (entry->wait.empty() &&
         ((entry->status == LockType::SHARED && type == LockType::SHARED) ||
          (entry->run.back()->xid() == xid)));

    if (!acquire && type == LockType::SHARED)
    {
        // if all wait locks are shared
        // we can change sequence of txn schedule

        acquire = true;
        for (const auto lk : entry->wait)
        {
            if (lk->type() == LockType::EXCLUSIVE)
            {
                acquire = false;
                break;
            }
        }

        if (entry->run.back()->xid() != xid)
        {
            acquire = false;
        }
    }

    if (acquire)
    {
        if (entry->status != LockType::EXCLUSIVE)
            entry->status = type;

        entry->run.emplace_back(lock_obj);

        return lock_obj;
    }

    entry->wait.push_back(lock_obj);

    WaitForGraph graph = build_wait_for_graph();
    WFGNode& node = graph[xid];

    if (!node.Out.empty())
    {
        if (check_cycle(graph, node))
        {
            // deadlock detected!!
            entry->wait.remove(lock_obj);
            delete lock_obj;

            return nullptr;
        }
    }

    lock_obj->wait(lock);

    return lock_obj;
}

bool LockManager::release(Lock* lock_obj)
{
    std::scoped_lock lock(mutex_);

    HashTableEntry* entry = lock_obj->sentinel();
    CHECK_FAILURE(entry != nullptr);

    if (auto it = std::find(begin(entry->run), end(entry->run), lock_obj);
        it != end(entry->run))
    {
        entry->run.erase(it);
        delete lock_obj;
    }
    else
    {
        // this is called ONLY abort

        entry->wait.remove(lock_obj);
        lock_obj->notify();
    }

    if (!entry->run.empty())
    {
        // there is conflict between locks in run and locks in wait
        // so if run is not empty, we don't need to manage this entry.
        return true;
    }

    if (entry->wait.empty())
    {
        entries_.erase(entry->hid);
        delete entry;

        return true;
    }

    if (entry->wait.front()->type() == LockType::EXCLUSIVE)
    {
        entry->status = LockType::EXCLUSIVE;

        Lock* lk = entry->wait.front();
        entry->run.emplace_back(lk);
        entry->wait.pop_front();

        lk->notify();

        return true;
    }

    // now front of wait list's lock type is SHARED
    entry->status = LockType::SHARED;

    for (auto it = begin(entry->wait); it != end(entry->wait);)
    {
        if ((*it)->type() == LockType::EXCLUSIVE)
            break;

        Lock* lk = *it;
        entry->run.emplace_back(lk);
        it = entry->wait.erase(it);

        lk->notify();
    }

    return true;
}

void LockManager::clear_all_entries()
{
    std::scoped_lock lock(mutex_);

    for (auto& pr : entries_)
    {
        delete pr.second;
    }

    entries_.clear();
}

WaitForGraph LockManager::build_wait_for_graph() const
{
    WaitForGraph graph;

    for (const auto& pr : entries_)
    {
        for (const auto after_lk : pr.second->wait)
        {
            const xact_id after_xid = after_lk->xid();

            for (const auto before_lk : pr.second->run)
            {
                const xact_id before_xid = before_lk->xid();

                graph[before_xid].Out.emplace_back(after_xid);
                graph[after_xid].In.emplace_back(before_xid);
            }
        }
    }

    return graph;
}

bool LockManager::check_cycle(WaitForGraph& graph, WFGNode& node) const
{
    if (node.visited)
    {
        return node.exploring;
    }

    node.exploring = true;
    node.visited = true;
    for (const xact_id after_xid : node.Out)
    {
        if (check_cycle(graph, graph[after_xid]))
        {
            return true;
        }
    }

    node.exploring = false;
    return false;
}
