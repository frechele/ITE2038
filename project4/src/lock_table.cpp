#include <lock_table.h>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <tuple>
#include <string>
#include <unordered_map>

class HashTableEntry;

struct lock_t {
	bool wait{ false };
	std::condition_variable* cond{ nullptr };

	HashTableEntry* entry{ nullptr };
	lock_t* prev{ nullptr };
	lock_t* next{ nullptr };
};

using table_record_t = std::tuple<int, int64_t>;

namespace std
{
template <>
struct hash<table_record_t>
{
	size_t operator()(const table_record_t& key) const
	{
		auto [tid, rid] = key;

		return std::hash<string>()(std::to_string(tid) + '|' + std::to_string(rid));
	}
};
}

class HashTableEntry final
{
public:
	HashTableEntry(table_record_t trid);

	const table_record_t& table_record_id() const;

	[[nodiscard]] lock_t* update_link();
	void wait(lock_t* lock_obj);
	void release(lock_t* lock_obj);

private:
	table_record_t trid_;

	std::mutex mutex_;
	std::condition_variable cond_;

	lock_t* head_{ nullptr };
	lock_t* tail_{ nullptr };
};

HashTableEntry::HashTableEntry(table_record_t trid)
	: trid_(std::move(trid))
{
}

const table_record_t& HashTableEntry::table_record_id() const
{
	return trid_;
}

lock_t* HashTableEntry::update_link()
{
	std::scoped_lock lock(mutex_);

	lock_t* lock_obj = new (std::nothrow) lock_t;
	CHECK_FAILURE2(lock_obj != nullptr, nullptr);

	lock_obj->cond = &cond_;
	lock_obj->entry = this;

	if (tail_ == nullptr)
	{
		// there is no any lock object
		
		head_ = lock_obj;
		tail_ = lock_obj;
	}
	else
	{
		tail_->next = lock_obj;	
		lock_obj->prev = tail_;

		tail_ = lock_obj;

		lock_obj->wait = true;
	}
	
	return lock_obj;
}

void HashTableEntry::wait(lock_t* lock_obj)
{
	std::unique_lock lock(mutex_);

	lock_obj->cond->wait(lock, [lock_obj]{ return !lock_obj->wait; });
}

void HashTableEntry::release(lock_t* lock_obj)
{
	std::scoped_lock lock(mutex_);

	if (tail_ == lock_obj)
	{
		tail_ = lock_obj->prev;
	}
	if (head_ == lock_obj)
	{
		head_ = lock_obj->next;
	}

	if (lock_obj->prev != nullptr)
	{
		lock_obj->prev->next = lock_obj->next;
	}
	if (lock_obj->next != nullptr)
	{
		lock_obj->next->prev = lock_obj->prev;
	}

	delete lock_obj;

	if (head_ == nullptr)
		return;

	head_->wait = false;
	cond_.notify_all();
}

class LockTableManager final
{
public:
	[[nodiscard]] static bool initialize();
	[[nodiscard]] static bool shutdown();

	[[nodiscard]] static LockTableManager& get_instance();

	[[nodiscard]] lock_t* acquire(int table_id, int64_t key);
	[[nodiscard]] bool release(lock_t* lock_obj);

private:
	void clear_all_locks();

private:
	inline static LockTableManager* instance_{ nullptr };

	std::mutex table_latch_;
	std::unordered_map<table_record_t, HashTableEntry*> locks_;
};

inline LockTableManager& LockTblMgr()
{
	return LockTableManager::get_instance();
}

bool LockTableManager::initialize()
{
	CHECK_FAILURE(instance_ == nullptr);

	instance_ = new (std::nothrow) LockTableManager;
	CHECK_FAILURE(instance_ != nullptr);

	return true;
}

bool LockTableManager::shutdown()
{
	CHECK_FAILURE(instance_ != nullptr);

	instance_->clear_all_locks();

	delete instance_;
	instance_ = nullptr;

	return true;
}

LockTableManager& LockTableManager::get_instance()
{
	assert(instance_ != nullptr);

	return *instance_;
}

lock_t* LockTableManager::acquire(int table_id, int64_t key)
{
	lock_t* lock_obj;
	HashTableEntry* entry;

	{
		std::scoped_lock lock(table_latch_);

		table_record_t trid{ table_id, key };

		auto it = locks_.find(trid);
		if (it == end(locks_))
		{
			entry = new (std::nothrow) HashTableEntry(trid);
			CHECK_FAILURE2(entry != nullptr, nullptr);

			locks_[trid] = entry;
		}
		else
		{
			entry = it->second;
		}

		lock_obj = entry->update_link();
	}

	if (lock_obj != nullptr)
		entry->wait(lock_obj);

	return lock_obj;
}

bool LockTableManager::release(lock_t* lock_obj)
{
	CHECK_FAILURE(lock_obj != nullptr);

	std::scoped_lock lock(table_latch_);

	HashTableEntry* entry = lock_obj->entry;
	
	entry->release(lock_obj);

	return true;
}

void LockTableManager::clear_all_locks()
{
	for (auto& pr : locks_)
	{
		delete pr.second;
	}
}

int init_lock_table()
{
	return LockTableManager::initialize() ? 0 : -1;
}

lock_t* lock_acquire(int table_id, int64_t key)
{
	return LockTblMgr().acquire(table_id, key);
}

int lock_release(lock_t* lock_obj)
{
	return LockTblMgr().release(lock_obj) ? 0 : -1;
}
