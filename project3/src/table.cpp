#include "table.h"

#include "bpt.h"
#include "common.h"

table_id_t Table::id() const
{
    return id_;
}

const std::string& Table::filename() const
{
    return filename_;
}

bool Table::insert(const page_data_t& record)
{
    return BPTree::insert(*this, record);
}

bool Table::remove(int64_t key)
{
    return BPTree::remove(*this, key);
}

std::optional<page_data_t> Table::find(int64_t key)
{
    return BPTree::find(*this, key);
}

std::vector<page_data_t> Table::find_range(int64_t key_start, int64_t key_end)
{
    return BPTree::find_range(*this, key_start, key_end);
}

std::string Table::to_string()
{
    return BPTree::to_string(*this);
}

void Table::set_file(File* file)
{
    file_ = file;
}

File* Table::file()
{
    return file_;
}

Table::Table(table_id_t id, std::string filename)
    : id_(id), filename_(std::move(filename))
{
}

bool TableManager::initialize(int num_buf)
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) TableManager;
    CHECK_FAILURE(instance_ != nullptr);

    return BPTree::initialize(num_buf);
}

bool TableManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    CHECK_FAILURE(BPTree::shutdown());

    delete instance_;
    instance_ = nullptr;

    return true;
}

TableManager& TableManager::get_instance()
{
    return *instance_;
}

std::optional<table_id_t> TableManager::open_table(const std::string& filename)
{
    table_id_t new_table_id = table_ids_.size() + 1;

    auto it = table_ids_.find(filename);
    if (it != end(table_ids_))
    {
        new_table_id = it->second;
    }

    CHECK_FAILURE2(new_table_id <= MAX_TABLE_COUNT, std::nullopt);
    CHECK_FAILURE2(tables_.find(new_table_id) == end(tables_), std::nullopt);

    Table table(new_table_id, filename);
    CHECK_FAILURE(BPTree::open_table(table));

    table_ids_.insert_or_assign(filename, new_table_id);
    tables_.insert_or_assign(new_table_id, std::move(table));

    return new_table_id;
}

bool TableManager::close_table(table_id_t tid)
{
    auto it = tables_.find(tid);
    CHECK_FAILURE(it != end(tables_));

    CHECK_FAILURE(BPTree::close_table(it->second));

    tables_.erase(it);

    return true;
}

std::optional<Table*> TableManager::get_table(table_id_t tid)
{
    auto it = tables_.find(tid);
    CHECK_FAILURE2(it != end(tables_), std::nullopt);

    return &it->second;
}
