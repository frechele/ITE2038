#ifndef TABLE_H_
#define TABLE_H_

#include "file.h"
#include "xact.h"

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

class Table final
{
 public:
    table_id_t id() const;
    const std::string& filename() const;

    [[nodiscard]] bool insert(const page_data_t& record);
    [[nodiscard]] bool remove(int64_t key);
    [[nodiscard]] std::optional<page_data_t> find(int64_t key, Xact* xact);
    [[nodiscard]] bool update(int64_t key, const char* value, Xact* xact);

    void set_file(File* file);
    File* file();

 private:
    Table(table_id_t id, std::string filename);

 private:
    table_id_t id_{ -1 };
    std::string filename_;

    File* file_{ nullptr };

    friend class TableManager;
};

class TableManager final
{
 public:
    constexpr static int MAX_TABLE_COUNT = 10;

 public:
    [[nodiscard]] static bool initialize(int num_buf);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static bool is_initialized();

    [[nodiscard]] static TableManager& get_instance();

    [[nodiscard]] std::optional<table_id_t> open_table(
        const std::string& filename);
    [[nodiscard]] bool close_table(table_id_t tid);

    [[nodiscard]] std::optional<Table*> get_table(table_id_t tid);

 private:
    std::unordered_map<std::string, table_id_t> table_ids_;
    std::unordered_map<table_id_t, Table> tables_;

    inline static TableManager* instance_{ nullptr };
};

inline TableManager& TblMgr()
{
    return TableManager::get_instance();
}

#endif  // TABLE_H_
