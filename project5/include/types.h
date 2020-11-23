#ifndef TYPES_H_
#define TYPES_H_

#include <cstdint>
#include <string>
#include <tuple>

using table_id_t = int;
using pagenum_t = uint64_t;
using table_page_t = std::tuple<table_id_t, pagenum_t>;

struct HierarchyID final
{
    table_id_t table_id{ 0 };
    pagenum_t pagenum{ 0 };
    int offset{ 0 };

    HierarchyID() = default;
    HierarchyID(table_id_t tid, pagenum_t pid, int off)
        : table_id(tid), pagenum(pid), offset(off)
    {
    }

    bool operator==(const HierarchyID& other) const
    {
        return (table_id == other.table_id) && (pagenum == other.pagenum) &&
               (offset == other.offset);
    }

    bool operator!=(const HierarchyID& other) const
    {
        return !(*this == other);
    }
};

using xact_id = int;

#include "hashing.h"

#endif  // TYPES_H_
