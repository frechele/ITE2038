#ifndef TYPES_H_
#define TYPES_H_

#include <cstdint>
#include <string>
#include <tuple>

using table_id_t = int;
using pagenum_t = uint64_t;
using table_page_t = std::tuple<table_id_t, pagenum_t>;

namespace std
{
template <>
struct hash<table_page_t> final
{
    std::size_t operator()(const table_page_t& tpid) const
    {
        auto [tid, pid] = tpid;

        return hash<string>()(to_string(tid) + '|' + to_string(pid));
    }
};
}  // namespace std

#endif  // TYPES_H_
