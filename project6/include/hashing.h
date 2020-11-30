#ifndef HASHING_H_
#define HASHING_H_

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

template <>
struct hash<HierarchyID> final
{
    std::size_t operator()(const HierarchyID& hid) const
    {
        return hash<string>()(to_string(hid.table_id) + '|' +
                              to_string(hid.pagenum) + '|' +
                              to_string(hid.offset));
    }
};
}  // namespace std

#endif  // HASHING_H_
