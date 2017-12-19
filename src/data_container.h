#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "data_index.h"
#include "defines.h"

namespace RiverDB {

class DataContainer {
public:
    DataContainer();
    ~DataContainer();

    bool init(const std::vector<std::string>& load_fpath_vec, const std::string& primary_key);
    bool load(const std::string& fpath);

    void set_primary_key(const std::string& primary_key);
    void append_time_index(const std::string& index);

private:
    bool compare(const std::vector<RowBinaryColMeta>& col_metas);
    void close();
private:
    std::vector<char *> _buf_vec;
    std::unordered_map<std::string, DataIndex*> _data_index_map;
    std::vector<RowBinaryColMeta> _col_metas;
    std::string _primary_key;
    std::unordered_set<std::string> _time_key_set;
};

} // namespace RiverDB
