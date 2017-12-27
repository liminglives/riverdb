#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "data_index.h"
#include "row_reader.h"
#include "defines.h"

namespace RiverDB {

class DataContainer {
public:
    DataContainer();
    ~DataContainer();

    bool init(const std::string& primary_key,
            const std::string& index_key);
    bool load(const std::string& fpath);
    //bool load(const std::vector<std::string>& load_fpath_vec);

    DataIndex* get_data_index(const std::string& kvalue);
    RowReader* new_row_reader();
    bool get(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool at(const std::string& kvalue, int index, RowReader* row_reader);

private:
    bool compare(const std::vector<RowBinaryColMeta>& col_metas);
    void close();
    bool init_meta(const std::vector<RowBinaryColMeta>& col_metas);
    void build_index();
private:
    std::vector<char *> _buf_vec;
    std::unordered_map<std::string, DataIndex*> _data_index_map;
    std::unordered_map<std::string, int> _col_meta_map;
    std::vector<RowBinaryColMeta> _col_metas;
    std::string _primary_key;
    std::string _index_key;
    RowReader* _row_reader;
};

} // namespace RiverDB
