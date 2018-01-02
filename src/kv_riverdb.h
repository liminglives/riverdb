#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "data_index.h"
#include "row_reader.h"
#include "defines.h"
#include "riverdb_writer.h"

namespace RiverDB {

class KVRiverDB { 
public:
    KVRiverDB(const std::string& primary_key);
    ~KVRiverDB();

    bool init(const std::string& fpath);

    RowReader* new_row_reader(); 

    bool get(const std::string& kvalue, RowReader* row_reader);
    bool set(const std::vector<std::string>& row);

private:
    bool load(const std::string& fpath);
    void close();
    bool init_meta(const std::vector<RowBinaryColMeta>& col_metas); 

private:
    std::vector<char *> _buf_vec;
    std::unordered_map<std::string, char*> _index_map;
    std::unordered_map<std::string, int> _col_name_index_map;
    std::vector<RowBinaryColMeta> _col_metas;
    std::string _primary_key;
    std::string _fpath;
    RiverDBWriter* _writer = NULL;
};

} // namespace RiverDB
