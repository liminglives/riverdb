#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <limits>

#include "data_index.h"
#include "row_reader.h"
#include "defines.h"
#include "riverdb_writer.h"

namespace RiverDB {

class KVRiverDB { 
public:
    KVRiverDB(const std::string& primary_key);
    ~KVRiverDB();

    bool init(const std::string& fpath, 
            const std::vector<ColMeta>& col_metas = {});

    RowReader* new_row_reader(); 

    template <class T>
    bool get(const T& kvalue, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        auto it = _index_map.find(k);
        if (it == _index_map.end()) {
            return false;
        }
        row_reader->init(it->second, std::numeric_limits<unsigned int>::max());
        return true;
    }

    bool set(const std::vector<std::string>& row);

private:
    bool load(const std::string& fpath);
    void close();
    bool init_meta(const std::vector<ColMeta>& col_metas); 

private:
    std::vector<char *> _buf_vec;
    std::unordered_map<std::string, char*> _index_map;
    std::unordered_map<std::string, int> _col_name_index_map;
    std::vector<ColMeta> _col_metas;
    std::string _primary_key;
    std::string _fpath;
    RiverDBWriter* _writer = NULL;
};

} // namespace RiverDB
