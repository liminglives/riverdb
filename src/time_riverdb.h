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

class TimeRiverDB {
public:
    TimeRiverDB(const std::string& primary_key,
            const std::string& index_key);
    ~TimeRiverDB();

    bool init(const std::vector<std::string>& load_fpath_vec,
            const std::string& append_file = "", 
            const std::vector<ColMeta>& col_metas = {});
    bool load(const std::string& fpath);
    bool append(const std::vector<std::string>& row); 

    DataIndex* get_data_index(const std::string& kvalue);
    RowReader* new_row_reader();

    bool at(const std::string& kvalue, int index, RowReader* row_reader);
    bool get(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool gt(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool ge(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool lt(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool le(const std::string& kvalue, uint64_t ts, RowReader* row_reader);

    unsigned int get_col_size() {
        return _col_metas.size();
    }

private:
    bool init_meta(const std::vector<ColMeta>& col_metas);
    bool compare(const std::vector<ColMeta>& col_metas);
    void close();
    void build_index();
    void append(const std::string& kvalue, uint64_t ts, char* data); 
    bool query(const std::string& kvalue, uint64_t ts, QueryOP op, RowReader* row_reader);

private:
    std::vector<char *> _buf_vec;
    std::unordered_map<std::string, DataIndex*> _data_index_map;
    std::unordered_map<std::string, int> _col_name_index_map;
    std::vector<ColMeta> _col_metas;
    std::string _primary_key;
    std::string _index_key;
    RiverDBWriter* _append_writer = NULL;
};

} // namespace RiverDB
