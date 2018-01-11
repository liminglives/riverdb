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

    template <class T>
    bool eq(const T& kvalue, uint64_t ts, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return query(k, ts, QueryOP::EQ, row_reader);
    }
    template <class T>
    bool gt(const T& kvalue, uint64_t ts, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return query(k, ts, QueryOP::GT, row_reader);
    }

    template <class T>
    bool ge(const T& kvalue, uint64_t ts, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return query(k, ts, QueryOP::GE, row_reader);
    }

    template <class T>
    bool lt(const T& kvalue, uint64_t ts, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return query(k, ts, QueryOP::LT, row_reader);
    }
    template <class T>
    bool le(const T& kvalue, uint64_t ts, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return query(k, ts, QueryOP::LE, row_reader);
    }

    template <class T>
    bool index(const T& kvalue, int index, RowReader* row_reader) {
        std::string k;
        Util::get_str_from_val(kvalue, k);
        return at(k, index, row_reader);
    }

    template <class T>
    bool index_range(const T& k, int index_start, int index_end, RowsReader* rows_reader) {
        if (index_start < 0 || index_end < 0) {
            return false;
        }
        std::string kvalue;
        Util::get_str_from_val(k, kvalue);

        DataIndex* di = get_data_index(kvalue);
        if (di == NULL) {
            Log("has no kvalue:" + kvalue);
            return false;
        }
        char* data = NULL;
        for (int i = index_start; i < index_end; ++i) {
            if ((data = di->at(i)) != NULL) {
                rows_reader->push(data);
            }
        }
        return true;
    }

    template <class T>
    bool range(const T& k, uint64_t ts_start, uint64_t ts_end, RowsReader* rows_reader) {
        std::string kvalue;
        Util::get_str_from_val(k, kvalue);

        DataIndex* di = get_data_index(kvalue);
        if (di == NULL) {
            Log("has no kvalue:" + kvalue);
            return false;
        }
        int start = di->get_index_by_ts(ts_start, QueryOP::GE);
        if (start == -1) {
            return false;
        }
        int end = di->get_index_by_ts(ts_start, QueryOP::LE);
        if (end == -1) {
            return false;
        }
        if (start > end) {
            return false;
        }
        for (int i = start; i < end; ++i) {
            rows_reader->push(di->at(i));
        }

        return true;
    }

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
