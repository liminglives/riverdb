#include "time_riverdb.h"

#include <limits>
#include <string.h>

#include "mmap_file_reader.h"
#include "gz_file_reader.h"
#include "riverdb_reader.h"

namespace RiverDB {

TimeRiverDB::TimeRiverDB(const std::string& primary_key,
            const std::string& index_key) : 
    _primary_key(primary_key), _index_key(index_key) {}

TimeRiverDB::~TimeRiverDB() {
    close();
}

bool TimeRiverDB::init(const std::vector<std::string>& load_fpath_vec,
            const std::string& append_file, 
            const std::vector<ColMeta>& col_metas) {
    if (col_metas.size() > 0) {
        if (!init_meta(col_metas)) {
            Log("init col meta failed");
            return false;
        }
    }

    for (const auto& fpath : load_fpath_vec) {
        if (!load(fpath)) {
            Throw("load " + fpath + " failed");
        }
    }

    if (!append_file.empty()) {
        if (Util::file_exists(append_file)) {
            _append_writer = new RiverDBWriter(append_file, FileOpenModeAppend);
            auto append_file_col_meta = _append_writer->get_col_metas();
            if (!init_meta(append_file_col_meta)) {
                Log("append file col meta not matched");
                return false;
            }
        } else if (_col_metas.size() > 0){
            _append_writer = new RiverDBWriter(append_file, FileOpenModeWrite);
            _append_writer->set_col_metas(_col_metas);
            _append_writer->write_header();
        } else {
            Log("init append_file failed : " + append_file);
            return false;
        }
    }

    return true;
}

//bool load(const std::vector<std::string>& load_fpath_vec) {
//    //std::vector<std::string> fpaths = load_fpath_vec;
//    //std::sort(fpaths.begin(), fpaths.end());
//
//    for (const auto& fpath : load_fpath_vec) {
//        if (!load(fpath)) {
//            Throw("load " + fpath + " failed");
//        }
//    }
//    return true;
//}

void TimeRiverDB::close() {
    for (auto it : _data_index_map) {
        delete it.second;
    }

    for (auto buf : _buf_vec) {
        free(buf);
    }

    if (_append_writer) {
        delete _append_writer;
        _append_writer = NULL;
    }
}

RowReader* TimeRiverDB::new_row_reader() {
    return new RowReader(&_col_metas, &_col_name_index_map);
}

bool TimeRiverDB::get(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::EQ, row_reader);
}

bool TimeRiverDB::at(const std::string& kvalue, int index, RowReader* row_reader) {
    DataIndex* di = get_data_index(kvalue);
    if (di == NULL) {
        Log("has no kvalue:" + kvalue + " ksize:" + std::to_string(kvalue.size()));
        return false;
    }

    char* data = di->at(index);
    if (data == NULL) {
        Log("out range " + kvalue + " " + std::to_string(index));
        return false;
    }
    
    row_reader->init(data, std::numeric_limits<unsigned int>::max());

    return true;
}

template <>
bool TimeRiverDB::eq(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::EQ, row_reader);
}

template <>
bool TimeRiverDB::gt(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::GT, row_reader);
}

template <>
bool TimeRiverDB::ge(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::GE, row_reader);
}

template <>
bool TimeRiverDB::lt(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::LT, row_reader);
}

template <>
bool TimeRiverDB::le(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::LE, row_reader);
}

template <>
bool TimeRiverDB::index(const std::string& kvalue, int index, RowReader* row_reader) {
    return at(kvalue, index, row_reader);
}

template <>
bool TimeRiverDB::index_range(const std::string& k, int index_start, int index_end, RowsReader* rows_reader) {
    return index_range_real(k, index_start, index_end, rows_reader);
}

template <>
bool TimeRiverDB::range(const std::string& k, uint64_t ts_start, uint64_t ts_end, RowsReader* rows_reader) {
    return range_real(k, ts_start, ts_end, rows_reader);
}

bool TimeRiverDB::index_range_real(const std::string& kvalue, int index_start, int index_end, RowsReader* rows_reader) {
    if (index_start < 0 || index_end < 0) {
        return false;
    }

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

bool TimeRiverDB::range_real(const std::string& kvalue, uint64_t ts_start, uint64_t ts_end, RowsReader* rows_reader) {
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

bool TimeRiverDB::query(const std::string& kvalue, uint64_t ts, QueryOP op, RowReader* row_reader) {
    DataIndex* di = get_data_index(kvalue);
    if (di == NULL) {
        Log("has no kvalue:" + kvalue);
        return false;
    }

    char* data = NULL;
    switch (op) {
        case QueryOP::EQ:
            data = di->get(ts);
            break;
        case QueryOP::GT:
            data = di->gt(ts);
            break;
        case QueryOP::GE:
            data = di->ge(ts);
            break;
        case QueryOP::LT:
            data = di->lt(ts);
            break;
        case QueryOP::LE:
            data = di->le(ts);
            break;
        default:
            data = NULL;
    }

    if (data == NULL) {
        Log("kvalue " + kvalue + " has no " + std::to_string(ts) + 
                ", op:" + std::to_string(static_cast<int>(op)));
        return false;
    }
    
    row_reader->init(data, std::numeric_limits<unsigned int>::max());

    return true;
}

bool TimeRiverDB::compare(const std::vector<ColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        for (auto& col_meta : col_metas) {
            _col_metas.push_back(col_meta);
        }
        return true;
    } else {
        if (col_metas.size() != _col_metas.size()) {
            return false;
        }

        for (unsigned int i = 0; i < col_metas.size(); ++i) {
            const ColMeta meta1 = col_metas[i];
            const ColMeta meta2 = _col_metas[i];
            if (meta1.name != meta2.name || meta1.type != meta2.type) {
                Log("meta not equal");
                return false;
            }
        }
    }

    return true;
}

bool TimeRiverDB::init_meta(const std::vector<ColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        _col_metas = col_metas;
        for (int i = 0; i < _col_metas.size(); ++i) {
            _col_name_index_map[_col_metas[i].name] = i;
        }
    }
    if (col_metas.size() == 0 || !compare(col_metas)) {
        Log("col metas are not matched");
        return false;
    }
    return true;
}

DataIndex* TimeRiverDB::get_data_index(const std::string& kvalue) {
    auto it = _data_index_map.find(kvalue);
    return it == _data_index_map.end() ? NULL : it->second;
}

bool TimeRiverDB::append(const std::vector<std::string>& row) {
    char* data = NULL;
    unsigned int len;
    if (!Util::serialize_row(_col_metas, row, data, len)) {
        Log("serialize_row failed");
        return false;
    }
    _buf_vec.push_back(data);

    uint64_t ts = 0;
    Util::get_value<uint64_t>(const_cast<char*>(row[_col_name_index_map[_index_key]].c_str()), &ts);
    append(row[_col_name_index_map[_primary_key]], ts, data);

    if (_append_writer) {
        //_append_writer->write_row(row);
        _append_writer->write_row(data, len);
        _append_writer->flush();
    }

    return true;

}

void TimeRiverDB::append(const std::string& kvalue, uint64_t ts, char* data) {
    auto di = get_data_index(kvalue);
    if (di == NULL) {
        //Log("new DataIndex for " + kvalue);
        di = new DataIndex(kvalue, _index_key);
        _data_index_map[kvalue] = di;
    }
    di->append(ts, data);
}

bool TimeRiverDB::load(const std::string& fpath) {
    Log("start load " + fpath);
    RiverDBReader reader(fpath);
    reader.read_header();
    const std::vector<ColMeta>& col_metas = reader.get_col_metas();
    if (!init_meta(col_metas)) {
        Throw("illegal col_meta, file path:" + fpath);
    }

    unsigned long long data_size  = reader.get_data_size();
    char* buf = (char *)malloc(data_size);
    _buf_vec.push_back(buf);
    reader.load_data(buf, data_size);

    RowReader row_reader(&_col_metas, &_col_name_index_map);
    unsigned int start = 0;
    unsigned int remain = data_size;

    while (remain > 0) {
        //Log("row reader init, start:" + std::to_string(start));
        row_reader.init(buf + start, remain);
        uint64_t ts = 0;
        if (!row_reader.get<uint64_t>(_index_key, &ts)) {
            Throw("row_reader failed, fpath:" + fpath);
        }

        std::string kvalue;
        if (!row_reader.get<std::string>(_primary_key, &kvalue)) {
            Throw("row_reader failed, fpath:" + fpath);
        }

        //Log("kvalue:" + kvalue + " ksize:" + std::to_string(kvalue.size()));
        append(kvalue, ts, buf + start);

        unsigned int len = row_reader.get_len();

        start += len;
        remain -= len;
    }
    Log("load " + fpath + " completed");
    return true;
}

} // namespace RiverDB
