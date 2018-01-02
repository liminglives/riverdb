#include "data_container.h"

#include <limits>
#include <string.h>

#include "mmap_file_reader.h"
#include "gz_file_reader.h"
#include "riverdb_reader.h"

namespace RiverDB {

DataContainer::DataContainer(const std::string& primary_key,
            const std::string& index_key) : 
    _primary_key(primary_key), _index_key(index_key) {}

DataContainer::~DataContainer() {
    close();
}

bool DataContainer::init() {
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

void DataContainer::close() {
    for (auto it : _data_index_map) {
        delete it.second;
    }

    for (auto buf : _buf_vec) {
        free(buf);
    }
}

RowReader* DataContainer::new_row_reader() {
    return new RowReader(&_col_metas, &_col_name_index_map);
}

bool DataContainer::get(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::EQ, row_reader);
}

bool DataContainer::at(const std::string& kvalue, int index, RowReader* row_reader) {
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

bool DataContainer::gt(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::GT, row_reader);
}

bool DataContainer::ge(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::GE, row_reader);
}

bool DataContainer::lt(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::LT, row_reader);
}

bool DataContainer::le(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return query(kvalue, ts, QueryOP::LE, row_reader);
}

bool DataContainer::query(const std::string& kvalue, uint64_t ts, QueryOP op, RowReader* row_reader) {
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

bool DataContainer::compare(const std::vector<RowBinaryColMeta>& col_metas) {
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
            const RowBinaryColMeta meta1 = col_metas[i];
            const RowBinaryColMeta meta2 = _col_metas[i];
            if (meta1._col_name != meta2._col_name || meta1._type != meta2._type) {
                Log("meta not equal");
                return false;
            }
        }
    }

    return true;
}

bool DataContainer::init_meta(const std::vector<RowBinaryColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        _col_metas = col_metas;
        for (int i = 0; i < _col_metas.size(); ++i) {
            _col_name_index_map[_col_metas[i]._col_name] = i;
        }
    }
    if (col_metas.size() == 0 || !compare(col_metas)) {
        Log("col metas are not matched");
        return false;
    }
    return true;
}

DataIndex* DataContainer::get_data_index(const std::string& kvalue) {
    auto it = _data_index_map.find(kvalue);
    return it == _data_index_map.end() ? NULL : it->second;
}

bool DataContainer::append(const std::vector<std::string>& row) {
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

    return true;

}

/*
bool DataContainer::append(const std::vector<std::string>& row) {
    unsigned int row_size = row.size();
    if (row_size != _col_metas.size()) {
        Log("row size is wrong, row_size:" + std::to_string(row_size) + 
                " col_metas size:" + std::to_string(_col_metas.size()));
        return false;
    }

    unsigned int len = 0;
    for (unsigned int i = 0; i < row_size; ++i) {
        len += row[i].size();
        if (_col_metas[i]._type >= Type_INT16 && _col_metas[i]._type <= Type_LD) {
            ++len;
        } else if (_col_metas[i]._type == Type_STRING) {
            len += 2;
        }
    }
    //len += row_size * 2;
    char* data = (char*)malloc(len);
    memset(data, 0, len);
    _buf_vec.push_back(data);

    unsigned int cur = 0;
    for (unsigned int i = 0; i < row_size; ++i) {
        char mark = '\0';
        if (_col_metas[i]._type >= Type_INT16 && _col_metas[i]._type <= Type_LD) {
            mark = '0' + row[i].size();
        }
        *(data + cur) = mark;
        ++cur;
        memcpy(data + cur, row[i].data(), row[i].size());
        cur += row[i].size();
        if (_col_metas[i]._type == Type_STRING) {
            *(data + cur) = '\0';
            ++cur;
        }
    }

    uint64_t ts = 0;
    Util::get_value<uint64_t>(const_cast<char*>(row[_col_name_index_map[_index_key]].c_str()), &ts);
    append(row[_col_name_index_map[_primary_key]], ts, data);

    return true;
}
*/

void DataContainer::append(const std::string& kvalue, uint64_t ts, char* data) {
    auto di = get_data_index(kvalue);
    if (di == NULL) {
        Log("new DataIndex for " + kvalue);
        di = new DataIndex(kvalue, _index_key);
        _data_index_map[kvalue] = di;
    }
    di->append(ts, data);
}

bool DataContainer::load(const std::string& fpath) {
    RiverDBReader reader(fpath);
    reader.read_header();
    const std::vector<RowBinaryColMeta>& col_metas = reader.get_col_metas();
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
        //auto it = _data_index_map.find(kvalue);
        //if (it == _data_index_map.end()) {
        //    Log("new dataindex for " + kvalue);
        //    _data_index_map[kvalue] = new DataIndex(kvalue, _index_key);
        //}
        //_data_index_map[kvalue]->append(ts, buf + start);

        unsigned int len = row_reader.get_len();

        start += len;
        remain -= len;
    }
    Log("load " + fpath + " completed");
    return true;
}

} // namespace RiverDB
