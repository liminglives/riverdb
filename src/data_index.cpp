#include "data_index.h"

#include "util.h"

namespace RiverDB {


DataIndex::DataIndex(const std::string& kvalue, 
        const std::string& index_key) : _kvalue(kvalue), _index_key(index_key) {
}

DataIndex::~DataIndex() {
}

void DataIndex::append(uint64_t ts, char* data) {
    auto it = _ts_data_map.find(ts);
    _ts_data_map[ts] = data; 
    if (it != _ts_data_map.end()) {
        return;
    }

    Util::sort_insert(_ts_index, ts);
}

char* DataIndex::get(uint64_t ts) {
    auto it = _ts_data_map.find(ts);
    return it == _ts_data_map.end() ? NULL : it->second;
}

char* DataIndex::at(int index) {
    unsigned int len = _ts_index.size();
    if (index < 0) {
        index = len + index;
        if (index < 0) {
            index = 0;
        }
    } else if (index >= len) {
        index = len - 1;
    }
    return _ts_data_map[_ts_index[index]];
}

int DataIndex::get_index_by_ts(uint64_t ts, QueryOP op) {
    int pos = -1;
    switch (op) {
        case QueryOP::EQ:
            pos = Util::binary_search(_ts_index, ts, Util::BinarySearchEqualFlag);
            break;
        case QueryOP::GT:
            pos = Util::binary_search(_ts_index, ts, Util::BinarySearchGTFlag);
            break;
        case QueryOP::GE:
            pos = Util::binary_search(_ts_index, ts, Util::BinarySearchGEFlag);
            break;
        case QueryOP::LT:
            pos = Util::binary_search(_ts_index, ts, Util::BinarySearchLTFlag);
            break;
        case QueryOP::LE:
            pos = Util::binary_search(_ts_index, ts, Util::BinarySearchLEFlag);
            break;
        default:
            pos = -1;
    }
    if (pos >= _ts_index.size()) {
        pos = -1;
    }
    return pos;
}

char* DataIndex::gt(uint64_t ts) {
    int pos = Util::binary_search(_ts_index, ts, Util::BinarySearchGTFlag);
    if (pos >= _ts_index.size()) {
        return NULL;
    }
    return at(pos); 
}

char* DataIndex::ge(uint64_t ts) {
    int pos = Util::binary_search(_ts_index, ts, Util::BinarySearchGEFlag);
    if (pos >= _ts_index.size()) {
        return NULL;
    }
    return at(pos); 
}

char* DataIndex::lt(uint64_t ts) {
    int pos = Util::binary_search(_ts_index, ts, Util::BinarySearchLTFlag);
    if (pos < 0) {
        return NULL;
    }
    return at(pos); 
}

char* DataIndex::le(uint64_t ts) {
    int pos = Util::binary_search(_ts_index, ts, Util::BinarySearchLEFlag);
    if (pos < 0) {
        return NULL;
    }
    return at(pos); 
}



} // namespace RiverDB
