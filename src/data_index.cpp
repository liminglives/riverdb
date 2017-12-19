#include "data_index.h"

namespace RiverDB {


DataIndex::DataIndex(const std::string& kvalue, 
        const std::unordered_set<std::string>& index_set) : _kvalue(kvalue) {
    for (auto index_key : index_set) {
        _index_map[index_key] = std::vector<unsigned int>();
    }
}

DataIndex::~DataIndex() {
    close();
}

void DataIndex::close() {
}

void DataIndex::append(const std::string& index, unsigned int ts, char* data) {
    auto index_it = _ts_index_map.find(index);
    if (index_it == _ts_index_map.end()) {
        Throw("append index " + index + " error");
    }
    
    (index_it->second).push_back(ts); // todo : sort insert
    _ts_data_map[ts] = data; //  todo : check repeated timestamp
}

char* DataIndex::query(unsigned int ts) {
    auto it = _ts_data_map.find(ts);
    return it == _ts_data_map.end() ? NULL : it->second;
}

} // namespace RiverDB
