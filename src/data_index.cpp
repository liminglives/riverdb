#include "data_index.h"

namespace RiverDB {


DataIndex::DataIndex(const std::string& kvalue, 
        const std::string& index_key) : _kvalue(kvalue), _index_key(index_key) {
    //for (auto index_key : index_set) {
    //    _index_map[index_key] = std::vector<unsigned int>();
    //}
}

DataIndex::~DataIndex() {
}

void DataIndex::append(uint64_t ts, char* data) {
    _ts_index.push_back(ts); // todo : sort insert
    _ts_data_map[ts] = data; //  todo : check repeated timestamp
}

char* DataIndex::get(unsigned int ts) {
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

} // namespace RiverDB
