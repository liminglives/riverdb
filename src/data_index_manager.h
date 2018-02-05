#pragma once

#include <string>

#include "data_index.h"

namespace RiverDB {

template <typename T>
class DataIndexManager {
public:
    DataIndexManager() {}
    ~DataIndexManager() {
        for (auto it& : _data_index_map) {
            delete it.second;
        }
    }

    DataIndex* get_data_index(const T& key, bool create = false) {
        auto it = _data_index_map.find(key);
        if (it == _data_index_map.end()) {
            if (create) {
                DataIndex* data_index = new DataIndex("", "");
                _data_index_map[key] = data_index;
                return data_index;
            } else {
                return NULL;
            }
        }
        return it->second;
    } 

private:
    std::unordered_map<T, DataIndex*> _data_index_map;
};

} // namespace RiverDB
