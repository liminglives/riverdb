#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "defines.h"

namespace RiverDB {

class DataIndex {
public:
    DataIndex(const std::string& kvalue, const std::string& index_key);
    ~DataIndex();

    void append(uint64_t ts, char* data);

    char* get(uint64_t ts);
    char* at(int index);

    char* gt(uint64_t ts); // great than: >
    char* ge(uint64_t ts); // great or equal: >=
    char* lt(uint64_t ts); // less than: <
    char* le(uint64_t ts); // less or equal: <=

private:

private:
    std::string _kvalue; // value of primary key
    std::string _index_key;
    std::unordered_map<uint64_t, char*> _ts_data_map;
    std::vector<uint64_t> _ts_index;
};

} // namespace RiverDB
