#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "defines.h"

namespace RiverDB {

class DataIndex {
public:
    DataIndex(const std::string& kvalue, const std::unordered_set<std::string>& index_set);
    ~DataIndex();

    void append(const std::string& index, unsigned int ts, char* data);

    char* query(unsigned int ts);

private:
    void close();

private:
    std::string _kvalue; // value of primary key
    std::unordered_map<std::string, std::vector<unsigned int> > _ts_index_map;
    std::unordered_map<unsigned int, char*> _ts_data_map;
};

} // namespace RiverDB
