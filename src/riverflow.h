#pragma once

#include "defines.h"

namespace RiverDB {

class RiverFlow {
public:
    RiverFlow();
    ~RiverFlow();

    bool init();

    // list
    bool linsert(const std::string& key, uint64_t ts, Data* data);
    bool ldel_by_index(const std::string& key, int start, int end);
    bool ldel(const std::string& key, uint64_t ts_start, uint64_t ts_end);
    bool lindex(const std::string& key, int index, Data* data);
    bool lindex_range(const std::string& key, int start, int end, 
            std::vector<Data*>& data_vec);
    bool lget(const std::string& key, uint64_t ts, Data* data);
    bool lrange(const std::string& key, uint64_t ts_start, uint64_t ts_end, 
            std::vector<Data*>& data_vec);

    // kv
    bool set(const std::string& key, Data* data);
    bool get(const std::string& key, Data* data);
    bool del(const std::string& key);

private:
    Riverbed* _riverbed;
    std::unordered_map<std::string, DataIndex*> _data_index_map;

};

} // namespace RiverDB
