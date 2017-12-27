#pragma once

#include "data_container.h"

#include <string>
#include <vector>

namespace RiverDB {

class RiverDB {
public:
    RiverDB();
    ~RiverDB();

    bool init(const std::string& primary_key,
            const std::string& index_key);
    bool load(const std::string& fpath); 
    
    RowReader* new_row_reader();

    bool get(const std::string& kvalue, uint64_t ts, RowReader* row_reader);
    bool at(const std::string& kvalue, int index, RowReader* row_reader);
            

private:
    DataContainer* _data_container;
};

} // namespace RiverDB
