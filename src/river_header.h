#pragma once

#include <vector>
#include <unordered_map>

#include "defines.h"


namespace RiverDB {

class RiverHeader {
public:
    RiverHeader();
    ~RiverHeader();

    bool init_header(const std::vector<ColMeta>& col_metas); 
    bool parse_header(const std::vector<std::string>& header_lines); 
    const std::vector<std::string>& header_lines() {
        return _header_lines;
    }
private:
    void build_index();
private:
    std::vector<std::string> _header_lines;
    std::vector<ColMeta> _col_metas;
    std::unordered_map<std::string, int> _col_name_index_map;
};

} // namespace RiverDB
