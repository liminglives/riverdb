#pragma once

#include <string>
#include <vector>

namespace RiverDB {

class RiverDB {
public:
    RiverDB();
    ~RiverDB();

    bool load(const std::string& fpath);

    void append(const std::vector<std::string>& row);

private:
    std::vector<unsigned int> _time_list;
    char* _data;
};

} // namespace RiverDB
