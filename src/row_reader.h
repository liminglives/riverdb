#pragma once

#include <unordered_map>
#include <vector>

#include "defines.h"
#include "util.h"

namespace RiverDB {

class RowReader {
public:
    RowReader(std::vector<RowBinaryColMeta>* col_metas, 
            std::unordered_map<std::string, int>* col_index_map);
    ~RowReader();

    bool init(char* data, unsigned int max_len);

    unsigned int get_len() {
        return _len;
    }

    template <class T>
    bool at(unsigned int index, T* value) {
        if (index >= _col_num) {
            return false;
        }
        Util::get_value(_index[index], value);
        return true;
    }

    template <class T>
    bool get(const std::string& col_name, T* value) {
        auto it = _col_index_map->find(col_name);
        if (it == _col_index_map->end()) {
            return false;
        }
        return at(it->second, value);
    }

private:
    int read_col(unsigned int max_len);

private:
    char* _data;
    std::vector<RowBinaryColMeta>* _col_metas;
    std::unordered_map<std::string, int>* _col_index_map;    
    unsigned int _col_num;
    std::vector<char *> _index;
    unsigned int _cur;
    unsigned int _len;

};

} // namespace RiverDB
