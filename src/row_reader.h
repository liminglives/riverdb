#pragma once

#include <unordered_map>
#include <vector>
#include <queue>
#include <limits>

#include "defines.h"
#include "util.h"

namespace RiverDB {

class RowReader {
public:
    RowReader(std::vector<ColMeta>* col_metas, 
            std::unordered_map<std::string, int>* col_index_map);
    virtual ~RowReader();

    bool init(char* data, unsigned int max_len = std::numeric_limits<unsigned int>::max());

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
            Log("col_name:" + col_name + " not found");
            return false;
        }
        return at(it->second, value);
    }

private:
    int read_col(unsigned int max_len);

private:
    char* _data;
    std::vector<ColMeta>* _col_metas;
    std::unordered_map<std::string, int>* _col_index_map;    
    unsigned int _col_num;
    std::vector<char *> _index;
    unsigned int _cur;
    unsigned int _len;

};

class RowsReader : public RowReader {
public:
    RowsReader(std::vector<ColMeta>* col_metas, 
            std::unordered_map<std::string, int>* col_index_map);
    ~RowsReader();

    void push(char* data, uint64_t max_len = std::numeric_limits<uint64_t>::max());
    bool next();
    void reset() {
        _cur = 0;
    }

public:
    int _cur = 0;
    std::vector<std::pair<char*, uint64_t> > _data_vec;
};

} // namespace RiverDB
