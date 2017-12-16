#pragma once

#include <unordered_set>
#include <vector>
#include <iostream>

#include "defines.h"
#include "util.h"
#include "gz_file_reader.h"
#include "mmap_file_reader.h"

namespace RiverDB {

class RiverDBReader {
public:
    RiverDBReader(const std::string& binary_file, const std::string split = ",") : _split(split) {
        _is_gzfile = Util::is_gzfile(binary_file);
        if (_is_gzfile) {
            _reader = new GZFileReader(binary_file);
        } else {
            _reader = new MMapFileReader(binary_file);

        }
        if (_reader == NULL) {
            Throw("new reader failed");
        }
    }

    ~RiverDBReader() {
        if (_reader) {
            delete _reader;
            _reader = NULL;
        }
    }

    int init();
    void read_header(); 

    bool is_gzfile() {
        return _is_gzfile;
    }

    void add_filter_col(const std::string& col_name) {
        _filter_cols.insert(col_name);
    }

    int read_row(std::vector<std::string>& vals); 

    void get_header_line(std::string& line) {
        for (auto it = _col_metas.begin(); it != _col_metas.end(); ++it) {
            //if (_filter_cols.empty() || _filter_cols.find(it->_col_name) != _filter_cols.end()) {
                line.append(it->_col_name);
                line.append(",");
            //}
        }
        line.pop_back();
    }
    const std::vector<RowBinaryColMeta>& get_col_metas() {
        return _col_metas;
    }

    int get_col_datatype(int index) {
        if (index < 0 || index > _col_metas.size()) {
            return RET_ERROR;
        }
        return _col_metas[index]._type;
    }

    int get_col_name(int index, std::string& col_name) {
        if (index < 0 || index > _col_metas.size()) {
            return RET_ERROR;
        }
        col_name = _col_metas[index]._col_name;
        return RET_OK;
    }

    int get_col_size() { // return filtered column size
        return _col_metas.size();
    }

    int get_row_size() {
        return _row_size;
    }

    template <class T> void get_value(std::string& bin_str, T * val) {
	    *val = *(static_cast<T *>(static_cast<void *>(const_cast<char *>(bin_str.c_str()))));
        //std::cout << "[" << *(static_cast<T *>(static_cast<void *>(const_cast<char *>(bin_str.c_str())))) << "]";
    } 

    
private:
    std::vector<RowBinaryColMeta> _col_metas;
    //BinaryMMapReader _reader;
    IFileReader *_reader;
    std::string _split;
    std::unordered_set<std::string> _filter_cols;
    std::unordered_set<int> _filter_col_ids;
    int _row_size = 0; // all row size
    int _col_size = 0; // all column size
    bool _is_gzfile = false;
};

} // namespace RiverDB
