#pragma once

#include "ifile_reader.h"

namespace RiverDB {

class MMapFileReader : public IFileReader {
public:
    MMapFileReader(const std::string& file); 
    ~MMapFileReader(); 

    char * get_buf() {
        return _buf;
    }

    unsigned long long get_file_size() {
        return _file_size;
    }

    int readline(std::string& line) override;
    
    int readline(char* &buf, int& len); 

    int read(char* &buf, int size);

    int read_col(std::string& val, bool is_scan = false) override; 
    
    int read_row(char*&  row_data, int& size);

    int read_col(char*& buf, int& len);

    unsigned long long get_cur() override {
        return _cur;
    }
    void set_cur(unsigned long long cur) override {
        _cur = cur;
    }

    void set_data_start(unsigned long long data_start) override {
        _data_start = data_start;
    }
    unsigned long long get_data_start() override {
        return _data_start;
    }
    void load(char* buf, unsigned long long buf_size) override; 

    unsigned long long get_data_size() override {
        return _file_size - _data_start;
    }

private:
    int fd = 0;
    char * _buf = 0;
    unsigned long long _file_size = 0;
    unsigned long long _cur = 0;
    unsigned long long _data_start = 0;
};

} // namespace RiverDB

