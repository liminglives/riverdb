#pragma once

#include "ifile_reader.h"
#include "zlib.h"

namespace RiverDB {

const static int GZREAD_BUF_SIZE = 1024 * 256;

class GZFileReader : public IFileReader {
public:
    GZFileReader(const std::string& fname); 
    ~GZFileReader(); 

    int readline(std::string& line); 

    int read(std::string& buf, int size, bool is_scan); 

    int read_col(std::string& col, bool is_scan = false); 

    unsigned long long get_cur() {
        return _cur;
    }

    void set_cur(unsigned long long cur) {
        _cur = cur;
        _end = cur;
        gzseek(_gf, cur, SEEK_SET);
    }

    void set_data_start(unsigned long long data_start) {
        _data_start = data_start;
    }
    unsigned long long get_data_start() {
        return _data_start;
    }

    virtual unsigned long long get_data_size() override {
        return _file_size - _data_start;
    }

    void load(char* buf, unsigned long long buf_size) override;

private:
    int gz_read(int start, int size); 

    bool read_char(char& c); 

private:
    gzFile _gf;
    char _buf[GZREAD_BUF_SIZE] = {0};
    unsigned long long _cur = 0;
    unsigned long long _end = 0;
    unsigned long long _data_start = 0;
    unsigned long long _file_size = 0;
};

} // namespace RiverDB

