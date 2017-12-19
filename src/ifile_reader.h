#pragma once

#include <string>

namespace RiverDB {

class IFileReader {
public:
    IFileReader() = default;
    virtual ~IFileReader() {}

    virtual int readline(std::string& line) = 0;

    virtual int read_col(std::string& val, bool is_scan = false) = 0; 

    virtual unsigned long long get_cur() = 0; 

    virtual void set_cur(unsigned long long cur) = 0; 

    virtual void set_data_start(unsigned long long data_start) = 0; 

    virtual unsigned long long get_data_start() = 0; 

    virtual unsigned long long get_data_size() = 0;

    virtual void load(char* buf, unsigned long long buf_size) = 0; 

};

} // namespace RiverDB

