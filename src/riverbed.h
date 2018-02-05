#pragma once

#include "defines.h"

namespace RiverDB {

class Riverbed {
public:
    Riverbed();
    ~Riverbed();

    bool init(uint64_t buf_len, RiverHeader* river_header, 
            const std::string& dump_fpath);
    void close();

    void write_header();
    void append(const char* data, uint32_t data_len, 
            EnumRowHeaderType type, EnumRowHeaderFlag flag = RowHeaderFlag_OK);
    char* get_buf();

    void notify();
    void gc();
    void dump();
private:
    void write(const char* data, uint64_t len);
    void write_line(const char* data, uint64_t len);
    void write_row_header(EnumRowHeaderFlag flag, EnumRowHeaderType type, uint32_t data_len);
private:
    char* _buf = NULL;
    uint64_t _buf_len = 0;
    uint64_t _data_start = 0;
    uint64_t _cursor = 0;
    uint64_t* _data_len = NULL;
    RiverHeader* _river_header = NULL;

};

} // namespace RiverDB
