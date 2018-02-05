#include "riverbed.h"

namespace RiverDB {

Riverbed::Riverbed() {}

Riverbed::~Riverbed() {
    close();
}

bool Riverbed::init(uint64_t buf_len, RiverHeader* river_header,
        const std::string& dump_fpath) {
    _buf_len = buf_len;
    _buf = (char*)malloc(_buf_len);

    _river_header = river_header;

    return true;
}

char* Riverbed::get_buf() {
    return _buf;
}

void Riverbed::close() {
    if (_buf) {
        free(_buf);
        _buf = NULL;
    }
}

void Riverbed::write(const char* data, uint64_t len) {
    if (_cursor + len > _buf_len) {
        Throw("write overflow, cursor:" + std::to_string(_cursor) + 
                " len:" + std::string(len) + 
                " buf_len" + std::string(_buf_len));
    }
    memcpy(_buf + _cursor, data, len);
    _cursor += len;
}

void Riverbed::write_line(const char* data, uint64_t len) {
    write(data, len);
    char c = '\n';
    write(&c, sizeof(c));
}

void Riverbed::write_header() {
    auto header_lines = _river_header->header_lines();
    
    _cursor = 0;
    for (const auto& line : header_lines) {
        write_line(line.c_str(), line.size());
    }

    _data_len = static_cast<uint64_t*>(_cursor);
    *_data_len = 0;
    _cursor += sizeof(uint64_t);
    _data_start = _cursor;
}

void Riverbed::write_row_header(EnumRowHeaderFlag flag, EnumRowHeaderType type, uint32_t data_len) {
    if (sizeof(RowHeader) + _cursor > _buf_len) {
        Throw("write row header overflow");
    }
    RowHeader* row_header = (RowHeader*)_cursor;
    memset(row_header, 0, sizeof(RowHeader));
    _cursor += sizeof(RowHeader);
    row_header->flag = static_cast<int8_t>(flag);
    row_header->type = static_cast<int8_t>(type);
    row_header->len = data_len;
}

void Riverbed::append(const char* data, uint32_t data_len, 
        EnumRowHeaderType type, EnumRowHeaderFlag flag) {
    write_row_header(RowHeaderFlag_OK, type, data_len);
    write(data, data_len);
    *_data_len = _cursor - _data_start;
}

} // namespace RiverDB
