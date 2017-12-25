#include "gz_file_reader.h"

#include <iostream>

#include "defines.h"
#include "util.h"

namespace RiverDB {

GZFileReader::GZFileReader(const std::string& fname) {
    _gf = gzopen(fname.c_str(), "rb");
    if (_gf == NULL) {
        std::cout << "reader gz open error" << std::endl;
        Throw("reader gz open failed");
    }
    std::cout << "gz file_size: " << _gf->have;
    _file_size = _gf->have;
}

GZFileReader::~GZFileReader() {
    gzclose(_gf);
}

int GZFileReader::readline(std::string& line) {
    char c;
    while (read_char(c) && c != '\n') {
        line.push_back(c);
    }
    if (!line.empty() && line[line.size() - 1] == '\r') {
        line.pop_back();
    }
    //std::cout << line<<std::endl;

    return RET_OK;
}

int GZFileReader::read(std::string& buf, int size, bool is_scan) {
    char c;
    int len = 0;
    while (len < size && read_char(c)) {
        if (!is_scan) {
            buf.push_back(c);
        }
        ++len;
    }

    return len;
}

int GZFileReader::read_col(std::string& col, bool is_scan) {
    char mark;
    if (!read_char(mark)) {
        return RET_READERROR;
    }
    if (mark == '\0') {
        char c;
        while (read_char(c) && c != '\0') {
            if (!is_scan) {
                col.push_back(c);
            }
        }
        //std::cout << "read col,  real:"<< col<< std::endl;
    } else {
        int readn = mark - '0';
        int ret = read(col, readn, is_scan);
        //std::cout << "read col,  real:"<<ret<<" exp:"<< readn << std::endl;
        if (ret != readn) {
            Throw("read col failed");
        }
    }

    return RET_OK;
}

void GZFileReader::load(char* data_buf, unsigned long long data_buf_size) {
    int remain_size = data_buf_size;
    char* buf_start = data_buf;
    int readn = 0;
    while (remain_size > 0) {
        readn = remain_size;
        int ret_readn = gzread(_gf, buf_start, readn);
        remain_size -= ret_readn;
        buf_start = buf_start + ret_readn;
    }
}

int GZFileReader::gz_read(int start, int size) {
    int readn = gzread(_gf, _buf + start, size);
    if (readn > 0) {
        _end += readn;
    }
    return readn;
}

bool GZFileReader::read_char(char& c) {
    if (_cur == _end) {
        int pos = _cur % GZREAD_BUF_SIZE;
        int readn = GZREAD_BUF_SIZE - pos;

        int real_readn = gz_read(pos, readn);
        if (real_readn <= 0) {
            //std::cout << "read end" << std::endl;
            return false;
        }
    } else if (_cur > _end) {
        std::cout << "read char error" << std::endl;
        Throw("read char error");
    }

    c = _buf[_cur++ % GZREAD_BUF_SIZE];

    return true;
}

} // namespace RiverDB
