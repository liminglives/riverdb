#include "mmap_file_reader.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>

#include "util.h"
#include "defines.h"

namespace RiverDB {

MMapFileReader::~MMapFileReader() {
    munmap(_buf, _file_size);
    close(fd);
}

MMapFileReader::MMapFileReader(const std::string& file) {
    struct stat st;
    if (stat(file.c_str(), &st) == -1) {
        Throw("failed to get file stat, " + file);
    }
    fd = open(file.c_str(), O_RDONLY);
    if (fd == -1) {
        Throw("failed to open file " + file);
    }
    _file_size = st.st_size;
    _buf = static_cast<char *>(mmap(NULL, _file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (_buf == MAP_FAILED || _buf == NULL) {
        Throw("failed to mmap");
    }
}

int MMapFileReader::readline(std::string& line) {
    char* buf = NULL;
    int len = 0;
    int ret = readline(buf, len);
    if (ret == RET_OK) {
        line.assign(buf, len);
    }
    return ret;
}

int MMapFileReader::readline(char* &buf, int& len) {
    unsigned long long pos = _cur;
    buf = _buf + _cur;
    len = 0;
    while (pos < _file_size) {
        if (_buf[pos] == '\n') {
            break;
        }
        ++pos;
    }
    len = pos - _cur;
    if (len > 0 && buf[pos - 1] == '\r') {
        --len;
    }

    _cur = pos + 1;
    if (len == 0 && pos >= _file_size) {
        return RET_READERROR;
    }
    return RET_OK;
}

int MMapFileReader::read(char*& buf, int size) {
    unsigned long long pos = _cur;
    buf = _buf + _cur;
    int len = 0;
    while (pos < _file_size && pos - _cur < size) {
        ++pos;
    }
    len = pos - _cur;
    _cur = pos;

    return len;
}

int MMapFileReader::read_col(std::string& val, bool is_scan) {
    char * buf = 0;
    int len = 0;
    int ret = read_col(buf, len); 
    if (ret == RET_OK && !is_scan) {
        val.assign(buf, len);
    } 
    return ret;
}

int MMapFileReader::read_col(char*& buf, int& len) {
    if (_cur >= _file_size) {
        return RET_READERROR;
    }
    unsigned int read_len = 0;
    char mark = _buf[_cur++];
    unsigned long long pos = _cur;
    if (mark == '\0') {
        while (pos < _file_size && _buf[pos] != '\0') {
            ++pos;
        }
        read_len = pos - _cur;

        ++pos;
    } else {
        read_len = mark - '0';
        pos += read_len;
    }
    if (read_len + _cur > _file_size) {
        std::cout << read_len + _cur << std::endl;
        Throw("read overflow");
    }
    buf = _buf + _cur;
    len = read_len;
    //val.assign(_buf + _cur, read_len);
    _cur = pos;
    //std::cout << "read_len:" << read_len << " mark:" << mark  << std::endl;
    return RET_OK;
}

} // namespace RiverDB
