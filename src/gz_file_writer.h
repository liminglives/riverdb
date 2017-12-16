#pragma once

#include "ifile_writer.h"

#include "zlib.h"

namespace RiverDB {

class GZFileWriter : public IFileWriter {
public:
    GZFileWriter(const std::string& fname) {
        _gf = gzopen(fname.c_str(), "wb");
        if (_gf == NULL) {
            std::cout << "writer gz open error" << std::endl;
            Throw("gz open " + fname + " failed");
        }
    }

    void write(const char* str, unsigned int size) {
        if (gzwrite(_gf, str, size) != size) {
            std::cout << "gz write error" << std::endl;
            Throw("gz write failed");
        }
    }

    void write_char(char c) {
        gzputc(_gf, c);
    }

    void writeline(const std::string& str) {
        write(str.c_str(), str.size());
        write_char('\n');
    }

    void flush() {
        gzflush(_gf, Z_NO_FLUSH);
    }

    ~GZFileWriter() {
        gzflush(_gf, Z_FINISH);
        gzclose(_gf);
    }


private:
    gzFile _gf;
};

} // namespace RiverDB
