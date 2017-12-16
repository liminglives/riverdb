#pragma once

#include <string>
#include <fstream>
#include <unistd.h>

#include "defines.h"

namespace RiverDB {

class NormalFileReader {
public:
	NormalFileReader(const std::string& file) : _file(file) {
        if (access(file.c_str(), 0) != 0) {
            Throw(file + " is not existed");
        }
	    _in.open(_file, std::ifstream::in);
	}

	~NormalFileReader() {
		_in.close();
	}

    int readline(std::string& line) {
		line.clear();
		int ret = getline(_in, line) ? RET_OK : RET_READERROR;
		if (ret == RET_OK && !line.empty() && line[line.size() - 1] == '\r') {
			line.pop_back();
		}
		return ret;
	}
	int read(char* buf, unsigned int size) {
		return _in.read(buf, size) ? RET_OK : RET_READERROR;
	}
	unsigned long long tellg() {
		return _in.tellg();
	}
	int seekg(unsigned long long pos) {
		return _in.seekg(pos) ? RET_OK : RET_ERROR;
	}
	int gcount() {
		return _in.gcount();
	}
private:
	std::string _file;
	std::ifstream _in;
};


} // namespace RiverDB
