#pragma once

#include <string>

namespace RiverDB {

class IFileWriter {
public:
    virtual ~IFileWriter() {}

	virtual void writeline(const std::string& line) = 0; 
	virtual void write(const char* str, unsigned int size) = 0; 
	virtual void write_char(char c) = 0;
    virtual void flush() = 0; 
};

} // namespace RiverDB
