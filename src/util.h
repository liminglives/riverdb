#pragma once 

#include <string>
#include <chrono>
#include <vector>

#include "defines.h"

namespace RiverDB {

#define Throw(x) do{throw RTTException(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

class RTTException {
public:
    RTTException(const std::string& file, int line, const std::string& func, const std::string& info="") :
	       _file(file), _line(line), _func(func), _info(info) {} std::string info() {
		std::string ret;
		ret.append(_file);
		ret.append(":");
		ret.append(std::to_string(_line));
		ret.append(" ");
		ret.append(_func + "():");
		ret.append(_info);
		return ret;
	}
private:
    std::string _file;
    int _line;
    std::string _func;
    std::string _info;
};

namespace Util {

void split(const std::string& src, const std::string& separator, std::vector<std::string>& dest);

std::string& trim(std::string &s);

DataType get_datatype(const std::string& in); 

void parse_val_from_str(const std::string& str, const int type, std::string& val); 

std::time_t getTimeStamp();

void encode_str(const std::string& str, const std::string& outfile); 

std::string decode_str(const std::string& file); 

bool is_gzfile(const std::string& fname);

unsigned int get_file_size(const std::string& fname);

} // namespace Util
} // namespace RiverDB
