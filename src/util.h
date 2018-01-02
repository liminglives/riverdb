#pragma once 

#include <string>
#include <chrono>
#include <vector>
#include <iostream>

#include "defines.h"

namespace RiverDB {

#define Throw(x) do { throw RTTException(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

#define Log(x) do { Print(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

void Print(const std::string& file, int line, const std::string& func, const std::string& info="");

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

const int BinarySearchLEFlag = -1;
const int BinarySearchGEFlag = 1;
const int BinarySearchEqualFlag = 0;
const int BinarySearchLTFlag = -2;
const int BinarySearchGTFlag = 2;

void split(const std::string& src, const std::string& separator, std::vector<std::string>& dest);

std::string& trim(std::string &s);

DataType get_datatype(const std::string& in); 

void parse_val_from_str(const std::string& str, const int type, std::string& val); 

std::time_t getTimeStamp();

void encode_str(const std::string& str, const std::string& outfile); 

std::string decode_str(const std::string& file); 

bool is_gzfile(const std::string& fname);

unsigned int get_file_size(const std::string& fname);

int binary_search(const std::vector<uint64_t>& ts_vec, uint64_t ts, int flag); 
    
void sort_insert(std::vector<uint64_t>& ts_vec, uint64_t ts);

template <class T> void get_value(char* data, T * val) {
    *val = *(static_cast<T *>(static_cast<void *>(data)));
} 

template <class T> 
void get_str_from_val(const T& val, std::string& str) {
    str.assign(static_cast<char*>(static_cast<void*>(const_cast<T *>(&val))), sizeof(T));
}

template <class T> int push_row(const T& val, std::vector<std::string>& row) {
    std::string str;
    get_str_from_val<T>(val, str);
    row.push_back(str);
    return RET_OK;
}

bool serialize_row(const std::vector<RowBinaryColMeta>& col_metas, 
        const std::vector<std::string>& row,
        char*& data, unsigned int& len); 


} // namespace Util
} // namespace RiverDB
