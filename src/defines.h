#pragma once 

#include <string>

namespace RiverDB {

enum EnumFileOpenMode {
    FileOpenModeWrite = 0,
    FileOpenModeAppend,
};

enum RETCODE {
	RET_OK = 0,
	RET_ERROR,
	RET_READERROR,
	RET_EMPTY,
	RET_READEND,
	RET_NOTFOUND,
};

using INT16 = int16_t;
using INT32 = int32_t;
using INT64 = int64_t;
using UINT16 = uint16_t;
using UINT32 = uint32_t;
using UINT64 = uint64_t;
using FLOAT = float;
using DOUBLE = double;
using LONGDOUBLE = long double;
using STRING = std::string;

enum DataType {
    Type_START = 0, // must be the first one

    Type_INT16= 1,	 // size 2
    Type_INT32 = 2,	// size 4
    Type_INT64 = 3,	// size 8 
    Type_UINT16 = 4,  // 
    Type_UINT32 = 5,  // 
    Type_UINT64 = 6, // 
    Type_FLOAT = 7, //  size 4
    Type_DOUBLE = 8, // size 8
    Type_LD = 9, // long double, size 16. not use 
    Type_STRING = 10,

	Type_END // must be the last one
};

enum class QueryOP : int {
    EQ = 1,
    GT,
    GE,
    LT,
    LE,
};

class ColMeta {
public:
    std::string _col_name;
    int _type;
};

class EmptyValue {
};

struct Data {
    char* data;
    unsigned int len;
};

} // namespace RiverDB
