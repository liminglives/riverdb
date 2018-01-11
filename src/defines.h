#pragma once 

#include <string>

namespace RiverDB {

static const std::string RiverDBHeaderMark = "##!RiverDB##";
static const std::string RiverDBHeaderV00 = "v0.0";
static const std::string RiverDBHeaderV10 = "v1.0";
static const std::string RiverDBHeaderSplit = ",";

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
    DT_START = 0, // must be the first one

    DT_INT16= 1,	 // size 2
    DT_INT32 = 2,	// size 4
    DT_INT64 = 3,	// size 8 
    DT_UINT16 = 4,  // 
    DT_UINT32 = 5,  // 
    DT_UINT64 = 6, // 
    DT_FLOAT = 7, //  size 4
    DT_DOUBLE = 8, // size 8
    DT_LD = 9, // long double, size 16. not use 
    DT_STRING = 10,

	DT_END // must be the last one
};

enum class QueryOP : int {
    EQ = 1,
    GT,
    GE,
    LT,
    LE,
    INDEX,
};

struct ColMeta {
    std::string name;
    int type;
};


struct RowHeader {
    int8_t flag;
    int8_t type;
    char reserved[2];
    uint32_t len;
};

enum EnumRowHeaderFlag : int8_t {
    RowHeaderFlag_Deleted = -1,  
    RowHeaderFlag_OK = 0,  
};

enum EnumRowHeaderType : int8_t {
    RowHeaderType_Add = 0,  
    RowHeaderType_Del,  
    RowHeaderType_Motify,  
};



class EmptyValue {
};

struct Data {
    char* data;
    unsigned int len;
};

} // namespace RiverDB
