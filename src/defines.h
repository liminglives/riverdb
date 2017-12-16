#pragma once 

#include <string>

namespace RiverDB {

enum RETCODE {
	RET_OK = 0,
	RET_ERROR,
	RET_READERROR,
	RET_EMPTY,
	RET_READEND,
};

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

class RowBinaryColMeta {
public:
    std::string _col_name;
    int _type;
};

class EmptyValue {
};

} // namespace RiverDB
