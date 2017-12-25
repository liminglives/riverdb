#include "util.h"

#include <chrono>
#include <sys/stat.h>

namespace RiverDB {

void Print(const std::string& file, int line, const std::string& func, const std::string& info) {
    std::cout << file << ":" << line << " " << func << " " << info << std::endl;
}

namespace Util {

void split(const std::string& src, const std::string& separator, std::vector<std::string>& dest) {
	//dest.clear();
    using namespace std;
    if (src.empty()) {
        return;
    }
    string str = src;
    string substring;
    string::size_type start = 0, index;

    do {
        index = str.find_first_of(separator,start);
        if (index != string::npos) {    
            substring = str.substr(start,index-start);
            dest.push_back(trim(substring));
			start = index + separator.size();
            //start = str.find_first_not_of(separator,index);
            if (start == string::npos) return;
        }
    } while (index != string::npos);
    
    //the last token
    substring = str.substr(start);
    dest.push_back(trim(substring));
}

std::string& trim(std::string &s) {  
    if (s.empty()) {  
        return s;  
    }  
    s.erase(0,s.find_first_not_of(" "));  
    s.erase(s.find_last_not_of(" ") + 1);  
    return s;  
} 

DataType get_datatype(const std::string& in) {
	if (in.empty()) {
		return Type_END;
	}
    bool has_dot = false;
    bool has_number = false;
	bool has_other = false;
    for (unsigned int i = 0; i < in.size(); ++i) {
		//if ((in[i] >= 'a' && in[i] =< "z") || (in[i] >= 'A' && in[i] =< 'Z')) {
		//	has_char = true;
		if (in[i] == '.') {
			has_dot = true;
		} else if (in[i] >= '0' && in[i] <= '9') {
			has_number = true;
		} else {
			has_other = true;
		}
    }
	if (has_other || !has_number) {
		return Type_STRING;
	} else if (has_dot && has_number) {
		return Type_FLOAT;
	} else {
		return Type_INT32;
	}
}

void parse_val_from_str(const std::string& str, const int type, std::string& val) {
	if (str.empty()) {
		return;
	}
	switch (type) {
		case Type_INT16:
		{
		    int16_t v = static_cast<int16_t>(std::stoi(str));
		    val.assign((char*)&v, sizeof(int16_t));
			break;
		}
		case Type_INT32:
		{
		    int32_t v = (std::stoi(str));
		    val.assign((char*)&v, sizeof(int32_t));
			break;
		}
		case Type_INT64:
		{
		    int64_t v = std::stol(str);
		    val.assign((char*)&v, sizeof(int64_t));
			break;
		}
		case Type_UINT16:
		{
		    uint16_t v = static_cast<uint16_t>(std::stoul(str));
		    val.assign((char*)&v, sizeof(uint16_t));
			break;
		}
		case Type_UINT32:
		{
		    uint32_t v = static_cast<uint32_t>(std::stoul(str));
		    val.assign((char*)&v, sizeof(uint32_t));
			break;
		}
		case Type_UINT64:
		{
		    uint64_t v = static_cast<uint64_t>(std::stoul(str));
		    val.assign((char*)&v, sizeof(uint64_t));
			break;
		}
		case Type_FLOAT:
		{
		    float v = std::stof(str);
		    val.assign((char*)&v, sizeof(float));
			break;
		}
		case Type_DOUBLE:
		{
		    double v = std::stod(str);
		    val.assign((char*)&v, sizeof(double));
			break;
		}
		case Type_LD:
		{
		    long double v = std::stold(str);
		    val.assign((char*)&v, sizeof(long double));
			break;
		}
		case Type_STRING:
		{
		    val = str;
			break;
		}
		default:
		    Throw( "unkonw type");
	}
}

std::time_t getTimeStamp() {
    std::chrono::time_point<std::chrono::system_clock,std::chrono::milliseconds> tp = 
        std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    auto tmp=std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
    std::time_t timestamp=tmp.count();
    //std::time_t timestamp=std::chrono::system_clock::to_time_t(tp);
    return timestamp;
}

bool is_gzfile(const std::string& fname) {
    if ((fname.size() > 3) && 
            (fname.substr(fname.size() - 3) == ".gz")) {
        return true;
    } else {
        return false;
    }
}

unsigned int get_file_size(const std::string& fname) {
    struct stat info;
    stat(fname.c_str(), &info);
    return info.st_size;
}

template <>
void get_value(char* data, std::string* val) {
    int len = 0;
    while (*(data + len) != '\0') { ++len; }

    val->assign(data, len);
}

} // namespace Util
} // namespace RiverDB
