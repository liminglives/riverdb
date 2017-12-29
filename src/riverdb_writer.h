#pragma once

#include <unordered_map>
#include <vector>
#include <iostream>

#include "util.h"
#include "normal_file_reader.h"
#include "gz_file_writer.h"
#include "normal_file_writer.h"

namespace RiverDB {

class RiverDBWriter {
public:
    RiverDBWriter(const std::string& out_file, int mode = FileOpenModeWrite) {
        _file_write_mode = mode;
        if (_file_write_mode == FileOpenModeAppend) {
            if (load_col_metas(out_file) != RET_OK) {
                Throw("load col metas, file:" + out_file);
            }
        }
        _is_gzfile = Util::is_gzfile(out_file);
        if (_is_gzfile) {
            _writer = new GZFileWriter(out_file, mode);
        } else {
            _writer = new NormalFileWriter(out_file, mode);
        }
        if (_writer == NULL) {
            Throw("new writer failed");
        }
    }

    ~RiverDBWriter() {
        delete _writer;
    }

    //template <class T> 
    //int get_str_from_val(const T& val, std::string& str) {
    //    str.assign(static_cast<char*>(static_cast<void*>(const_cast<T *>(&val))), sizeof(T));
    //    return RET_OK;
    //}


    int load_col_dict(const std::string& dict_file) {
        NormalFileReader dict_reader(dict_file);
        build_cols_dict(dict_reader);
        return RET_OK;
    }

    int write_header() {
        write_header(_writer);
        return RET_OK;
    }

    int get_data_type(int index) {
        if (index < 0 || index >= _col_metas.size()) {
            return -1;
        }
        return _col_metas[index]._type; 
    }

    void flush() {
        _writer->flush();
    }

    int write_row(const std::vector<std::string>& row) {
        return write_binary_line(_writer, row);
    }

    template <class T> int push_row(const T& val, std::vector<std::string>& row) {
        Util::push_row(val, row);
        //std::string str;
        //Util::get_str_from_val<T>(val, str);
        //row.push_back(str);
        //return RET_OK;
    }

    void push_col_meta(const RowBinaryColMeta& col_meta) {
        if (col_meta._type <= Type_START || col_meta._type >= Type_END) {
            Throw("push error, unknown data type " + std::to_string(col_meta._type));
        }
        _col_metas.push_back(col_meta);
    }

    void push_col_meta(const std::string& col_name, int datatype) {
        RowBinaryColMeta col_meta;
        col_meta._col_name = col_name;
        col_meta._type = datatype;
        push_col_meta(col_meta);
    }

    int get_col_size() {
        return _col_metas.size();
    }

    void convert(const std::string& csv_file, const std::string& dict_file, const std::string split = ","); 

private:
    void build_cols_dict(NormalFileReader& reader); 
    void parse_csv_header(NormalFileReader& reader, const std::string& split); 
    void parse_line(const std::string& line, std::vector<std::string>& vals, const std::string& split); 
    int write_binary_line(IFileWriter* writer, const std::vector<std::string>& vals); 
    void write_header(IFileWriter* writer); 
    int load_col_metas(const std::string& file);

private:
    IFileWriter* _writer;
    std::string _split;
	std::unordered_map<std::string, DataType> _col_datatype_map;
    std::vector<RowBinaryColMeta> _col_metas;
    bool _is_gzfile = false;
    int _file_write_mode;
};

} //  namespace RiverDB
