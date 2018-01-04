#include "riverdb_writer.h"
#include "riverdb_reader.h"

namespace RiverDB {

void RiverDBWriter::build_cols_dict(NormalFileReader& reader) {
	std::string line;
	//std::cout << "create column" << std::endl;
	while (reader.readline(line) == RET_OK) {
		//std::cout << "line:" << line << std::endl;
		if (line.empty() || line[0] == '#') {
			continue;
		}
		std::vector<std::string> val;
		Util::split(line, ":", val);
		if (val.size() != 2 || val[0].empty() || val[1].empty()) {
			printf("error line:%s", line.c_str());
			Throw( "error line in dict:" + line);
		}
		DataType data_type = static_cast<DataType>(std::stoi(val[1], nullptr));
		if (data_type <= Type_START || data_type >= Type_END) {
			Throw( "unkonw type for [" + line + "]");
		}
		if (_col_datatype_map.find(val[0]) != _col_datatype_map.end()) {
		    Throw( "repeated col:" + val[0]);	
		}
		_col_datatype_map[val[0]] = data_type;
	}
}

int RiverDBWriter::load_col_metas(const std::string& file) {
    RiverDBReader reader(file);
    //int ret = reader.init();
    //if (ret != RET_OK) {
    //    Log("init RiverDBReader failed, file:" + file);
    //    return ret;
    //}
    reader.read_header();
    _col_metas = reader.get_col_metas();

    return RET_OK;
}

void RiverDBWriter::convert(const std::string& csv_file, const std::string& dict_file, const std::string split) {
    NormalFileReader dict_reader(dict_file);
    build_cols_dict(dict_reader);

    NormalFileReader csv_reader(csv_file);
    //FileWriter binary_writer(binary_file);

    parse_csv_header(csv_reader, split);

    write_header(_writer);

    std::vector<std::string> vals;
    vals.reserve(_col_metas.size());
    std::string line;
    while (csv_reader.readline(line) == RET_OK) {
        parse_line(line, vals, split);
        if (RET_OK != write_binary_line(_writer, vals)) {
            std::cout << "write error line:" << line << std::endl;
        }
        vals.clear();
        line.clear();
    }
}


void RiverDBWriter::parse_csv_header(NormalFileReader& reader, const std::string& split) {
	std::string line;
    std::vector<std::string> str_vals;

    // parse header
	int ret = reader.readline(line); 
	if (ret != RET_OK) {
		Throw( "read csv header error");
	}
	//std::cout << "header:" << line << std::endl;
	Util::split(line, split, str_vals);
	if (str_vals.empty()) {
		Throw( "empty header");
	}
	if (str_vals.size() != _col_datatype_map.size()) {
		Throw( "csv header col size is not equal to dict col size");
	}
    ColMeta col_meta;
	for (unsigned int i = 0; i < str_vals.size(); ++i) {
		//std::cout << str_vals[i] << std::endl;
		if (_col_datatype_map.find(str_vals[i]) == _col_datatype_map.end()) {
			Throw( "csv col[" + str_vals[i] + "] cannot be found in dict col");
		}

        push_col_meta(str_vals[i], _col_datatype_map[str_vals[i]]);
	}
}

void RiverDBWriter::parse_line(const std::string& line, std::vector<std::string>& vals, const std::string& split) {
    std::vector<std::string> cols;
    Util::split(line, split, cols);
    if (cols.size() != _col_metas.size()) {
        std::cout << "cols size=" << cols.size() << ", " << _col_metas.size() << std::endl;
        std::cout << line << std::endl;
        Throw("col size is not equal to header col size");
    }
    for (unsigned int i = 0; i < cols.size(); ++i) {
        std::string val;
        Util::parse_val_from_str(cols[i], _col_metas[i]._type, val);
        vals.push_back(val);
    }
}

int RiverDBWriter::write_row(const char* data, unsigned int data_len) {
    _writer->write(data, data_len);
    return RET_OK;
}

int RiverDBWriter::write_binary_line(IFileWriter* writer, const std::vector<std::string>& vals) {
    char* data = NULL;
    unsigned int len;
    if (!Util::serialize_row(_col_metas, vals, data, len)) {
        Log("serialize_row failed");
        if (data) {
            free(data);
        }
        return RET_ERROR;
    }

    if (write_row(data, len) != RET_OK) {
        Log("write row failed");
        if (data) {
            free(data);
        }
        return RET_ERROR;
    }
    free(data);
    return RET_OK;
}

/*
int RiverDBWriter::write_binary_line(IFileWriter* writer, const std::vector<std::string>& vals) {
    if (vals.size() != _col_metas.size()) {
        //Throw("col val size is not equal to header col size");
        Log("col val size is not equal to header col size");
        return RET_ERROR;

    }
    for (unsigned int i = 0; i < vals.size(); ++i) {
        char mark = '\0';
        if (_col_metas[i]._type >= Type_INT16 && _col_metas[i]._type <= Type_LD) {
            mark = '0' + vals[i].size();
        }
        writer->write_char(mark);
        writer->write(vals[i].c_str(), vals[i].size());
        if (_col_metas[i]._type == Type_STRING) {
            writer->write_char('\0');
        }
    }
    return RET_OK;
}
*/

void RiverDBWriter::write_header(IFileWriter* writer) {
    //if (_file_write_mode == FileOpenModeAppend) {
    //    return;
    //}

    std::string type_line;
    std::string name_line;
    for (auto it = _col_metas.begin(); it != _col_metas.end(); ++it) {
        type_line.append(std::to_string(it->_type));
        type_line.append(",");
        name_line.append(it->_col_name);
        name_line.append(",");
    }
    type_line.pop_back();
    name_line.pop_back();

    writer->writeline(name_line);
    writer->writeline(type_line);
    writer->flush();
}

} // namespace RiverDB
