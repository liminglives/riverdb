#include "riverdb_reader.h"

namespace RiverDB {

RiverDBReader::RiverDBReader(const std::string& binary_file, const std::string split) : _split(split) {
    _is_gzfile = Util::is_gzfile(binary_file);
    if (_is_gzfile) {
        _reader = new GZFileReader(binary_file);
    } else {
        _reader = new MMapFileReader(binary_file);

    }
    if (_reader == NULL) {
        Throw("new reader failed");
    }
}

RiverDBReader::~RiverDBReader() {
    if (_reader) {
        delete _reader;
        _reader = NULL;
    }
}

void RiverDBReader::get_header_line(std::string& line) {
    for (auto it = _col_metas.begin(); it != _col_metas.end(); ++it) {
        //if (_filter_cols.empty() || _filter_cols.find(it->_col_name) != _filter_cols.end()) {
            line.append(it->_col_name);
            line.append(",");
        //}
    }
    line.pop_back();
}

int RiverDBReader::init() {
    int all_col_num = 0;
    try {
        read_header();
        std::string col;
        // scan col
        while(_reader->read_col(col, true) == RET_OK) {
            ++all_col_num;
        }
    } catch (RTTException& e) {
        std::cout << e.info() << std::endl;
        return RET_ERROR;
    }
    int col_num = get_col_size();
    if (all_col_num % col_num != 0) {
        std::cout << "data error" << std::endl;
        return RET_ERROR;
    }
    _row_size = all_col_num / col_num;
    _reader->set_cur(_reader->get_data_start());
    return RET_OK;
}

void RiverDBReader::read_header() {
    std::string line;
    char * buf = 0;
    int buf_len = 0;

    _reader->readline(line);
    std::vector<std::string> arr;
    Util::split(line, _split, arr);
    if (arr.size() == 0) {
        Throw("empty header, line:" + line);
    }
    _col_size = arr.size();
    ColMeta col_meta;
    _col_metas.reserve(arr.size());
    for (unsigned int i = 0; i < arr.size(); ++i) {
         if (_filter_cols.empty() || _filter_cols.find(arr[i]) != _filter_cols.end()) {
            col_meta._col_name = arr[i];
            _col_metas.push_back(col_meta);

            _filter_col_ids.insert(i);
        }
    }

    // read column data type
    line.clear();
    _reader->readline(line);
    arr.clear();
    Util::split(line, _split, arr);
    if (arr.size() != _col_size) {
        Throw("meta size is not equal");
    }
    int j = 0;
    for (unsigned int i = 0; i < arr.size(); ++i) {
        if (arr[i].size() == 0) {
            Throw("data type is empty");
        }
        if (_filter_col_ids.find(i) != _filter_col_ids.end()) {
            int type = std::stoi(arr[i]);
            if (type <= Type_START || type >= Type_END) {
                Throw("illegal data type " + arr[i]);
            }
            _col_metas[j++]._type = std::stoi(arr[i]);
        }
    }

    _reader->set_data_start(_reader->get_cur());
}

void RiverDBReader::load_data(char* buf, unsigned long long buf_size) {
    _reader->load(buf, buf_size);
}

int RiverDBReader::read_row(std::vector<std::string>& vals) {
    int read_col_num = 0;
    std::string val;
    while (read_col_num < _col_size) {
        
        val.clear();
        if (_reader->read_col(val) != RET_OK) {
            break;
        }
        if (_filter_col_ids.empty() ||  _filter_col_ids.find(read_col_num) != _filter_col_ids.end()) {
            //std::cout << read_col_num << std::endl;
            vals.push_back(val);
        }
        ++read_col_num;
    }
    if (read_col_num == 0) {
        //std::cout << "no read" << std::endl;
        return RET_READEND;
    }
    if (read_col_num != _col_size) {
        Throw("read col num not enough");
    }
    return RET_OK;
}

template <>
void RiverDBReader::get_value(std::string& bin_str, std::string* val) {
    *val = bin_str;
}

} // namespace RiverDB
