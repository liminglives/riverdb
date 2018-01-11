#include "row_reader.h"

namespace RiverDB {

RowReader::RowReader(std::vector<ColMeta>* col_metas, 
        std::unordered_map<std::string, int>* col_index_map) :
    _data(NULL), 
    _col_metas(col_metas),
    _col_index_map(col_index_map), 
    _col_num(_col_metas->size()), 
    _index(_col_num) {}

bool RowReader::init(char* data_start, unsigned int max_len) {
    _index.clear();
    _cur = 0;
    _len = 0;
    _data = data_start;

    for (int i = 0; i < _col_num; ++i) {
        if (read_col(max_len) != RET_OK) {
            Throw("read col error");
        }
    }
    _len = _cur;
    return true;
}

RowReader::~RowReader() {}

int RowReader::read_col(unsigned int max_len) {

    unsigned int read_len = 0;
    char mark = _data[_cur++];
    unsigned int pos = _cur;
    //std::cout << "mark::" << mark << std::endl;
    if (mark == '\0') {
        while (pos < max_len && _data[pos] != '\0') {
            ++pos;
        }
        read_len = pos - _cur;
        ++pos;
    } else {
        //Log("other");
        read_len = mark - '0';
        //std::cout << read_len << std::endl;
        pos += read_len;
    }
    if (read_len + _cur > max_len) {
        printf("mark:%c, %d\n", mark, mark - '\0');
        Throw("read overflow cur:" + std::to_string(_cur) + 
                " readn:" + std::to_string(read_len) +
                " max_len:" + std::to_string(max_len) + 
                " mark:" + (mark == '\n' ? "str" : std::to_string(read_len)));
    }
    _index.push_back(_data + _cur);
    _cur = pos;
    return RET_OK;

}

template <>
bool RowReader::at(unsigned int index, std::string* value) {
    if (index >= _col_num) {
        return false;
    }
    char* start = _index[index];
    char* end = nullptr;
    if (index == _col_num - 1) {
        end = _data + _len;
    } else {
        end = _index[index + 1] - 1;
    }

    // get rid of \0 which is end of string
    if (_col_metas->at(index).type == DT_STRING) {
        end -= 1;
    }
    value->assign(start, end - start);
    return true;
}

RowsReader::RowsReader(std::vector<ColMeta>* col_metas, 
            std::unordered_map<std::string, int>* col_index_map) : 
    RowReader(col_metas, col_index_map) {}

RowsReader::~RowsReader() {}

void RowsReader::push(char* data, uint64_t max_len) {
    _data_vec.push_back(std::make_pair(data, max_len));
}

bool RowsReader::next() {
    if (_cur >= _data_vec.size()) {
        return false;
    }

    const auto& data = _data_vec[_cur++];
    return init(data.first, data.second);
}

} // namespace RiverDB
