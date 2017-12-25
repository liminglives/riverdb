#include "data_container.h"

#include <limits>

#include "mmap_file_reader.h"
#include "gz_file_reader.h"
#include "riverdb_reader.h"

namespace RiverDB {

DataContainer::DataContainer() {}

DataContainer::~DataContainer() {
    close();
}

bool DataContainer::init(const std::vector<std::string>& load_fpath_vec, const std::string& key, const std::string& index_key) {
    _primary_key = key;
    _index_key = index_key;

    _row_reader = new RowReader(&_col_metas, &_col_meta_map);

    for (const auto& fpath : load_fpath_vec) {
        if (!load(fpath)) {
            Throw("load " + fpath + " failed");
        }
    }

    return true;
}

void DataContainer::close() {
    for (auto it : _data_index_map) {
        delete it.second;
    }

    for (auto buf : _buf_vec) {
        free(buf);
    }
}

RowReader* DataContainer::create_row_reader() {
    return new RowReader(&_col_metas, &_col_meta_map);
}

bool DataContainer::get(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    DataIndex* di = get_data_index(kvalue);
    if (di == NULL) {
        Log("has no kvalue:" + kvalue);
        return false;
    }

    char* data = di->get(ts);
    if (data == NULL) {
        Log("kvalue " + kvalue + " has no " + std::to_string(ts));
        return false;
    }
    
    row_reader->init(data, std::numeric_limits<unsigned int>::max());

    return true;
}

bool DataContainer::compare(const std::vector<RowBinaryColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        for (auto& col_meta : col_metas) {
            _col_metas.push_back(col_meta);
        }
        return true;
    } else {
        if (col_metas.size() != _col_metas.size()) {
            return false;
        }

        for (unsigned int i = 0; i < col_metas.size(); ++i) {
            const RowBinaryColMeta meta1 = col_metas[i];
            const RowBinaryColMeta meta2 = _col_metas[i];
            if (meta1._col_name != meta2._col_name || meta1._type != meta2._type) {
                std::cout << "meta not equal" << std::endl;
                return false;
            }
        }
    }

    return true;
}

bool DataContainer::init_meta(const std::vector<RowBinaryColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        _col_metas = col_metas;
        for (int i = 0; i < _col_metas.size(); ++i) {
            _col_meta_map[_col_metas[i]._col_name] = i;
        }
    }
    if (col_metas.size() == 0 || !compare(col_metas)) {
        Throw("col metas are not matched");
    }
    return true;
}

DataIndex* DataContainer::get_data_index(const std::string& kvalue) {
    auto it = _data_index_map.find(kvalue);
    return it == _data_index_map.end() ? NULL : it->second;
}

bool DataContainer::load(const std::string& fpath) {
    RiverDBReader reader(fpath);
    reader.read_header();
    const std::vector<RowBinaryColMeta>& col_metas = reader.get_col_metas();

    unsigned long long data_size  = reader.get_data_size();
    char* buf = (char *)malloc(data_size);
    _buf_vec.push_back(buf);

    //_row_reader = new RowReader(&_col_metas, &_col_meta_map);
    unsigned int start = 0;
    unsigned int remain = data_size;

    while (remain > 0) {
        _row_reader->init(buf + start, remain);
        uint64_t ts = 0;
        if (!_row_reader->get<uint64_t>(_index_key, &ts)) {
            Throw("row_reader failed, fpath:" + fpath);
        }

        std::string kvalue;
        if (!_row_reader->get<std::string>(_primary_key, &kvalue)) {
            Throw("row_reader failed, fpath:" + fpath);
        }
        auto it = _data_index_map.find(kvalue);
        if (it == _data_index_map.end()) {
            _data_index_map[kvalue] = new DataIndex(kvalue, _index_key);
        }
        _data_index_map[kvalue]->append(ts, buf + start);

        unsigned int len = _row_reader->get_len();

        start += len;
        remain -= len;
    }
    std::cout << "load " << fpath << " completed" << std::endl;
    return true;
}

} // namespace RiverDB
