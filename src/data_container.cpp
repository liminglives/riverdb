#include "data_container.h"

#include "mmap_file_reader.h"
#include "gz_file_reader.h"

namespace RiverDB {

DataContainer::DataContainer() {}

DataContainer::~DataContainer() {
    close();
}

bool DataContainer::init(const std::vector<std::string>& load_fpath_vec, const std::string& key) {
    return true;
}

void DataContainer::close() {
    for (auto it : _data_index_map) {
        delete it->second;
    }

    for (auto buf : _buf_vec) {
        delete buf;
    }
}

bool DataContainer::compare(const std::vector<RowBinaryColMeta>& col_metas) {
    if (_col_metas.size() == 0) {
        for (auto& col_meta : col_metas) {
            _col_meatas.push_back(col_meta);
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

bool DataContainer::load(const std::string& fpath) {
    RiverDBReader* reader = new RiverDBReader(fpath);
    reader->read_header();
    const std::vector<RowBinaryColMeta>& col_metas = reader->get_col_metas();
    if (!compare(col_metas)) {
        Throw("col metas are not matched");
    }

    unsigned long long data_size  = reader.get_data_size();
    char* buf = (char *)malloc(data_size);
    _buf_vec.push_back(buf);

}

} // namespace RiverDB
