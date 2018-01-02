#include "kv_riverdb.h"

#include <limits>

#include "riverdb_reader.h"

namespace RiverDB {

KVRiverDB::KVRiverDB(const std::string& primary_key) : _primary_key(primary_key) {}

KVRiverDB::~KVRiverDB() {
    close();
}

bool KVRiverDB::init(const std::string& fpath) {
    _fpath = fpath;
    if (!load(_fpath)) {
        Log("load failed, file path:" + fpath);
        return false;
    }
    _writer = new RiverDBWriter(fpath, FileOpenModeAppend);
}

void KVRiverDB::close() {
    for (auto buf : _buf_vec) {
        free(buf);
    }
    if (_writer) {
        delete _writer;
        _writer = NULL;
    }
}

bool KVRiverDB::init_meta(const std::vector<RowBinaryColMeta>& col_metas) {
    if (col_metas.size() ==0) {
        return false;
    }
    _col_metas = col_metas;
    for (int i = 0; i < _col_metas.size(); ++i) {
        _col_name_index_map[_col_metas[i]._col_name] = i;
    }
    return true;
}

bool KVRiverDB::load(const std::string& fpath) {
    RiverDBReader reader(fpath);
    reader.read_header();
    const std::vector<RowBinaryColMeta>& col_metas = reader.get_col_metas();
    if (!init_meta(col_metas)) {
        Throw("illegal col_meta, file path:" + fpath);
    }

    unsigned long long data_size  = reader.get_data_size();
    char* buf = (char *)malloc(data_size);
    _buf_vec.push_back(buf);
    reader.load_data(buf, data_size);

    RowReader row_reader(&_col_metas, &_col_name_index_map);
    unsigned int start = 0;
    unsigned int remain = data_size;

    while (remain > 0) {
        row_reader.init(buf + start, remain);

        std::string kvalue;
        if (!row_reader.get<std::string>(_primary_key, &kvalue)) {
            Throw("row_reader failed, fpath:" + fpath);
        }

        _index_map[kvalue] = buf + start;

        unsigned int len = row_reader.get_len();

        start += len;
        remain -= len;
    }
    Log("load " + fpath + " completed");
    return true;
}

RowReader* KVRiverDB::new_row_reader() {
    return new RowReader(&_col_metas, &_col_name_index_map);
}

bool KVRiverDB::get(const std::string& kvalue, RowReader* row_reader) {
    auto it = _index_map.find(kvalue);
    if (it == _index_map.end()) {
        return false;
    }
    row_reader->init(it->second, std::numeric_limits<unsigned int>::max());
    return true;
}

bool KVRiverDB::set(const std::vector<std::string>& row) {
    char* data = NULL;
    unsigned int len;
    if (!Util::serialize_row(_col_metas, row, data, len)) {
        Log("serialize_row failed");
        return false;
    }
    _buf_vec.push_back(data);

    _index_map[row[_col_name_index_map[_primary_key]]] = data; 

    _writer->write_row(row);
    _writer->flush();

    return true;
}

} // namespace RiverDB
