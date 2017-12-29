#include "riverdb.h"

#include "util.h"

namespace RiverDB {

RiverDB::RiverDB() {}

RiverDB::~RiverDB() {
    delete _data_container;
}

bool RiverDB::init(const std::string& primary_key,
            const std::string& index_key) {
    _data_container = new DataContainer(primary_key, index_key);
    if (!_data_container->init()) {
        Throw("data container init failed");
    }
    return true;
}

bool RiverDB::load(const std::string& fpath) {
    return _data_container->load(fpath);
}

RowReader* RiverDB::new_row_reader() {
    return _data_container->new_row_reader();
}

bool RiverDB::get(const std::string& kvalue, uint64_t ts, RowReader* row_reader) {
    return _data_container->get(kvalue, ts, row_reader);
}

bool RiverDB::at(const std::string& kvalue, int index, RowReader* row_reader) {
    return _data_container->at(kvalue, index, row_reader);
}

} // namespace RiverDB
