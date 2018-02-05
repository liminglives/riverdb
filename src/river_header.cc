#include "river_header.h"

namespace RiverDB {

RiverHeader::RiverHeader() {}

RiverHeader::~RiverHeader() {}

void build_index(const std::vector<ColMeta>& col_metas) {
    for (size_t i = 0; i < col_metas.size(); ++i) {
        _col_name_index_map[col_metas[i].name] = i;
    }
}

bool RiverHeader::init_header(const std::vector<ColMeta>& col_metas) {
    _col_metas = col_metas;
    build_index(_col_metas);
    std::string col_name_line;
    std::string col_type_line;
    for (const auto& col : _col_metas) {
        col_name_line.append(col.name).append(",");
        col_type_line.append(col.type).append(",");
    }
    col_name_line.pop_back();
    col_type_line.pop_back();

    std::string version = RiverDBHeaderMark + "," + RiverDBHeaderV10;
    _header_lines.append(version);
    _header_lines.append(col_name_line);
    _header_lines.append(col_type_line);
    return true;
}

bool parse_header(const std::vector<std::string>& header_lines) {
    if (header_lines.size() != 3) {
        Log("header_lines number should be 3, now is " + 
                std::to_string(header_lines.size()));
        return false;
    }
    if (RiverDBMark != header_lines[0].substr(0, RiverDBHeaderMark.size())) {
        Log("error format: " + header_lines[0]);
        return false;
    }
    _header_lines = header_lines;

    std::vector<std::string> name_arr;
    Util::split(_header_lines[1], RiverDBHeaderSplit, name_arr);

    std::vector<std::string> type_arr;
    Util::split(_header_lines[2], RiverDBHeaderSplit, type_arr);

    if (name_arr.size() != type_arr.size()) {
        Log("col name len is not equal to col type len");
        return false;
    }

    for (size_t i = 0; i < name_arr.size(); ++i) {
        ColMeta col = {name_arr[i], type_arr[i]};
        _col_metas.push_back(col);
    }

    build_index(_col_metas);

    return true;
}

} // namespace RiverDB
