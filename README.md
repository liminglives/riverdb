# RiverDB## Simple time series and kv database based on csv like format file  ## Example 1    // time series    std::string file = "./testdata/write.rdb";    _writer = new RiverDB::RiverDBWriter(file);    _writer->push_col_meta("K", RiverDB::DT_INT32);    _writer->push_col_meta("TS", RiverDB::DT_UINT64);    _writer->push_col_meta("A", RiverDB::DT_STRING);    _writer->push_col_meta("B", RiverDB::DT_INT32);    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);    _writer->write_header();    std::vector<std::string> row;    int col_size = _writer->get_col_size();    row.reserve(col_size);    RiverDB::EmptyValue empty_value;    for (int i = 0; i < 10; ++i) {        int32_t k = (11210 + i%3);        //std::string k = std::to_string(11210 + i%3);        uint64_t ts = 10 + i;        std::string a = "col_a11";        int32_t b = 2321;        double c = 1.23131211;        row.clear();        _writer->push_row<int32_t>(k, row);        _writer->push_row<uint64_t>(ts, row);        _writer->push_row<std::string>(a, row);        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);        _writer->push_row<double>(c, row);        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK);     }    delete _writer;    // specify primary_key("K") and index_key("TS")    RiverDB::TimeRiverDB* db = new RiverDB::TimeRiverDB("K", "TS");    std::vector<std::string> file_vec;    file_vec.push_back(file);    db->init(file_vec);    RiverDB::RowReader* row_reader = db->new_row_reader();    std::string kvalue;     RiverDB::Util::get_str_from_val(11210, kvalue);    //db->at(kvalue, 1, row_reader);    db->index(kvalue, 1, row_reader);    int32_t k = 0;    row_reader->get("K", &k);    ASSERT_TRUE(k == 11210);    uint64_t ts = 0;    row_reader->get("TS", &ts);    ASSERT_TRUE(ts == 13);    std::string a;    row_reader->get("A", &a);    ASSERT_TRUE(a == "col_a11");    double c = 0.0;    row_reader->get("C", &c);    ASSERT_TRUE(c == 1.23131211);    // eq     ASSERT_TRUE(db->eq(11210, 11, row_reader) == false);    ASSERT_TRUE(db->eq(11210, 13, row_reader) == true);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11210);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 13);    // gt    ASSERT_TRUE(db->gt(11210, 19, row_reader) == false);    ASSERT_TRUE(db->gt(11210, 13, row_reader) == true);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11210);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 16);    // ge    ASSERT_TRUE(db->ge(11212, 19, row_reader) == false);    ASSERT_TRUE(db->ge(11212, 0, row_reader) == true);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11212);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 12);    // lt    ASSERT_TRUE(db->lt(11212, 12, row_reader) == false);    ASSERT_TRUE(db->lt(11212, 20, row_reader) == true);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11212);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 18);    // le    ASSERT_TRUE(db->le(11212, 11, row_reader) == false);    ASSERT_TRUE(db->le(11212, 12, row_reader) == true);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11212);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 12);    delete row_reader;    delete db;## Example 2    // kv    std::string file = "./testdata/write.rdb";    _writer = new RiverDB::RiverDBWriter(file);    _writer->push_col_meta("K", RiverDB::DT_INT32);    _writer->push_col_meta("TS", RiverDB::DT_UINT64);    _writer->push_col_meta("A", RiverDB::DT_STRING);    _writer->push_col_meta("B", RiverDB::DT_INT32);    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);    _writer->write_header();    std::vector<std::string> row;    int col_size = _writer->get_col_size();    row.reserve(col_size);    RiverDB::EmptyValue empty_value;    for (int i = 0; i < 10; ++i) {        int32_t k = (11210 + i);        //std::string k = std::to_string(11210 + i%3);        uint64_t ts = 10 + i;        std::string a = "col_a11";        int32_t b = 2321;        double c = 1.23131211;        row.clear();        _writer->push_row<int32_t>(k, row);        //_writer->push_row<std::string>(k, row);        _writer->push_row<uint64_t>(ts, row);        _writer->push_row<std::string>(a, row);        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);        _writer->push_row<double>(c, row);        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK);     }    delete _writer;    // specify primary_key("K")    RiverDB::KVRiverDB kvdb("K");    kvdb.init(file);    RiverDB::RowReader* row_reader = kvdb.new_row_reader();        ASSERT_TRUE(kvdb.get(11210, row_reader));     int32_t k = 0;    row_reader->get("K", &k);    ASSERT_TRUE(k == 11210);    uint64_t ts = 0;    row_reader->get("TS", &ts);    ASSERT_TRUE(ts == 10);    row.clear();    RiverDB::Util::push_row<int32_t>(11210, row);    RiverDB::Util::push_row<uint64_t>(20, row);    RiverDB::Util::push_row<std::string>("11210a", row);    RiverDB::Util::push_row<int32_t>(222, row);    RiverDB::Util::push_row<double>(1.8988, row);    kvdb.set(row);    kvdb.get(11210, row_reader);    k = 0;    row_reader->get<int32_t>("K", &k);    ASSERT_TRUE(k == 11210);    ts = 0;    row_reader->get<uint64_t>("TS", &ts);    ASSERT_TRUE(ts == 20);    delete row_reader;