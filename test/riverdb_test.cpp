#include "riverdb.h"

#include "gtest/gtest.h"

class RiverDBWriterTest : public testing::Test {
protected:
    virtual void SetUp() {}

    RiverDB::RiverDBWriter* _writer;
    RiverDB::RiverDBReader* _reader;
};

TEST_F(RiverDBWriterTest, TestReadIllegalFile) {
    int is_catch = 0;
    try {
        _reader = new RiverDB::RiverDBReader("unexist.binary");
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
        is_catch = 1;
    }
    ASSERT_TRUE(is_catch);

    _reader = new RiverDB::RiverDBReader("./testdata/illegal_file_1.binary");
    ASSERT_TRUE(_reader->init() != RiverDB::RET_OK);
    delete _reader;

    _reader = new RiverDB::RiverDBReader("./testdata/illegal_file_2.binary");
    ASSERT_TRUE(_reader->init() != RiverDB::RET_OK);
    delete _reader;

    _reader = new RiverDB::RiverDBReader("./testdata/illegal_file_3.binary");
    ASSERT_TRUE(_reader->init() != RiverDB::RET_OK);
    delete _reader;

    _reader = new RiverDB::RiverDBReader("./testdata/illegal_file_4.binary");
    ASSERT_TRUE(_reader->init() != RiverDB::RET_OK);
    delete _reader;

}

TEST_F(RiverDBWriterTest, TestReadLegalFile) {
    _reader = new RiverDB::RiverDBReader("./testdata/legal_file.binary");
    ASSERT_TRUE(_reader->init() == RiverDB::RET_OK);
    delete _reader;
}

TEST_F(RiverDBWriterTest, TestAppendWriter) {
    std::string file = "./testdata/append_write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::DT_INT32);
    _writer->push_col_meta("TS", RiverDB::DT_UINT64);
    _writer->push_col_meta("A", RiverDB::DT_STRING);
    _writer->push_col_meta("B", RiverDB::DT_INT32);
    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    row.reserve(col_size);
    RiverDB::EmptyValue empty_value;
    for (int i = 0; i < 1; ++i) {
        int32_t k = (11210 + i%3);
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        double c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<double>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;

    _reader = new RiverDB::RiverDBReader(file);
    ASSERT_TRUE(_reader->init() == RiverDB::RET_OK);
    delete _reader;


    _writer = new RiverDB::RiverDBWriter(file, RiverDB::FileOpenModeAppend);
    ASSERT_TRUE(col_size == _writer->get_col_size());
    for (int i = 1; i < 2; ++i) {
        int32_t k = (11210 + i%3);
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        double c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<double>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;

    _reader = new RiverDB::RiverDBReader(file);
    ASSERT_TRUE(_reader->init() == RiverDB::RET_OK);
    delete _reader;

    bool is_catch = false;
    try {
        _writer = new RiverDB::RiverDBWriter("unexist.rdb", RiverDB::FileOpenModeAppend);
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
        is_catch = true;
    }

    ASSERT_TRUE(is_catch);

}

TEST_F(RiverDBWriterTest, TestKVRiverDB) {
    std::string file = "./testdata/write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::DT_INT32);
    _writer->push_col_meta("TS", RiverDB::DT_UINT64);
    _writer->push_col_meta("A", RiverDB::DT_STRING);
    _writer->push_col_meta("B", RiverDB::DT_INT32);
    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    row.reserve(col_size);
    RiverDB::EmptyValue empty_value;
    for (int i = 0; i < 10; ++i) {
        int32_t k = (11210 + i);
        //std::string k = std::to_string(11210 + i%3);
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        double c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        //_writer->push_row<std::string>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<double>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;

    RiverDB::KVRiverDB kvdb("K");
    kvdb.init(file);

    RiverDB::RowReader* row_reader = kvdb.new_row_reader();
    
    std::string kvalue;
    RiverDB::Util::get_str_from_val(11210, kvalue);
    ASSERT_TRUE(kvdb.get(kvalue, row_reader)); 
    ASSERT_TRUE(kvdb.get(11210, row_reader)); 

    int32_t k = 0;
    row_reader->get("K", &k);
    ASSERT_TRUE(k == 11210);
    uint64_t ts = 0;
    row_reader->get("TS", &ts);
    ASSERT_TRUE(ts == 10);

    row.clear();
    RiverDB::Util::push_row<int32_t>(11210, row);
    RiverDB::Util::push_row<uint64_t>(20, row);
    RiverDB::Util::push_row<std::string>("11210a", row);
    RiverDB::Util::push_row<int32_t>(222, row);
    RiverDB::Util::push_row<double>(1.8988, row);

    kvdb.set(row);
    kvdb.get(kvalue, row_reader);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11210);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 20);

    delete row_reader;

}



TEST_F(RiverDBWriterTest, TestWriter) {
    std::string file = "./testdata/write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::DT_INT32);
    _writer->push_col_meta("TS", RiverDB::DT_UINT64);
    _writer->push_col_meta("A", RiverDB::DT_STRING);
    _writer->push_col_meta("B", RiverDB::DT_INT32);
    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    row.reserve(col_size);
    RiverDB::EmptyValue empty_value;
    for (int i = 0; i < 10; ++i) {
        int32_t k = (11210 + i%3);
        //std::string k = std::to_string(11210 + i%3);
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        double c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        //_writer->push_row<std::string>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<double>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;

    _reader = new RiverDB::RiverDBReader(file);
    ASSERT_TRUE(_reader->init() == RiverDB::RET_OK);
    delete _reader;

}

TEST_F(RiverDBWriterTest, TestRiverDBWithIntTypeKey) {
    try {
    std::string file = "./testdata/write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::DT_INT32);
    _writer->push_col_meta("TS", RiverDB::DT_UINT64);
    _writer->push_col_meta("A", RiverDB::DT_STRING);
    _writer->push_col_meta("B", RiverDB::DT_INT32);
    _writer->push_col_meta("C", RiverDB::DT_DOUBLE);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    row.reserve(col_size);
    RiverDB::EmptyValue empty_value;
    for (int i = 0; i < 10; ++i) {
        int32_t k = (11210 + i%3);
        //std::string k = std::to_string(11210 + i%3);
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        double c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        //_writer->push_row<std::string>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<double>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;


    RiverDB::TimeRiverDB* db = new RiverDB::TimeRiverDB("K", "TS");
    std::vector<std::string> file_vec;
    file_vec.push_back(file);
    db->init(file_vec);
    //db->load(file);

    RiverDB::RowReader* row_reader = db->new_row_reader();
    std::string kvalue; 
    RiverDB::Util::get_str_from_val(11210, kvalue);

    // at
    //db->at(kvalue, 1, row_reader);
    db->index(kvalue, 1, row_reader);
    int32_t k = 0;
    row_reader->get("K", &k);
    ASSERT_TRUE(k == 11210);
    uint64_t ts = 0;
    row_reader->get("TS", &ts);
    ASSERT_TRUE(ts == 13);
    std::string a;
    row_reader->get("A", &a);
    ASSERT_TRUE(a == "col_a11");
    double c = 0.0;
    row_reader->get("C", &c);
    ASSERT_TRUE(c == 1.23131211);

    // eq 
    ASSERT_TRUE(db->eq(11210, 11, row_reader) == false);
    ASSERT_TRUE(db->eq(11210, 13, row_reader) == true);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11210);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 13);

    // gt
    ASSERT_TRUE(db->gt(11210, 19, row_reader) == false);
    ASSERT_TRUE(db->gt(11210, 13, row_reader) == true);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11210);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 16);

    // ge
    ASSERT_TRUE(db->ge(11212, 19, row_reader) == false);
    ASSERT_TRUE(db->ge(11212, 0, row_reader) == true);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11212);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 12);

    // lt
    ASSERT_TRUE(db->lt(11212, 12, row_reader) == false);
    ASSERT_TRUE(db->lt(11212, 20, row_reader) == true);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11212);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 18);

    // le
    ASSERT_TRUE(db->le(11212, 11, row_reader) == false);
    ASSERT_TRUE(db->le(11212, 12, row_reader) == true);
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11212);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 12);

    delete row_reader;
    delete db;
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
    }
}

TEST_F(RiverDBWriterTest, TestRiverDBWithStringTypeKey) {
    try {
    RiverDB::TimeRiverDB* db = new RiverDB::TimeRiverDB("K", "TS");
    db->init({"./testdata/riverdb_test.rdb"});
    //db->load("./testdata/riverdb_test.rdb");
    RiverDB::RowReader* row_reader = db->new_row_reader();
    std::string kvalue = "11210";
    //RiverDB::Util::get_str_from_val(11210, kvalue);
    db->at(kvalue, 1, row_reader);
    
    //int32_t k = 0;
    std::string k;
    //row_reader->get<int32_t>("K", &k);
    row_reader->get<std::string>("K", &k);
    ASSERT_TRUE(k == "11210");

    uint64_t ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 13);

    std::string a;
    row_reader->get<std::string>("A", &a);
    ASSERT_TRUE(a == "col_a11");

    double c = 0.0;
    row_reader->get<double>("C", &c);
    ASSERT_TRUE(c == 1.23131211);

    delete row_reader;
    delete db;
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
    }
}

TEST_F(RiverDBWriterTest, TestRiverDBWriteAndIndex) {
    try {
    std::string file = "./testdata/riverdb_test2.rdb";
    RiverDB::TimeRiverDB* db = new RiverDB::TimeRiverDB("K", "TS");
    db->init({file});
    //db->load(file);

    RiverDB::RowReader* row_reader = db->new_row_reader();

    std::string kvalue; 
    RiverDB::Util::get_str_from_val(11210, kvalue);
    db->at(kvalue, 1, row_reader);
    
    int32_t k = 0;
    row_reader->get<RiverDB::INT32>("K", &k);
    ASSERT_TRUE(k == 11210);
    uint64_t ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 13);
    std::string a;
    row_reader->get<std::string>("A", &a);
    ASSERT_TRUE(a == "col_a11");
    double c = 0.0;
    row_reader->get<double>("C", &c);
    ASSERT_TRUE(c == 1.23131211);

    std::vector<std::string> row;
    int col_size = db->get_col_size();
    row.reserve(col_size);
    int32_t kk = (11210);
    uint64_t tts = 100;
    std::string aa = "new_append";
    int32_t bb = 2321;
    double cc = 1.23;
    RiverDB::Util::push_row<int32_t>(kk, row);
    RiverDB::Util::push_row<uint64_t>(tts, row);
    RiverDB::Util::push_row<std::string>(aa, row);
    RiverDB::Util::push_row<int32_t>(bb, row);
    RiverDB::Util::push_row<double>(cc, row);
    db->append(row);

    db->at(kvalue, -1, row_reader);
 
    k = 0;
    row_reader->get<int32_t>("K", &k);
    ASSERT_TRUE(k == 11210);
    ts = 0;
    row_reader->get<uint64_t>("TS", &ts);
    ASSERT_TRUE(ts == 100);
    a = "";
    row_reader->get<std::string>("A", &a);
    ASSERT_TRUE(a == "new_append");
    int32_t b = 0;
    row_reader->get<int32_t>("B", &b);
    ASSERT_TRUE(b == 2321);
    c = 0.0;
    row_reader->get<double>("C", &c);
    ASSERT_TRUE(c == 1.23);

    delete row_reader;
    delete db;
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
    }
}
TEST_F(RiverDBWriterTest, TestTimeRiverDBLoad) {
    std::string data_file_path = "./testdata/market_data.rdb.3";
    //std::string data_file_path = "./market_data.rdb";
    std::vector<RiverDB::ColMeta> col_metas = {
        {"RTId", RiverDB::DT_INT32},
        {"FType", RiverDB::DT_INT16},
        {"EID", RiverDB::DT_STRING},
        {"UID", RiverDB::DT_STRING},
        {"EX", RiverDB::DT_INT16},
        {"TimeStamp", RiverDB::DT_UINT64},
        {"AskPrice1", RiverDB::DT_DOUBLE},
        {"AskPrice2", RiverDB::DT_DOUBLE},
        {"AskPrice3", RiverDB::DT_DOUBLE},
        {"AskPrice4", RiverDB::DT_DOUBLE},
        {"AskPrice5", RiverDB::DT_DOUBLE},
        {"AskSize1", RiverDB::DT_UINT32},
        {"AskSize2", RiverDB::DT_UINT32},
        {"AskSize3", RiverDB::DT_UINT32},
        {"AskSize4", RiverDB::DT_UINT32},
        {"AskSize5", RiverDB::DT_UINT32},
        {"BidPrice1", RiverDB::DT_DOUBLE},
        {"BidPrice2", RiverDB::DT_DOUBLE},
        {"BidPrice3", RiverDB::DT_DOUBLE},
        {"BidPrice4", RiverDB::DT_DOUBLE},
        {"BidPrice5", RiverDB::DT_DOUBLE},
        {"BidSize1", RiverDB::DT_UINT32},
        {"BidSize2", RiverDB::DT_UINT32},
        {"BidSize3", RiverDB::DT_UINT32},
        {"BidSize4", RiverDB::DT_UINT32},
        {"BidSize5", RiverDB::DT_UINT32},
    };
    auto time_riverdb_ = new RiverDB::TimeRiverDB("RTId", "TimeStamp");
    try {

        std::vector<std::string> load_file_vec;
        if (RiverDB::Util::file_exists(data_file_path)) {
            load_file_vec.push_back(data_file_path);
        }
        ASSERT_TRUE(time_riverdb_->init(load_file_vec, data_file_path, col_metas)); 


        /*
        for (int i = 0; i < 100; ++i) {
            std::vector<std::string> row;
            RiverDB::Util::push_row<RiverDB::INT32>(i % 7, row);
            RiverDB::Util::push_row<RiverDB::INT16>(0, row);
            RiverDB::Util::push_row<RiverDB::STRING>("eid", row);
            RiverDB::Util::push_row<RiverDB::STRING>("uid", row);
            RiverDB::Util::push_row<RiverDB::INT16>(static_cast<RiverDB::INT16>(8), row);
            RiverDB::Util::push_row<RiverDB::UINT64>(i, row);

            RiverDB::Util::push_row<RiverDB::FLOAT>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(2.0 + 0.01*i, row);

            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);

            RiverDB::Util::push_row<RiverDB::FLOAT>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::FLOAT>(1.0+0.01*i, row);

            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            time_riverdb_->append(row);
        } */

        auto row_reader = time_riverdb_->new_row_reader();
        std::string k = "";
        RiverDB::Util::get_str_from_val(3, k);
        if (!time_riverdb_->at(k, -1, row_reader)) {
            std::cout << "has no key :" << 3 << std::endl;
        } else {
            RiverDB::UINT64 ts = 0;
            row_reader->get("TimeStamp", &ts);
            std::cout << "TimeStamp:" << ts << std::endl;
        }


    } catch (RiverDB::RTTException e) {
        std::cout << e.info() << std::endl; 
    }


}

TEST_F(RiverDBWriterTest, TestTimeRiverDBWrite) {
    std::string data_file_path = "./testdata/market_data.rdb.3";
    std::vector<RiverDB::ColMeta> col_metas = {
        {"RTId", RiverDB::DT_INT32},
        {"FType", RiverDB::DT_INT16},
        {"EID", RiverDB::DT_STRING},
        {"UID", RiverDB::DT_STRING},
        {"EX", RiverDB::DT_INT16},
        {"TimeStamp", RiverDB::DT_UINT64},
        {"AskPrice1", RiverDB::DT_DOUBLE},
        {"AskPrice2", RiverDB::DT_DOUBLE},
        {"AskPrice3", RiverDB::DT_DOUBLE},
        {"AskPrice4", RiverDB::DT_DOUBLE},
        {"AskPrice5", RiverDB::DT_DOUBLE},
        {"AskSize1", RiverDB::DT_UINT32},
        {"AskSize2", RiverDB::DT_UINT32},
        {"AskSize3", RiverDB::DT_UINT32},
        {"AskSize4", RiverDB::DT_UINT32},
        {"AskSize5", RiverDB::DT_UINT32},
        {"BidPrice1", RiverDB::DT_DOUBLE},
        {"BidPrice2", RiverDB::DT_DOUBLE},
        {"BidPrice3", RiverDB::DT_DOUBLE},
        {"BidPrice4", RiverDB::DT_DOUBLE},
        {"BidPrice5", RiverDB::DT_DOUBLE},
        {"BidSize1", RiverDB::DT_UINT32},
        {"BidSize2", RiverDB::DT_UINT32},
        {"BidSize3", RiverDB::DT_UINT32},
        {"BidSize4", RiverDB::DT_UINT32},
        {"BidSize5", RiverDB::DT_UINT32},
    };
    auto time_riverdb_ = new RiverDB::TimeRiverDB("RTId", "TimeStamp");
    try {

        std::vector<std::string> load_file_vec;
        if (RiverDB::Util::file_exists(data_file_path)) {
            load_file_vec.push_back(data_file_path);
        }
        ASSERT_TRUE(time_riverdb_->init(load_file_vec, data_file_path, col_metas)); 


        for (int i = 0; i < 10; ++i) {
            std::vector<std::string> row;
            RiverDB::Util::push_row<RiverDB::INT32>(i % 7, row);
            RiverDB::Util::push_row<RiverDB::INT16>(0, row);
            RiverDB::Util::push_row<RiverDB::STRING>("eid", row);
            RiverDB::Util::push_row<RiverDB::STRING>("uid", row);
            RiverDB::Util::push_row<RiverDB::INT16>(static_cast<RiverDB::INT16>(8), row);
            RiverDB::Util::push_row<RiverDB::UINT64>(i, row);

            RiverDB::Util::push_row<RiverDB::DOUBLE>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(2.0 + 0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(2.0 + 0.01*i, row);

            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);

            RiverDB::Util::push_row<RiverDB::DOUBLE>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(1.0+0.01*i, row);
            RiverDB::Util::push_row<RiverDB::DOUBLE>(1.0+0.01*i, row);

            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            RiverDB::Util::push_row<RiverDB::UINT32>(10, row);
            time_riverdb_->append(row);
        } 

        auto row_reader = time_riverdb_->new_row_reader();
        ASSERT_TRUE(time_riverdb_->index(3, -1, row_reader)); 
        RiverDB::UINT64 ts = 0;
        row_reader->get("TimeStamp", &ts);
        ASSERT_TRUE(ts == 3);

    } catch (RiverDB::RTTException e) {
        std::cout << e.info() << std::endl; 
        ASSERT_TRUE(false);
    }


}



int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
