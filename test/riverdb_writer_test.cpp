#include "riverdb_writer.h"
#include "riverdb_reader.h"
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

TEST_F(RiverDBWriterTest, TestWriter) {
    std::string file = "./testdata/write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::Type_INT32);
    _writer->push_col_meta("TS", RiverDB::Type_UINT64);
    _writer->push_col_meta("A", RiverDB::Type_STRING);
    _writer->push_col_meta("B", RiverDB::Type_INT32);
    _writer->push_col_meta("C", RiverDB::Type_DOUBLE);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    std::string binary_val;
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
    RiverDB::RiverDB* db = new RiverDB::RiverDB();
    db->init("K", "TS");
    db->load("./testdata/riverdb_test2.rdb");
    RiverDB::RowReader* row_reader = db->new_row_reader();
    std::string kvalue; 
    RiverDB::Util::get_str_from_val(11210, kvalue);
    db->at(kvalue, 1, row_reader);
    
    int32_t k = 0;
    row_reader->get<int32_t>("K", &k);
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

    delete row_reader;
    delete db;
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
    }
}

TEST_F(RiverDBWriterTest, TestRiverDBWithStringTypeKey) {
    try {
    std::cout << "llllll"<< std::endl;
    RiverDB::RiverDB* db = new RiverDB::RiverDB();
    db->init("K", "TS");
    db->load("./testdata/riverdb_test.rdb");
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

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
