#include "riverdb_writer.h"

#include "gtest/gtest.h"

class RiverDBWriterTest : public testing::Test {
protected:
    virtual void SetUp() {}

    RiverDB::RiverDBWriter* _writer;
};

TEST_F(RiverDBWriterTest, TestWriter) {
    std::string file = "./testdata/write.rdb";
    _writer = new RiverDB::RiverDBWriter(file);

    _writer->push_col_meta("K", RiverDB::Type_INT32);
    _writer->push_col_meta("TS", RiverDB::Type_UINT64);
    _writer->push_col_meta("A", RiverDB::Type_STRING);
    _writer->push_col_meta("B", RiverDB::Type_INT32);
    _writer->push_col_meta("C", RiverDB::Type_FLOAT);
    _writer->write_header();

    std::vector<std::string> row;
    int col_size = _writer->get_col_size();
    std::string binary_val;
    row.reserve(col_size);
    RiverDB::EmptyValue empty_value;
    for (int i = 0; i < 10; ++i) {
        int32_t k = 11210 + i;
        uint64_t ts = 10 + i;
        std::string a = "col_a11";
        int32_t b = 2321;
        float c = 1.23131211;

        row.clear();

        _writer->push_row<int32_t>(k, row);
        _writer->push_row<uint64_t>(ts, row);
        _writer->push_row<std::string>(a, row);
        _writer->push_row<RiverDB::EmptyValue>(empty_value, row);
        _writer->push_row<float>(c, row);

        ASSERT_TRUE(_writer->write_row(row) == RiverDB::RET_OK); 
    }
    delete _writer;

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
