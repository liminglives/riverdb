#include "riverbed.h"

#include "gtest/gtest.h"

class RiverbedTest : public testing::Test {
protected:
    virtual void SetUp() {}
};


TEST_F(RiverbedTest, TestReadIllegalFile) {
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
