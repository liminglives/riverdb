cmake_minimum_required(VERSION 2.8)

PROJECT(riverdb_test)

file(GLOB CPP_SRC_LIST "${CMAKE_CURRENT_SOURCE_DIR}/../src/*.cpp")
#MESSAGE("${CPP_SRC_LIST}")

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/zlib/include)

LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest/lib/.libs)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/zlib/lib)

SET(CMAKE_CXX_FLAGS "-std=c++11 -g -ftest-coverage -fprofile-arcs -Wno-deprecated")

SET(RIVERDB_WRITER_SRC ${CPP_SRC_LIST} riverdb_writer_test.cpp)
ADD_EXECUTABLE(riverdb_writer_test ${RIVERDB_WRITER_SRC})
target_link_libraries(riverdb_writer_test -lpthread -lgtest -lgtest_main -lz)
