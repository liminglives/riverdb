cmake_minimum_required(VERSION 2.8)

PROJECT(rtt_binary)
SET(SRC src/gz_file_reader.cpp
    src/mmap_file_reader.cpp
    src/riverdb_reader.cpp
    src/riverdb_writer.cpp
    src/time_riverdb.cpp
    src/data_index.cpp
    src/row_reader.cpp
    src/kv_riverdb.cpp
    src/util.cpp)

SET(PYSRC
    ${SRC}
    src/py/riverdb.cpp
    )

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/src)
INCLUDE_DIRECTORIES(/usr/include/python2.7/)
INCLUDE_DIRECTORIES(/usr/lib/python2.7/config-x86_64-linux-gnu/)


SET(CMAKE_CXX_FLAGS "-std=c++11 -g")

ADD_LIBRARY(riverdb STATIC ${SRC})
SET_TARGET_PROPERTIES(riverdb PROPERTIES OUTPUT_NAME "riverdb")
target_link_libraries(riverdb -lz)

ADD_LIBRARY(riverdb_so SHARED ${PYSRC})
SET_TARGET_PROPERTIES(riverdb_so PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(riverdb_so PROPERTIES OUTPUT_NAME "riverdb_py")
target_link_libraries(riverdb_so -lz)
