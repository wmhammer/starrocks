cmake_minimum_required(VERSION 3.15)
project(StarRocks)

set(CMAKE_CXX_STANDARD 17)

include_directories(
        be/
        be/src
        be/test

        // thrift/protobuf files
        gensrc/build
        gensrc/build/common
        gensrc/build/gen_cpp

        // header files of third-parties
        deps/include
)

add_executable(StarRocks
        be/src/service/starrocks_main.cpp)