cmake_minimum_required(VERSION 3.22)
project(ex3)

set(CMAKE_CXX_STANDARD 11)

add_executable(ex3 MapReduceFramework.cpp ThreadAction.cpp Barrier.cpp SampleClient.cpp)

