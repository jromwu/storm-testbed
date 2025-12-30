cmake_minimum_required(VERSION 3.16)
project(rdma_prio_ring LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBIBVERBS REQUIRED libibverbs)

add_executable(rdma_ring src/main.cpp)
add_executable(hello_world src/hello_world.cpp)
add_executable(rdma_hello src/rdma_hello.cpp)

target_include_directories(rdma_ring PRIVATE ${LIBIBVERBS_INCLUDE_DIRS})
target_link_libraries(rdma_ring PRIVATE ${LIBIBVERBS_LIBRARIES} pthread)
target_compile_options(rdma_ring PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(rdma_ring PRIVATE _POSIX_C_SOURCE=200809L)

target_link_libraries(hello_world PRIVATE pthread)
target_compile_options(hello_world PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(hello_world PRIVATE _POSIX_C_SOURCE=200809L)

target_include_directories(rdma_hello PRIVATE ${LIBIBVERBS_INCLUDE_DIRS})
target_link_libraries(rdma_hello PRIVATE ${LIBIBVERBS_LIBRARIES} pthread)
target_compile_options(rdma_hello PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(rdma_hello PRIVATE _POSIX_C_SOURCE=200809L)
