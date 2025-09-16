cmake_minimum_required(VERSION 3.21)
project(FOSKV VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_definitions(NEED_KOSIO_LOG)

message(STATUS "Version: " "${CMAKE_PROJECT_VERSION}")
message(STATUS "C++ standard: " "${CMAKE_CXX_STANDARD}")
message(STATUS "Compiler: " "${CMAKE_CXX_COMPILER_ID}")
message(STATUS "Build type: " "${CMAKE_BUILD_TYPE}")

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

find_package(Protobuf REQUIRED)
if(Protobuf_FOUND)
    message(STATUS "Protobuf version: ${Protobuf_VERSION}")
else()
    message(FATAL_ERROR "Protobuf not found")
endif()

find_package(RocksDB REQUIRED)
if (RocksDB_FOUND)
    message(STATUS "RocksDB version: ${RocksDB_VERSION}")
endif ()

find_package(TBB REQUIRED)
if (TBB_FOUND)
    message(STATUS "TBB version: ${TBB_VERSION}")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# ====== LIBRARY ======
set(ROCKSDB_LIBRARIES
        rocksdb
        snappy
        z
        bz2
        lz4
        zstd
)

set(DEPEND_LIBS
        pthread
        ${PROTOBUF_LIBRARIES}
        ${LIBURING_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        tbb
)

add_subdirectory(src/rpc)
add_subdirectory(src/storage)

set(SRC_FILES
        ${RAFT_FILES}
        ${RPC_FILES}
        ${STORAGE_FILES}
)

# ====== TEST ======
add_subdirectory(tests)