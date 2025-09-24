cmake_minimum_required(VERSION 3.21)
project(FOSKV VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(OPTIONS
        ENABLE_KOSIO_LOG
        ENABLE_HUMAN_READABLE_RPC_NAMES
        ENABLE_HUMAN_READABLE_JSON
)

set(ENABLE_KOSIO_LOG ON)
set(ENABLE_HUMAN_READABLE_RPC_NAMES OFF)
set(ENABLE_HUMAN_READABLE_JSON ON)

foreach(opt IN LISTS OPTIONS)
    option(${opt} "Enable ${opt}" ${${opt}})
endforeach()

foreach(opt IN LISTS OPTIONS)
    if(${opt})
        add_compile_definitions(${opt})
    endif()
endforeach()

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

find_package(PkgConfig REQUIRED)
pkg_check_modules(XXHASH REQUIRED libxxhash)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(include/foskv/rpc/pb)

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
        tbb
        ${PROTOBUF_LIBRARIES}
        ${LIBURING_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        ${XXHASH_LIBRARIES}
)

add_subdirectory(src/foskv)

# ====== TEST ======
add_subdirectory(tests)