cmake_minimum_required(VERSION 3.21)
project(FOSKV VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(${CMAKE_INSTALL_PREFIX} /usr/local)

# add_compile_definitions(NEED_KOSIO_LOG)

message(STATUS "Version: " "${CMAKE_PROJECT_VERSION}")
message(STATUS "C++ standard: " "${CMAKE_CXX_STANDARD}")
message(STATUS "Compiler: " "${CMAKE_CXX_COMPILER_ID}")
message(STATUS "Build type: " "${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_INSTALL_PREFIX: " "${CMAKE_INSTALL_PREFIX}")

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

# ====== LIBRARY ======
set(DEPEND_LIBS
        pthread
        ${LIBURING_LIBRARIES}
)

# ====== TEST ======
add_subdirectory(tests)