cmake_minimum_required(VERSION 3.20 FATAL_ERROR)
project(Empy VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 99)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build configuration" FORCE)
endif()

# cmake output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")

option(EMPY_BUILD_TESTS "Build protocol and integration tests" ON)
option(EMPY_ENABLE_QUIC "Build the optional MsQuic transport" ON)

set(EMPY_MSQUIC_RUNTIME "$ENV{EMPY_MSQUIC_RUNTIME}" CACHE FILEPATH "Optional MsQuic DLL, matching TLS provider")
set(EMPY_ASIO_INCLUDE_DIR "" CACHE PATH "Standalone Asio include directory")
set(EMPY_MSQUIC_ROOT "$ENV{EMPY_MSQUIC_ROOT}" CACHE PATH "MsQuic installation directory")
include(cmake/Runtime.cmake)

function(configure_project_target target)
    set_target_properties(${target} PROPERTIES PREFIX "" IMPORT_PREFIX "" OUTPUT_NAME "${target}")
    target_compile_options(${target} PRIVATE
        $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/W4;/permissive-;/Zc:__cplusplus>
        $<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang>:-Wall;-Wextra;-Wpedantic>)
    target_compile_definitions(${target} PUBLIC $<$<CONFIG:Debug>:EMPY_LOGGING>)
    if (WIN32)
        target_compile_definitions(${target} PUBLIC _WIN32_WINNT=0x0A00 NOMINMAX WIN32_LEAN_AND_MEAN)
    endif()
endfunction()

find_package(Threads REQUIRED)
find_package(asio CONFIG QUIET)
if (NOT TARGET asio::asio)
    find_path(EMPY_ASIO_PATH asio.hpp HINTS "${EMPY_ASIO_INCLUDE_DIR}" REQUIRED)
    add_library(EmpyAsio INTERFACE)
    target_include_directories(EmpyAsio SYSTEM INTERFACE "${EMPY_ASIO_PATH}")
    target_compile_definitions(EmpyAsio INTERFACE ASIO_STANDALONE)
    add_library(asio::asio ALIAS EmpyAsio)
endif()
find_package(OpenSSL 3 REQUIRED COMPONENTS Crypto SSL)

# define executable and shared library names
set(EMPY_IMPL_BROKER EmpyBroker)
set(EMPY_IMPL_CLIENT EmpyKlient)
set(EMPY_LIB_CLIENT EmpyClient)
set(EMPY_LIB_CORE EmpyCore)
set(EMPY_LIB_MQ EmpyMQ)

add_subdirectory(EMPY_LIB_CORE)
add_subdirectory(EMPY_LIB_MQ)
add_subdirectory(EMPY_LIB_CLIENT)
add_subdirectory(EMPY_IMPL_BROKER)
add_subdirectory(EMPY_IMPL_CLIENT)

if (EMPY_BUILD_TESTS)
    enable_testing()
    add_subdirectory(EMPY_TESTS)
endif()


