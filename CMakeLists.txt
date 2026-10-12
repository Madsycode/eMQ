cmake_minimum_required(VERSION 3.20 FATAL_ERROR)

# ------------------------------------------------------------------------------
# Windows runtime staging, script mode. Each executable's generated
# Runtime/<target>-<config>.cmake sets the EMPY_* inputs and includes this file:
# it copies the resolved PE import graph plus runtime-loaded providers beside the
# executable and writes a SHA-256 manifest. Configure-time code follows below.
# ------------------------------------------------------------------------------
if (CMAKE_SCRIPT_MODE_FILE)
    if (POLICY CMP0207)
        cmake_policy(SET CMP0207 NEW)
    endif()
    get_filename_component(output "${EMPY_EXECUTABLE}" DIRECTORY)
    file(LOCK "${output}/.runtime.lock" TIMEOUT 60)
    set(CMAKE_GET_RUNTIME_DEPENDENCIES_PLATFORM "windows+pe")
    if (EMPY_OBJDUMP)
        set(CMAKE_GET_RUNTIME_DEPENDENCIES_TOOL "objdump")
        set(CMAKE_GET_RUNTIME_DEPENDENCIES_COMMAND "${EMPY_OBJDUMP}")
    endif()
    file(GET_RUNTIME_DEPENDENCIES
        EXECUTABLES "${EMPY_EXECUTABLE}"
        LIBRARIES ${EMPY_DYNAMIC_DLLS}
        DIRECTORIES "${output}" ${EMPY_DLL_SEARCH}
        RESOLVED_DEPENDENCIES_VAR resolved
        UNRESOLVED_DEPENDENCIES_VAR unresolved
        PRE_EXCLUDE_REGEXES "^api-ms-" "^ext-ms-"
        POST_INCLUDE_REGEXES ".*/[Vv][Cc][Rr][Uu][Nn][Tt][Ii][Mm][Ee].*\\.dll$" ".*/[Mm][Ss][Vv][Cc][Pp].*\\.dll$"
        POST_EXCLUDE_REGEXES ".*[/\\\\][Ss][Yy][Ss][Tt][Ee][Mm]32[/\\\\].*" ".*[/\\\\][Ss][Yy][Ss][Ww][Oo][Ww]64[/\\\\].*")
    if (unresolved)
        message(FATAL_ERROR "Missing runtime DLLs for ${EMPY_EXECUTABLE}: ${unresolved}. Set EMPY_RUNTIME_DIRECTORIES to their directories.")
    endif()
    get_filename_component(name "${EMPY_EXECUTABLE}" NAME)
    set(manifest "Runtime imports and dynamic providers for ${name}; Windows OS DLLs are supplied by Windows.\n")
    list(APPEND resolved ${EMPY_DYNAMIC_DLLS})
    list(REMOVE_DUPLICATES resolved)
    foreach(dependency IN LISTS resolved)
        get_filename_component(filename "${dependency}" NAME)
        get_filename_component(source "${dependency}" ABSOLUTE)
        get_filename_component(destination "${output}/${filename}" ABSOLUTE)
        if (NOT source STREQUAL destination)
            execute_process(COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${source}" "${destination}"
                RESULT_VARIABLE copied)
            if (NOT copied EQUAL 0)
                message(FATAL_ERROR "Could not stage ${source}")
            endif()
        endif()
        file(SHA256 "${destination}" hash)
        string(APPEND manifest "${hash}  ${filename}\n")
    endforeach()
    file(WRITE "${output}/${name}.runtime.txt" "${manifest}")
    return()
endif()

project(Empy VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 99)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build configuration" FORCE)
endif()

# Binaries and their staged runtime live in <build>/bin; one build tree per configuration.
get_property(EMPY_MULTI_CONFIG GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if (EMPY_MULTI_CONFIG)
    set(EMPY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/$<CONFIG>")
else()
    set(EMPY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${EMPY_OUTPUT_DIRECTORY}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${EMPY_OUTPUT_DIRECTORY}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${EMPY_OUTPUT_DIRECTORY}")

option(EMPY_BUILD_TESTS "Build protocol and integration tests" ON)
option(EMPY_ENABLE_QUIC "Build the optional MsQuic transport" ON)
option(EMPY_STAGE_RUNTIME "Copy Windows runtime dependencies beside executables" ON)

set(EMPY_MSQUIC_RUNTIME "$ENV{EMPY_MSQUIC_RUNTIME}" CACHE FILEPATH "Optional MsQuic DLL, matching TLS provider")
set(EMPY_ASIO_INCLUDE_DIR "" CACHE PATH "Standalone Asio include directory")
set(EMPY_MSQUIC_ROOT "$ENV{EMPY_MSQUIC_ROOT}" CACHE PATH "MsQuic installation directory")
set(EMPY_RUNTIME_DIRECTORIES "" CACHE STRING "Additional DLL search directories")
set(EMPY_RUNTIME_LIBRARIES "" CACHE STRING "Additional runtime-loaded DLL files to stage")

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

# Stages the actual PE import graph (compiler, OpenSSL, MsQuic and transitive DLLs)
# beside a Windows executable. A per-target runtime target also repairs a deleted
# dependency when the executable did not relink.
function(empy_stage_runtime target)
    if (NOT WIN32 OR NOT EMPY_STAGE_RUNTIME)
        return()
    endif()
    get_filename_component(compiler_bin "${CMAKE_CXX_COMPILER}" DIRECTORY)
    set(search "${compiler_bin}" "${OPENSSL_ROOT_DIR}/bin" ${EMPY_RUNTIME_DIRECTORIES})
    foreach(library IN ITEMS "${OPENSSL_CRYPTO_LIBRARY}" "${OPENSSL_SSL_LIBRARY}" "${MSQUIC_LIBRARY}" "${EMPY_MSQUIC_RUNTIME}")
        if (library)
            get_filename_component(directory "${library}" DIRECTORY)
            list(APPEND search "${directory}" "${directory}/../bin" "${directory}/../../bin")
        endif()
    endforeach()
    get_cmake_property(variables VARIABLES)
    foreach(variable IN LISTS variables)
        if (variable MATCHES "(_BIN_DIRS|_BINDIRS)")
            list(APPEND search ${${variable}})
        endif()
    endforeach()
    list(APPEND search "${EMPY_MSQUIC_ROOT}/bin")
    list(REMOVE_DUPLICATES search)
    # OpenSSL loads compression providers through DSO_load, outside the PE import graph.
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES "${OPENSSL_INCLUDE_DIR}")
    set(dynamic ${EMPY_RUNTIME_LIBRARIES})
    foreach(algorithm IN ITEMS ZLIB BROTLI ZSTD)
        check_cxx_source_compiles("#include <openssl/configuration.h>
#if defined(OPENSSL_NO_COMP) || defined(OPENSSL_NO_${algorithm}) || defined(OPENSSL_NO_${algorithm}_DYNAMIC)
#error Compression provider is not loaded dynamically
#endif
int main() { return 0; }" EMPY_OPENSSL_DYNAMIC_${algorithm})
        if (EMPY_OPENSSL_DYNAMIC_${algorithm})
            if (algorithm STREQUAL "ZLIB")
                set(components ZLIB)
                set(ZLIB_names zlib1.dll ZLIB1.dll)
            elseif(algorithm STREQUAL "BROTLI")
                set(components BROTLI_DEC BROTLI_ENC)
                set(BROTLI_DEC_names brotlidec.dll libbrotlidec.dll)
                set(BROTLI_ENC_names brotlienc.dll libbrotlienc.dll)
            else()
                set(components ZSTD)
                set(ZSTD_names libzstd.dll zstd.dll)
            endif()
            foreach(component IN LISTS components)
                find_file(EMPY_${component}_RUNTIME NAMES ${${component}_names} HINTS ${search} REQUIRED)
                list(APPEND dynamic "${EMPY_${component}_RUNTIME}")
            endforeach()
        endif()
    endforeach()
    list(REMOVE_DUPLICATES dynamic)
    set(configuration "${CMAKE_BINARY_DIR}/Runtime/${target}-$<CONFIG>.cmake")
    file(GENERATE OUTPUT "${configuration}" CONTENT
        "set(EMPY_EXECUTABLE [==[$<TARGET_FILE:${target}>]==])
        set(EMPY_DLL_SEARCH [==[${search}]==])
        set(EMPY_DYNAMIC_DLLS [==[${dynamic}]==])
        set(EMPY_OBJDUMP [==[${CMAKE_OBJDUMP}]==])
        include([==[${CMAKE_SOURCE_DIR}/CMakeLists.txt]==])")
    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -P "${configuration}" VERBATIM)
    add_custom_target(${target}Runtime ALL
        COMMAND "${CMAKE_COMMAND}" -P "${configuration}"
        DEPENDS ${target} VERBATIM)
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
set(EMPY_LIB_MQ EmpyMQ)

add_subdirectory(EMPY_LIB_MQ)
add_subdirectory(EMPY_LIB_CLIENT)
add_subdirectory(EMPY_IMPL_BROKER)
add_subdirectory(EMPY_IMPL_CLIENT)

if (EMPY_BUILD_TESTS)
    enable_testing()
    add_subdirectory(EMPY_TESTS)
endif()
