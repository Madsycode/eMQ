#pragma once  
#include <bit>
#include <span>
#include <array>
#include <memory>
#include <optional>
#include <utility>
#include <cstdint>
#include <limits>
#include <chrono>
#include <atomic>
#include <vector>
#include <assert.h>
#include <stdexcept>
#include <algorithm>
#include <functional>
#include <string_view>
#include <type_traits>

// platform
#ifdef _WIN32
    // optimize win32 headers
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif  
    // desable min/max macros
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif
    // include win32 headers
    #include <windows.h> 
#elif defined(__linux__)
    #include <unistd.h>
#endif

// export macros: each shared library owns its exported declarations
#ifdef _WIN32
    #define EMPY_EXPORT __declspec(dllexport)
    #define EMPY_IMPORT __declspec(dllimport)
#else
    #define EMPY_EXPORT __attribute__((visibility("default")))
    #define EMPY_IMPORT
#endif
#ifdef EMPY_SHARED
    #define EMPY_API EMPY_EXPORT
#else
    #define EMPY_API EMPY_IMPORT
#endif
#ifdef EMPY_CORE_SHARED
    #define EMPY_CORE_API EMPY_EXPORT
#else
    #define EMPY_CORE_API EMPY_IMPORT
#endif
#ifdef EMPY_MQ_SHARED
    #define EMPY_MQ_API EMPY_EXPORT
#else
    #define EMPY_MQ_API EMPY_IMPORT
#endif
#ifdef EMPY_CLIENT_SHARED
    #define EMPY_CLIENT_API EMPY_EXPORT
#else
    #define EMPY_CLIENT_API EMPY_IMPORT
#endif

// uint24 max value
#ifndef UINT24_MAX
    #define UINT24_MAX 0xFFFFFF
#endif

// runtime assertion
#define EMPY_ASSERT assert

// compile assertion
#define EMPY_STATIC static_assert

// discard attribute
#if __cplusplus >= 201703L
    #define EMPY_NODISCARD [[nodiscard]]
#else
    #define EMPY_NODISCARD
#endif

// free allocated memory
#define EMPY_FREE(ptr) do { delete ptr; ptr = nullptr; } while(0)

// function inline macros
#if defined(__clang__) || defined(__GNUC__)
    #define EMPY_INLINE __attribute__((always_inline)) inline
    #define EMPY_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
    #define EMPY_INLINE __forceinline
    #define EMPY_NOINLINE __declspec(noinline)
#else
    #define EMPY_INLINE inline
    #define EMPY_NOINLINE
#endif   

// lambda binding
#define EMPY_BIND(f) [this](auto&&...args)->decltype(auto) { \
    return this->f(std::forward<decltype(args)>(args)...); }

// macros logging
#ifdef EMPY_LOGGING
    #include "Log.h"
    #define EMPY_TRACE(...) eCore::Trace(__VA_ARGS__)
    #define EMPY_DEBUG(...) eCore::Debug(__VA_ARGS__)
    #define EMPY_INFO(...)  eCore::Info(__VA_ARGS__)
    #define EMPY_WARN(...)  eCore::Warn(__VA_ARGS__)
    #define EMPY_ERROR(...) eCore::Error(__VA_ARGS__)
    #define EMPY_FATAL(...) eCore::Fatal(__VA_ARGS__)
#else
    #define EMPY_TRACE(...)
    #define EMPY_DEBUG(...)
    #define EMPY_INFO(...)
    #define EMPY_WARN(...)
    #define EMPY_ERROR(...)	
    #define EMPY_FATAL(...)
#endif


