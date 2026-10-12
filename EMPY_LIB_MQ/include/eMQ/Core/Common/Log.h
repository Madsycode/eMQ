#pragma once
#include <mutex>
#include <atomic>
#include <format>
#include <thread>
#include <string>
#include <cstdint>
#include <sstream>
#include <iostream>
// Rang's Windows API lookup uses an upstream function-pointer cast.
#if defined(__GNUC__) && defined(_WIN32)
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wcast-function-type"
#endif
#include "../Vendors/Rang.h"
#if defined(__GNUC__) && defined(_WIN32)
    #pragma GCC diagnostic pop
#endif

namespace eCore
{
    enum class Level : uint8_t
    {
        None, Trace, Debug, Info,
        Warn, Error, Fatal
    };

    inline std::atomic<Level> g_Level = Level::Trace;
    inline std::mutex g_Mutex;

    // -------------------------
    // internal log function
    // -------------------------

    template <typename..._Args>
    inline void Write(Level level, rang::fg color, const char* tag, 
        std::format_string<_Args...> fmt, _Args&&...args)
    {
        if (level < g_Level.load(std::memory_order_relaxed)) { return; }

        auto sec = std::chrono::time_point_cast<std::chrono::seconds>(
            std::chrono::system_clock::now());

        std::ostringstream tid; tid << std::this_thread::get_id();
        std::string msg = std::format(fmt, std::forward<_Args>(args)...);
        //std::string msg = std::format(fmt, std::make_format_args(args...));

        std::scoped_lock lock(g_Mutex);
        std::cout << color 
            << std::format("[{:%Y-%m-%d %H:%M:%S}] [T:{}] [{:^5}] {}", 
                sec, tid.str(), tag, msg) << rang::style::reset << "\n";
    }

    // -------------------------
    // public API
    // -------------------------

    template <typename... _Args>
    inline void Trace(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Trace, rang::fg::reset, "TRACE", fmt, std::forward<_Args>(args)...);
    }

    template <typename... _Args>
    inline void Debug(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Debug, rang::fg::cyan, "DEBUG", fmt, std::forward<_Args>(args)...);
    }

    template <typename... _Args>
    inline void Info(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Info, rang::fg::green, "INFO", fmt, std::forward<_Args>(args)...);
    }

    template <typename... _Args>
    inline void Warn(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Warn, rang::fg::yellow, "WARN", fmt, std::forward<_Args>(args)...);
    }

    template <typename... _Args>
    inline void Error(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Error, rang::fg::red, "ERROR", fmt, std::forward<_Args>(args)...);
    }

    template <typename... _Args>
    inline void Fatal(std::format_string<_Args...> fmt, _Args&&...args)
    {
        Write(Level::Fatal, rang::fg::magenta, "FATAL", fmt, std::forward<_Args>(args)...);
    }

    // -------------------------
    // configuration
    // -------------------------

    inline void SetLevel(Level lvl)
    {
        g_Level.store(lvl, std::memory_order_relaxed);
    }
}
