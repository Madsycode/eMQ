#pragma once
#include <eCore/Common/Core.h>
#ifdef __linux__
    #include <time.h>
#endif

namespace eMQ
{
    // Lease expiry must advance across OS suspend; wall-clock corrections must not affect it.
    struct LeaseClock
    {
        using duration = std::chrono::milliseconds;
        using rep = duration::rep;
        using period = duration::period;
        using time_point = std::chrono::time_point<LeaseClock>;
        static constexpr bool is_steady = true;

        static time_point now()
        {
#ifdef _WIN32
            return time_point(duration(GetTickCount64()));
#elif defined(__linux__)
            timespec value{};
            if (::clock_gettime(CLOCK_BOOTTIME, &value)) throw std::runtime_error("Suspend-aware lease clock unavailable");
            return time_point(duration(value.tv_sec * 1000ULL + value.tv_nsec / 1000000));
#else
            throw std::runtime_error("This platform needs a suspend-aware lease clock implementation");
#endif
        }
    };
    using LeaseTimePoint = LeaseClock::time_point;
}
