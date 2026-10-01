#pragma once
#include <eMQ/API.h>

namespace eMQ
{
    struct Service : Broker
    {
        EMPY_INLINE Service(BrokerOptions options, uint32_t duration = 0): Broker(std::move(options))
        {
            if (duration) m_Deadline = Clock::now() + std::chrono::seconds(duration);
        }

        EMPY_INLINE virtual ~Service() = default; 

        EMPY_INLINE bool Expired() const { return Clock::now() >= m_Deadline; }

    private:
        TimePoint m_Deadline = TimePoint::max();
    };
}
