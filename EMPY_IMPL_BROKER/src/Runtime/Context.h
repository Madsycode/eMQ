#pragma once
#include "Service.h"
#include <csignal>

namespace eMQ
{
    inline volatile std::sig_atomic_t g_Interrupted = 0;

    struct Context : eCore::ImplContext<Service>
    {
        EMPY_INLINE Context(Service*)
        {
            
        }

        void OnUpdate(Service* service) override
        {
            if (g_Interrupted || service->Expired()) service->Terminate();
        }
    };
}

