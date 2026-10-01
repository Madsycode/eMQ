#pragma once
#include "../Runtime/Runtime.h"
#include <iostream>

namespace eMQ
{
    struct Monitoring : eCore::ImplModule<Service, Context>
    {
        void OnAttach(Service*, Context*) override
        {
            EMPY_DEBUG("Module attached!");
        }
    
        void OnUpdate(Service* service, Context*) override
        {
            if (Clock::now() < m_Next) return;
            auto metrics = service->Metrics();
            std::cout << "Sessions: " << metrics.sessions << ", online: " << metrics.online
                << ", publications: " << metrics.publications << ", retained bytes: " << metrics.retainedBytes << std::endl;
            m_Next = Clock::now() + std::chrono::seconds(1);
        }
    
        void OnDetach(Service*, Context*) override
        {
            EMPY_DEBUG("Module detached!");
        }

    private:
        TimePoint m_Next{};
    };
}

