#pragma once
#include "Service.h"

namespace eCore
{   
    template <typename _Service> struct ImplContext
    {
        EMPY_INLINE ImplContext& operator=(const ImplContext&) = delete;        
        EMPY_INLINE ImplContext(const ImplContext&) = delete;
        EMPY_INLINE virtual ~ImplContext() = default;
        EMPY_INLINE ImplContext() = default;

        virtual void OnStart(_Service*) {}
        virtual void OnUpdate(_Service*) {}
        virtual void OnStop(_Service*) {}
    };
}
