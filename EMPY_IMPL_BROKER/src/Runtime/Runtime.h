#pragma once
#include "Context.h"

namespace eMQ
{
    struct Runtime : eCore::ImplCoreRuntime<Service, Context>
    {
        using eCore::ImplCoreRuntime<Service, Context>::ImplCoreRuntime;
    };
}
