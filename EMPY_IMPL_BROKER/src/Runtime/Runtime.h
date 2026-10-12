#pragma once
#include "Context.h"

namespace eMQ
{
    struct Runtime : eMQ::ImplCoreRuntime<Service, Context>
    {
        using eMQ::ImplCoreRuntime<Service, Context>::ImplCoreRuntime;
    };
}
