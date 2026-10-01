#include "Checks.h"
#include <eCore/API.h>
#include <eCore/Auxiliaries/YAML.h>

namespace
{
    struct Event { int value = 0; };
    struct Service : eCore::ImplService {};
    struct Context : eCore::ImplContext<Service> {};
    using Runtime = eCore::ImplCoreRuntime<Service, Context>;

    struct Module : eCore::ImplModule<Service, Context>
    {
        int& received;
        int& detached;
        Module(int& value, int& count): received(value), detached(count) {}
        void OnAttach(Service*, Context*) override
        {
            this->On<Event>([this] (const Event& event) { received += event.value; });
        }
        void OnUpdate(Service*, Context*) override {}
        void OnDetach(Service*, Context*) override { ++detached; }
    };

    struct SelfDetach : eCore::ImplModule<Service, Context>
    {
        int& count;
        explicit SelfDetach(int& value): count(value) {}
        void OnAttach(Service*, Context*) override {}
        void OnUpdate(Service* service, Context*) override { this->Detach(); service->Terminate(); }
        void OnDetach(Service*, Context*) override { ++count; }
    };
}

void TestRuntime()
{
    int first = 0, second = 0, detached = 0;
    {
        Runtime left;
        Runtime right;
        auto identt = left.Attach<Module>(first, detached);
        right.Attach<Module>(second, detached);
        left.Post<Event>(5);
        right.Post<Event>(7);
        left.Poll();
        right.Poll();
        EMPY_CHECK(first == 5 && second == 7);
        left.Detach(identt);
        left.Poll();
        left.Post<Event>(100);
        left.Poll();
        EMPY_CHECK(first == 5 && detached == 1);
        auto worker = right.Async([&] { for (int i = 0; i < 100; ++i) right.Post<Event>(1); });
        worker.get();
        right.Poll();
        EMPY_CHECK(second == 107);
    }
    EMPY_CHECK(detached == 2);
    Runtime self;
    self.Attach<SelfDetach>(detached);
    self.StartService(1);
    EMPY_CHECK(detached == 3);
    Service bounded;
    bounded.SetDispatchLimits(2, 10);
    EMPY_CHECK(bounded.TryDispatch([] {}, 6));
    EMPY_CHECK(!bounded.TryDispatch([] {}, 5));
    EMPY_CHECK(bounded.TryDispatch([] {}, 4));
    EMPY_CHECK(!bounded.TryDispatch([] {}));
    bounded.Poll();
    EMPY_CHECK(bounded.TryDispatch([] {}, 10));

    auto config = fkyaml::node::deserialize("port: 65536\nnegative: -1\nvalid: 42\nfraction: 1.5\n");
    Throws([&] { eCore::ReadYAML<uint16_t>(config, "port", 5600); });
    Throws([&] { eCore::ReadYAML<uint32_t>(config, "negative", 0); });
    Throws([&] { eCore::ReadYAML<uint16_t>(config, "fraction", 0); });
    EMPY_CHECK(eCore::ReadYAML<uint8_t>(config, "valid", 0) == 42);
}
