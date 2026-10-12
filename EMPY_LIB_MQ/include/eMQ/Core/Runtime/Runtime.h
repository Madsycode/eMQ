#pragma once
#include "Module.h"

namespace eMQ
{    
    template <typename _Service, typename _Context> 
    struct ImplCoreRuntime : _Service
    {  
        using IModule = ImplModule<_Service, _Context>*;

        EMPY_INLINE void StartService(uint32_t delay)
        {
            m_Context->OnStart(this);
            while (this->PollEvents())
            {
                auto modules = this->Modules();
                for (auto identt : modules)
                {
                    auto entity = this->ToEntt(identt);
                    if (entity.template Has<IModule>())
                        entity.template Get<IModule>()->OnUpdate(this, m_Context);
                }
                m_Context->OnUpdate(this);
                this->WaitEvents(delay);
            }
            m_Context->OnStop(this);
        }

        template <typename _Module, typename..._Args>
        EMPY_INLINE Identt Attach(_Args&&...args)
        {            
            EMPY_STATIC((std::is_base_of_v<ImplModule<_Service, _Context>, _Module>));
            auto handle = std::make_unique<_Module>(std::forward<_Args>(args)...);
            Entt entity = this->CreateEntt();
            entity.Attach<IModule>(handle.get());
            handle->m_Identt = entity;
            handle->m_Service = this;
            try
            {
                handle->OnAttach(this, m_Context);
            }
            catch (...)
            {
                entity.Release();
                throw;
            }
            handle.release();
            return entity;
        }

        EMPY_INLINE void Detach(Identt identt)
        {       
            this->Dispatch([this, identt]
            {
                this->DetachNow(identt);
            });
        }

        void DetachModule(Identt identt) override
        {
            this->Detach(identt);
        }

        template <typename..._Args>
        EMPY_INLINE ImplCoreRuntime(_Args&&...args):
            _Service(std::forward<_Args>(args)...)
        {               
            // validate context type
            EMPY_STATIC(std::is_base_of_v<ImplContext<_Service>, _Context>
                && "_Context must inherit from ImplContext<>");
                
            // initialize user context
            if constexpr(std::is_constructible_v<_Context, _Service*>)
                this->m_Context = new _Context(this);
            else
                this->m_Context = new _Context(); 

            // validate service type
            EMPY_STATIC(std::is_base_of_v<ImplService, _Service> 
                && "Service must inherit from ImplService");                

        }   
               
        EMPY_INLINE virtual ~ImplCoreRuntime()
        {            
            this->Terminate();
            this->WaitTasks();
            for (auto identt : this->Modules())
            {
                try { this->DetachNow(identt); }
                catch (...) { EMPY_ERROR("Module detach failed during shutdown"); }
            }
            this->WaitTasks();
            EMPY_FREE(m_Context);
        }  

    private:
        std::vector<Identt> Modules()
        {
            std::vector<Identt> modules;
            this->template View<IModule>([&] (auto entity, auto&)
            {
                modules.emplace_back(entity);
            });
            return modules;
        }

        void DetachNow(Identt identt)
        {
            auto entity = this->ToEntt(identt);
            if (!entity.template Has<IModule>())
                return;
            std::unique_ptr<ImplModule<_Service, _Context>> module(entity.template Get<IModule>());
            entity.template Detach<IModule>();
            try { module->OnDetach(this, m_Context); }
            catch (...) { entity.Release(); throw; }
            entity.Release();
        }

        _Context* m_Context = nullptr;
    };
}
