#pragma once
#include "../Common/ECS.h"
#include "../Vendors/Pool.h"
#include <deque>


namespace eMQ
{      
    struct ImplService
    {   
        template <typename _F, typename..._A>
        using _Result = std::invoke_result_t<std::decay_t<_F>, std::decay_t<_A>...>;

        EMPY_INLINE ImplService& operator=(const ImplService&) = delete;

        EMPY_INLINE ImplService(const ImplService&) = delete; 
                    
        EMPY_INLINE virtual ~ImplService() 
        {
            this->Terminate();
            this->WaitTasks();
            this->m_Dispatcher.clear();
            this->m_Registry.clear();
        }

        EMPY_INLINE ImplService(): 
            m_Pool(1)
        {
            this->m_Identt = this->m_Registry.create();
        }

        // --

        template <typename _Comp, typename _Entt = Entt, typename _Callback>
        EMPY_INLINE void View(_Callback&& callback) 
        {            
            EMPY_STATIC(std::is_base_of_v<Entt, _Entt> && "Entt must inherite from Entt");
            m_Registry.view<_Comp>().each([&] (auto identt, auto& comp) 
            { 
                callback(_Entt(&m_Registry, identt), comp); 
            });                
        }
        
        template <typename _Entt = Entt> 
        EMPY_INLINE _Entt ToEntt(Identt identt)
        {
            EMPY_STATIC(std::is_base_of_v<Entt, _Entt> && 
                "Entt must inherite from Entt");
            return _Entt(&m_Registry, identt);
        }

        template <typename _Entt = Entt>
        EMPY_INLINE _Entt CreateEntt() 
        {            
            EMPY_STATIC(std::is_base_of_v<Entt, _Entt> && 
                "Entt must inherite from Entt");
            return _Entt(&m_Registry);
        }

        // --  

        template <typename _Callback, typename..._Args>
        EMPY_INLINE auto Async(_Callback&& fn, _Args&&...args) 
            -> std::future<_Result<_Callback, _Args...>>
        {
            return m_Pool.submit(std::forward<_Callback>(fn), 
                std::forward<_Args>(args)...);
        }
               
        template <typename _Event, typename..._Args>
        EMPY_INLINE void Invoke(_Args&&...args) 
        {            
            this->m_Dispatcher.trigger<_Event>(std::forward<_Args>(args)...);             
        }
        
        template <typename _Event, typename..._Args>
        EMPY_INLINE void Post(_Args&&...args)
        {            
            this->Dispatch([this, event = _Event{std::forward<_Args>(args)...}]
            {
                this->m_Dispatcher.trigger(event);
            });
        }

        template <typename _Event, typename..._Args>
        EMPY_INLINE bool TryPost(size_t bytes, _Args&&...args)
        {
            if (bytes > SIZE_MAX - sizeof(_Event)) return false;
            return this->TryDispatch([this, event = _Event{std::forward<_Args>(args)...}]
            {
                this->m_Dispatcher.trigger(event);
            }, bytes + sizeof(_Event));
        }

        template <typename _Event, typename _Callback> 
        EMPY_INLINE void On(_Callback&& fn) 
        {   
            this->template OnEvent<_Event>(
                std::forward<_Callback>(fn), this->m_Identt);
        }
        
        EMPY_INLINE void Terminate()
        {
            this->m_Run.store(false);
            m_Condition.notify_all();
        }

        // Registry access and callbacks belong to the thread calling Poll().
        EMPY_INLINE void Dispatch(std::function<void()> task)
        {
            if (!this->TryDispatch(std::move(task))) throw std::overflow_error("Service dispatch queue is full");
        }

        EMPY_INLINE bool TryDispatch(std::function<void()> task, size_t bytes = 0)
        {
            {
                std::scoped_lock lock(m_Mutex);
                if (m_Tasks.size() >= m_MaxTasks || bytes > m_MaxTaskBytes - m_TaskBytes) return false;
                m_Tasks.emplace_back(std::move(task));
                m_TaskBytes += bytes;
            }
            m_Condition.notify_one();
            return true;
        }

        EMPY_INLINE void SetDispatchLimits(size_t tasks, size_t bytes)
        {
            std::scoped_lock lock(m_Mutex);
            if (!tasks || !bytes || tasks < m_Tasks.size() || bytes < m_TaskBytes)
                throw std::invalid_argument("Dispatch limits must be positive and cover pending work");
            m_MaxTasks = tasks;
            m_MaxTaskBytes = bytes;
        }

        bool Poll()
        {
            std::deque<std::function<void()>> tasks;
            {
                std::scoped_lock lock(m_Mutex);
                tasks.swap(m_Tasks);
                m_TaskBytes = 0;
            }
            for (auto& task : tasks)
                task();
            return m_Run.load();
        }

        void StartService(uint32_t delay = 10)
        {
            while (this->Poll())
                this->WaitEvents(delay);
        }

        EMPY_INLINE void WaitTasks()
        {
            m_Pool.wait_for_tasks();
        }

        virtual void DetachModule(Identt)
        {
            throw std::logic_error("No module runtime is attached to this service");
        }
        
        template <typename _Event>
        EMPY_INLINE void Off() 
        { 
            this->template OffEvent<_Event>(this->m_Identt);
        } 

    protected:
        template <typename _Event, typename _Callback> 
        EMPY_INLINE void OnEvent(_Callback&& fn, Identt identt) 
        {   
            auto entity = this->ToEntt(identt);
            if (!entity)
                throw std::invalid_argument("Invalid event listener entity");
            entity.template Attach<Listener<_Event>>() = std::forward<_Callback>(fn);
            this->m_Dispatcher.template sink<_Event>().template 
                connect<&ImplService::template OnListen<_Event>>(*this);
        }

        template <typename _Event>
        EMPY_INLINE void OnListen(const _Event& event) 
        {            
            std::vector<Identt> listeners;
            this->template View<Listener<_Event>>([&] (auto entity, auto&)
            {
                listeners.emplace_back(entity);
            });
            for (auto identt : listeners)
            {
                auto entity = this->ToEntt(identt);
                if (entity.template Has<Listener<_Event>>())
                {
                    auto callback = entity.template Get<Listener<_Event>>();
                    callback(event);
                }
            }
        }

        template <typename _Event>
        EMPY_INLINE void OffEvent(Identt identt) 
        { 
            if(auto entity = this->ToEntt(identt))  
                entity.template Detach<Listener<_Event>>();  
            else
            {
                EMPY_ERROR("Failed to unregister callback! invalid entity identt!");
            }
        }
        
        EMPY_INLINE bool PollEvents()
        {
            return this->Poll();
        }

        void WaitEvents(uint32_t delay)
        {
            std::unique_lock lock(m_Mutex);
            m_Condition.wait_for(lock, std::chrono::milliseconds(delay), [this]
            {
                return !m_Tasks.empty() || !m_Run.load();
            });
        }
              
    private:         
        std::atomic_bool m_Run = true;      
        template<typename,typename>
        friend struct ImplModule;

        Identt m_Identt = IDENTT;
        Dispatcher m_Dispatcher;    
        Registry m_Registry; 
        std::mutex m_Mutex;
        std::condition_variable m_Condition;
        std::deque<std::function<void()>> m_Tasks;
        size_t m_MaxTasks = 4096;
        size_t m_MaxTaskBytes = 16 * 1024 * 1024;
        size_t m_TaskBytes = 0;
        tp::thread_pool m_Pool;
    };
}
