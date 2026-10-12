#pragma once
#include "Context.h"

namespace eMQ
{   
    template <typename _Service, typename _Context> struct ImplModule
    {
        template <typename _F, typename..._A>
        using _Result = std::invoke_result_t<std::decay_t<_F>, std::decay_t<_A>...>;

        EMPY_INLINE ImplModule& operator=(const ImplModule&) = delete;        
        EMPY_INLINE ImplModule(const ImplModule&) = delete;
        EMPY_INLINE virtual ~ImplModule() = default;
        EMPY_INLINE ImplModule() = default;
                
        EMPY_INLINE virtual void OnAttach(_Service*, _Context*) = 0;
        EMPY_INLINE virtual void OnUpdate(_Service*, _Context*) = 0;        
        EMPY_INLINE virtual void OnDetach(_Service*, _Context*) = 0;
        
        template <typename _Callback, typename... _Args>
        EMPY_INLINE auto Async(_Callback&& callback, _Args&&...args) 
            -> std::future<_Result<_Callback, _Args...>>
        {
            return m_Service->Async(std::forward<_Callback>(callback), 
                std::forward<_Args>(args)...);
        }

        template <typename _Event, typename _Callback> 
        EMPY_INLINE void On(_Callback&& callback) 
        {
            m_Service->template OnEvent<_Event>(std::forward<_Callback>(callback), m_Identt);
        }

        template <typename _Event, typename..._Args>
        EMPY_INLINE void Invoke(_Args&&...args) 
        {
            m_Service->template Invoke<_Event>(std::forward<_Args>(args)...);
        }

        template <typename _Event, typename..._Args>
        EMPY_INLINE void Post(_Args&&...args) 
        { 
            m_Service->template Post<_Event>(std::forward<_Args>(args)...);
        } 

        EMPY_INLINE void Detach() 
        {       
            m_Service->DetachModule(m_Identt);
        }
        
        template <typename _Event> 
        EMPY_INLINE void Off() 
        { 
            m_Service->template OffEvent<_Event>(m_Identt);
        } 

    private:
        _Service* m_Service = nullptr;
        template <typename, typename>
        friend struct ImplCoreRuntime;
        Identt m_Identt = IDENTT;
    }; 
}
