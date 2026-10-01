#pragma once
#include "Core.h"
#include "../Vendors/Entt.h"

namespace eCore
{
    /// @brief entity identt
    using Identt = uint32_t;
    
    /// @brief event dispatcher
    using Dispatcher = entt::dispatcher;

    /// @brief invalid entity identt
    constexpr Identt IDENTT = entt::null;
    
    /// @brief entity registry typedef
    using Registry = entt::basic_registry<Identt>;   

    /// @brief event listener callback
    template <typename _Event> 
    using Listener = std::function<void(const _Event&)>;

    /// @brief entity wrapper class
    struct Entt 
    {
        EMPY_INLINE Entt(Registry* registry): m_Registry(registry) 
        {
            m_Identt = m_Registry->create();		 			
        }

        EMPY_INLINE Entt(Registry* registry, Identt identt): 
            m_Registry(registry), m_Identt(identt) {}
        
        EMPY_INLINE virtual ~Entt() = default;

        EMPY_INLINE Entt() = default;		
         
        EMPY_INLINE operator Identt() const
        { 
            return m_Identt; 
        }

        EMPY_INLINE explicit operator bool() const
        { 
            return m_Registry && m_Registry->valid(m_Identt);
        }     

        // --

        template<typename _T, typename... Args>
        EMPY_INLINE _T& Attach(Args&&... args) 
        {
            return m_Registry->get_or_emplace<_T>
            (m_Identt, std::forward<Args>(args)...);
        }
        
        template<typename _T>
        EMPY_INLINE void Detach() 
        { 
            m_Registry->remove<_T>(m_Identt); 
        }

        EMPY_INLINE void Release() 
        {
            if(m_Registry && m_Registry->valid(m_Identt))
                m_Registry->destroy(m_Identt);    
            m_Registry = nullptr;
            m_Identt = IDENTT;         
        }

        template<typename... _T>
        EMPY_INLINE bool Has() const
        { 
            return m_Registry && m_Registry->valid(m_Identt)
                && m_Registry->all_of<_T...>(m_Identt);
        }

        template<typename _T>
        EMPY_INLINE _T& Get() 
        { 
            return m_Registry->get<_T>(m_Identt); 
        }

    protected:
        Registry* m_Registry = nullptr;
        Identt m_Identt = IDENTT;
    };
}
