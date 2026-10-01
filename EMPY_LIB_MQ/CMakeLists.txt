project(${EMPY_LIB_MQ})

# gather source files
file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE headers CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
add_library(${PROJECT_NAME} SHARED ${sources} ${headers})
configure_project_target(${PROJECT_NAME})

# export library .dll
target_compile_definitions(${PROJECT_NAME} PRIVATE 
    EMPY_MQ_SHARED
)

# includes directories
target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# link window libraries
if (WIN32)
    target_link_libraries(${PROJECT_NAME} PUBLIC
        ws2_32
        mswsock
    )
endif()

# link others libraries
target_link_libraries(${PROJECT_NAME} PUBLIC
    ${EMPY_LIB_CORE}
    asio::asio
    OpenSSL::Crypto
    OpenSSL::SSL
)

if (EMPY_ENABLE_QUIC)
    find_path(MSQUIC_INCLUDE_DIR msquic.h HINTS "${EMPY_MSQUIC_ROOT}/include" REQUIRED)
    find_library(MSQUIC_LIBRARY NAMES msquic HINTS "${EMPY_MSQUIC_ROOT}/lib" "${EMPY_MSQUIC_ROOT}/lib/x64" REQUIRED)
    target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC "${MSQUIC_INCLUDE_DIR}")
    target_link_libraries(${PROJECT_NAME} PUBLIC "${MSQUIC_LIBRARY}")
    target_compile_definitions(${PROJECT_NAME} PUBLIC EMPY_ENABLE_QUIC)
    if (WIN32)
        target_link_libraries(${PROJECT_NAME} PRIVATE crypt32)
        if (EMPY_MSQUIC_RUNTIME)
            if (NOT EXISTS "${EMPY_MSQUIC_RUNTIME}")
                message(FATAL_ERROR "EMPY_MSQUIC_RUNTIME does not exist")
            endif()
            add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different "${EMPY_MSQUIC_RUNTIME}" "$<TARGET_FILE_DIR:${PROJECT_NAME}>/msquic.dll")
        elseif (EXISTS "${EMPY_MSQUIC_ROOT}/bin/msquic.dll")
            add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different "${EMPY_MSQUIC_ROOT}/bin/msquic.dll" "$<TARGET_FILE_DIR:${PROJECT_NAME}>")
        endif()
    endif()
endif()
