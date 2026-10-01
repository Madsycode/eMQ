project(${EMPY_LIB_CLIENT})

# gather source files
file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE headers CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
add_library(${PROJECT_NAME} SHARED ${sources} ${headers})
target_sources(${PROJECT_NAME} PRIVATE src/Vendors/Ascon/aead.c)
target_include_directories(${PROJECT_NAME} PRIVATE src/Vendors/Ascon)
configure_project_target(${PROJECT_NAME})

# define api macros
target_compile_definitions(${PROJECT_NAME} PRIVATE
    EMPY_CLIENT_SHARED
)

# include directories
target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include 
)

target_link_libraries(${PROJECT_NAME} PUBLIC
    ${EMPY_LIB_MQ}
)
