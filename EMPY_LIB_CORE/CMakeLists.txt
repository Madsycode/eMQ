project(${EMPY_LIB_CORE})

# gather source files
file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE headers CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
add_library(${PROJECT_NAME} SHARED ${sources} ${headers})
configure_project_target(${PROJECT_NAME})

# define api macros
target_compile_definitions(${PROJECT_NAME} PRIVATE
    EMPY_CORE_SHARED
)

# include directories
target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include 
)

target_link_libraries(${PROJECT_NAME} PUBLIC Threads::Threads)
