project(${EMPY_IMPL_CLIENT})

# gather all source files
file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE headers CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h)
add_executable(${PROJECT_NAME} ${sources})
configure_project_target(${PROJECT_NAME})

# includes directories
target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# libraries directories
target_link_libraries(${PROJECT_NAME} PRIVATE
    ${EMPY_LIB_CLIENT}
)
empy_stage_runtime(${PROJECT_NAME})
