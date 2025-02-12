set(SOURCES)

if(USE_UMASH)
  list(APPEND SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/umash.c)
endif()

if(SOURCES)
  # Disable clang-tidy for imported code
  add_library(target_no_static_code_analysis OBJECT ${SOURCES})
  set_target_properties(target_no_static_code_analysis PROPERTIES C_CLANG_TIDY
  target_sources(${TSL_LIBRARY_NAME}
                 PRIVATE $<TARGET_OBJECTS:target_no_static_code_analysis>)
endif()
