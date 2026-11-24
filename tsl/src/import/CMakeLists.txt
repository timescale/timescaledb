# The forked toast writer copies core functions as they stand in PG 17.11 and
# 18.6 and is only built there, so the copies need no version branches of their
# own. This has to agree with COMPRESSION_TOASTER_SUPPORTED in
# compression_toast.h, which supplies stand-ins everywhere else.
set(SOURCES "")

if((PG_VERSION_MAJOR EQUAL 17 AND PG_VERSION_MINOR GREATER_EQUAL 11)
   OR (PG_VERSION_MAJOR EQUAL 18 AND PG_VERSION_MINOR GREATER_EQUAL 6))
  list(
    APPEND
    SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/access/common/toast_internals.c
    ${CMAKE_CURRENT_SOURCE_DIR}/access/heap/heapam.c
    ${CMAKE_CURRENT_SOURCE_DIR}/access/heap/heaptoast.c
    ${CMAKE_CURRENT_SOURCE_DIR}/access/table/toast_helper.c)
endif()

if(USE_UMASH)
  list(APPEND SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/umash.c)
endif()

if(SOURCES)
  # Disable clang-tidy for imported code
  add_library(target_no_static_code_analysis OBJECT ${SOURCES})
  set_target_properties(target_no_static_code_analysis PROPERTIES C_CLANG_TIDY
                                                                  "")
  target_sources(${TSL_LIBRARY_NAME}
                 PRIVATE $<TARGET_OBJECTS:target_no_static_code_analysis>)
endif()
