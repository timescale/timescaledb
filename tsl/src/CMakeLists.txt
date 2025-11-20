set(SOURCES
    chunk_api.c
    chunk.c
    chunk_merge.c
    chunk_split.c
    chunkwise_agg.c
    init.c
    partialize_finalize.c
    planner.c
    process_utility.c
    reorder.c)

# Add test source code in Debug builds
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

set(TSL_LIBRARY_NAME ${PROJECT_NAME}-tsl)

include(build-defs.cmake)

if(CMAKE_BUILD_TYPE MATCHES Debug OR COMPRESSION_FUZZING)
  add_library(${TSL_LIBRARY_NAME} MODULE
              ${SOURCES} $<TARGET_OBJECTS:${TSL_TESTS_LIB_NAME}>)
else()
  add_library(${TSL_LIBRARY_NAME} MODULE ${SOURCES})
endif()

set_target_properties(
  ${TSL_LIBRARY_NAME}
  PROPERTIES OUTPUT_NAME ${TSL_LIBRARY_NAME}-${PROJECT_VERSION_MOD} PREFIX "")

target_include_directories(${TSL_LIBRARY_NAME} PRIVATE ${PG_INCLUDEDIR})

target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_TSL)
target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_SUBMODULE)

if(WIN32)
  target_link_libraries(${TSL_LIBRARY_NAME} ${PG_LIBDIR}/libpq.lib)
else()
  target_link_libraries(${TSL_LIBRARY_NAME} pq)
endif()

install(TARGETS ${TSL_LIBRARY_NAME} DESTINATION ${PG_PKGLIBDIR})

# if (WIN32) target_link_libraries(${PROJECT_NAME}
# ${PROJECT_NAME}-${PROJECT_VERSION_MOD}.lib) endif(WIN32)

# We use the UMASH library for hashing in vectorized grouping. If it was not
# explicitly disabled already, detect if we can compile it on this platform.
if((NOT DEFINED USE_UMASH) OR USE_UMASH)
  # Check whether we can enable the pclmul instruction required for the UMASH
  # hashing on amd64. Shouldn't be done if the user has manually specified the
  # target architecture, no idea how to detect this, but at least we shouldn't
  # do this when cross-compiling.
  if(NOT CMAKE_CROSSCOMPILING)
    check_c_compiler_flag(-mpclmul CC_PCLMUL)
    if(CC_PCLMUL)
      add_compile_options(-mpclmul)
    endif()
  endif()

  # The C compiler flags that we add using add_compile_options() are not
  # automatically used by check_c_source_compiles() because it works in a
  # separate project, so we have to add them manually.
  get_directory_property(DIR_COMPILE_OPTIONS_LIST COMPILE_OPTIONS)
  list(JOIN DIR_COMPILE_OPTIONS_LIST " " DIR_COMPILE_OPTIONS)
  set(CMAKE_REQUIRED_FLAGS
      "${CMAKE_REQUIRED_FLAGS} ${CMAKE_C_FLAGS} ${DIR_COMPILE_OPTIONS} -Werror=implicit-function-declaration"
  )

  check_c_source_compiles(
    "
#if defined(__PCLMUL__)
#include <stdint.h>
#include <immintrin.h>
/*
 * For some reason, this doesn't compile on our i386 CI, but I also can't detect
 * it using the standard condition of defined(__x86_64__) && !defined(__ILP32__),
 * as described at https://wiki.debian.org/X32Port .
 */
int main() { (void) _mm_cvtsi64_si128((uint64_t) 0); return 0; }
#elif defined(__ARM_FEATURE_CRYPTO)
/* OK */
int main() { return 0; }
#else
#error Unsupported platform for UMASH
#endif
"
    UMASH_SUPPORTED)
  unset(CMAKE_REQUIRED_FLAGS)
else()
  set(UMASH_SUPPORTED OFF)
endif()

option(USE_UMASH
       "Use the UMASH hash for string and multi-column vectorized grouping"
       ${UMASH_SUPPORTED})

if(USE_UMASH)
  if(NOT UMASH_SUPPORTED)
    message(
      FATAL_ERROR
        "UMASH use is requested, but it is not supported in the current configuration"
    )
  endif()
  add_compile_definitions(TS_USE_UMASH)
endif()

add_subdirectory(bgw_policy)
add_subdirectory(compression)
add_subdirectory(continuous_aggs)
add_subdirectory(import)
add_subdirectory(nodes)
