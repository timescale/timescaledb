option(CODECOVERAGE "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

# Check whether we can enable the pclmul instruction required for the UMASH
# hashing on amd64. Shouldn't be done if the user has manually specified the
# target architecture, no idea how to detect this, but at least we shouldn't do
# this when cross-compiling.
if(NOT CMAKE_CROSSCOMPILING)
  check_c_compiler_flag(-mpclmul CC_PCLMUL)
  if(CC_PCLMUL)
    add_compile_options(-mpclmul)
    # The "C source compiles" check below doesn't use the global compilation
    # flags, so we have to modify its flags separately.
    set(CMAKE_REQUIRED_FLAGS -mpclmul)
  endif()
endif()

# Detect if we can compile UMASH on this platform.
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
check_c_source_compiles(
  "
#if defined(__PCLMUL__)
#include <stdint.h>
#include <immintrin.h>
/*
 * For some reason, this doesn't compile on our i386 CI, but I also can't detect
 * it using the standart condition of defined(__x86_64__) && !defined(__ILP32__),
 * as described at https://wiki.debian.org/X32Port .
 */
void test() { (void) _mm_cvtsi64_si128((uint64_t) 0); }
#elif defined(__ARM_FEATURE_CRYPTO)
#else
#error Unsupported platform for UMASH
#endif
"
  UMASH_SUPPORTED)
unset(CMAKE_REQUIRED_FLAGS)
unset(CMAKE_TRY_COMPILE_TARGET_TYPE)

option(USE_UMASH
       "Use the UMASH hash for string and multi-column vectorized grouping"
       ${UMASH_SUPPORTED})

if(USE_UMASH)
  add_compile_definitions(TS_USE_UMASH)
endif()

# Add the subdirectories
add_subdirectory(test)
add_subdirectory(src)
