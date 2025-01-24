option(CODECOVERAGE "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

## We use the UMASH library for hashing in vectorized grouping. If it was not
## explicitly disabled already, detect if we can compile it on this platform.
##
## It is not tested on Windows and leads to a weird CI freeze, so don't even try
## for now.
##
## In WSL, it somehow freezes the CMake process and the entire GitHub action,
## with no possibility to get the logs. I was unable to debug it after a couple
## of days, so on WSL it's disabled as well. We don't really build for this
## configuration anyway, and just use it to generate and run the tests.
#if((NOT WIN32)
#   AND (NOT (CMAKE_HOST_SYSTEM_VERSION MATCHES "microsoft-standard-WSL2$"))
#   AND ((NOT DEFINED USE_UMASH) OR USE_UMASH))
#  # Check whether we can enable the pclmul instruction required for the UMASH
#  # hashing on amd64. Shouldn't be done if the user has manually specified the
#  # target architecture, no idea how to detect this, but at least we shouldn't
#  # do this when cross-compiling.
#  if(NOT CMAKE_CROSSCOMPILING)
#    check_c_compiler_flag(-mpclmul CC_PCLMUL)
#    if(CC_PCLMUL)
#      add_compile_options(-mpclmul)
#      # The "C source compiles" check below doesn't use the global compilation
#      # flags, so we have to modify its flags separately.
#      set(CMAKE_REQUIRED_FLAGS -mpclmul)
#    endif()
#  endif()

#  set(CMAKE_REQUIRED_FLAGS
#      "${CMAKE_REQUIRED_FLAGS} -Werror=implicit-function-declaration")
#  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
#  check_c_source_compiles(
#    "
##if defined(__PCLMUL__)
##include <stdint.h>
##include <immintrin.h>
#/*
# * For some reason, this doesn't compile on our i386 CI, but I also can't detect
# * it using the standard condition of defined(__x86_64__) && !defined(__ILP32__),
# * as described at https://wiki.debian.org/X32Port .
# */
#static void test() { (void) _mm_cvtsi64_si128((uint64_t) 0); }
##elif defined(__ARM_FEATURE_CRYPTO)
#/* OK */
##else
##error Unsupported platform for UMASH
##endif
#"
#    UMASH_SUPPORTED)
#  unset(CMAKE_REQUIRED_FLAGS)
#  unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
#else()
#  set(UMASH_SUPPORTED OFF)
#endif()

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

# Add the subdirectories
add_subdirectory(test)
add_subdirectory(src)
