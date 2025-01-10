option(CODECOVERAGE "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

# Check whether the UMASH hashing can be supported. We use it for vectorized
# grouping.
check_c_compiler_flag(-mpclmul CC_PCLMUL)
if(CC_PCLMUL)
  add_compile_options(-mpclmul)
  # For the "C source compiles" check below, it doesn't use the global
  # compilation flags.
  set(CMAKE_REQUIRED_FLAGS -mpclmul)
endif()

set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
check_c_source_compiles(
  "
#if !defined(__PCLMUL__) && !defined(__ARM_FEATURE_CRYPTO) \
    && !(defined(__aarch64__) || defined(__x86_64__))
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
