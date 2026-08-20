option(COMPRESSION_FUZZING "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

option(
  TMP_ENABLE_GRANULAR_REFRESH_TESTS
  "TODO: the granular refresh tests are disabled because of a shared memory leak"
  OFF)

# Add the subdirectories
add_subdirectory(test)
add_subdirectory(src)
