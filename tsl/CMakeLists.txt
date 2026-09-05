option(COMPRESSION_FUZZING "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

option(
  TMP_ENABLE_GRANULAR_REFRESH_TESTS
  "Run the cagg granular refresh tests. Turn off where /dev/shm is too small for the tracker's shared memory leak, as in linux-32bit-build-and-test.yaml."
  ON)

# Add the subdirectories
add_subdirectory(test)
add_subdirectory(src)
