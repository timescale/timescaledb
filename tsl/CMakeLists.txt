option(CODECOVERAGE "Enable fuzzing of compression using Libfuzzer" OFF)

if(COMPRESSION_FUZZING)
  add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

add_subdirectory(test)
add_subdirectory(src)
