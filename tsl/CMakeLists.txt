option(CODECOVERAGE "Enable fuzzing of compression using Libfuzzer" OFF)

if (COMPRESSION_FUZZING)
    if (NOT CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang")
        message(FATAL_ERROR "Code coverage is only available with Clang or AppleClang compilers, but we're using ${CMAKE_C_COMPILER_ID} (${CMAKE_C_COMPILER})")
    endif()
    get_filename_component(CLANG_DIR ${CMAKE_C_COMPILER} DIRECTORY)
    string(REGEX MATCH "^[0-9]+" CMAKE_C_COMPILER_VERSION_MAJOR
        ${CMAKE_C_COMPILER_VERSION})
    find_program(LLVM_CONFIG "llvm-config-${CMAKE_C_COMPILER_VERSION_MAJOR}" PATHS ${CLANG_DIR} NO_DEFAULT_PATH REQUIRED)
    message(STATUS "llvm-config is ${LLVM_CONFIG}")
    execute_process(COMMAND ${LLVM_CONFIG} --libdir OUTPUT_VARIABLE LLVM_LIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "libdir is ${LLVM_LIB_DIR}")
    set(LIB_FUZZER_NAME "libclang_rt.fuzzer_no_main-x86_64.a")
    execute_process(COMMAND find "${LLVM_LIB_DIR}/clang/${CMAKE_C_COMPILER_VERSION_MAJOR}" -type f -name ${LIB_FUZZER_NAME} OUTPUT_VARIABLE LIB_FUZZER_FULL_NAMEPATH OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "full is ${LIB_FUZZER_FULL_NAMEPATH}")
    # add_link_options(-fsanitize=fuzzer-no-link -static-libsan -lstdc++)
    add_compile_definitions(TS_COMPRESSION_FUZZING=1)
endif()

add_subdirectory(test)
add_subdirectory(src)

