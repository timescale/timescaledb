cmake_minimum_required(VERSION 3.4)
include(CheckCCompilerFlag)

configure_file("version.config" "version.config" COPYONLY)
file(READ version.config VERSION_CONFIG)
set(VERSION_REGEX "version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.*[0-9]*)([-]([a-z]+))*\r?\nupdate_from_version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.*[0-9]*)(\r?\n)*$")

if (NOT (${VERSION_CONFIG} MATCHES ${VERSION_REGEX}))
  message(FATAL_ERROR "Cannot read version from version.config")
endif ()
# a hack to avoid change of SQL extschema variable
set(extschema "@extschema@")
set(VERSION ${CMAKE_MATCH_1})
set(VERSION_MOD ${CMAKE_MATCH_3})
set(UPDATE_FROM_VERSION ${CMAKE_MATCH_4})

if (VERSION_MOD)
  set(PROJECT_VERSION_MOD ${VERSION}-${VERSION_MOD})
else ()
  set(PROJECT_VERSION_MOD ${VERSION})
endif ()

# Set project name, version, and language. Language needs to be set for compiler checks
project(timescaledb VERSION ${VERSION} LANGUAGES C)

message(STATUS "TimescaleDB version ${PROJECT_VERSION_MOD}. Can be updated from version ${UPDATE_FROM_VERSION}")
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

set(PROJECT_INSTALL_METHOD source CACHE STRING "Specify what install platform this binary
is built for")

if (NOT CMAKE_BUILD_TYPE)
  # Default to Release builds
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel" FORCE)
endif ()

if (CMAKE_BUILD_TYPE MATCHES Debug)
  # CMAKE_BUILD_TYPE is set at CMake configuration type. But usage of CMAKE_C_FLAGS_DEBUG is
  # determined at build time by running cmake --build . --config Debug (at least on Windows).
  # Therefore, we only set these flags if the configuration-time CMAKE_BUILD_TYPE is set to
  # Debug. Then Debug enabled builds will only happen on Windows if both the configuration-
  # and build-time settings are Debug.
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DUSE_ASSERT_CHECKING=1 -DDEBUG=1 -DTS_DEBUG=1")
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set(SUPPORTED_COMPILERS "GNU" "Clang" "AppleClang" "MSVC")

# Check for a supported compiler
if (NOT CMAKE_C_COMPILER_ID IN_LIST SUPPORTED_COMPILERS)
   message(FATAL_ERROR "Unsupported compiler ${CMAKE_C_COMPILER_ID}. Supported compilers are: ${SUPPORTED_COMPILERS}")
endif ()

# Check for supported platforms and compiler flags
if (WIN32)
  if (NOT CMAKE_CONFIGURATION_TYPES)
    # Default to only include Release builds so MSBuild.exe 'just works'
    set(CMAKE_CONFIGURATION_TYPES Release CACHE STRING "Semicolon separated list of supported configuration types, only supports Debug, Release, MinSizeRel, and RelWithDebInfo, anything else will be ignored." FORCE)
  endif ()
elseif (UNIX)
  # On UNIX, the compiler needs to support -fvisibility=hidden to hide symbols by default
  check_c_compiler_flag(-fvisibility=hidden CC_SUPPORTS_VISIBILITY_HIDDEN)

  if (NOT CC_SUPPORTS_VISIBILITY_HIDDEN)
    message(FATAL_ERROR "The compiler ${CMAKE_C_COMPILER_ID} does not support -fvisibility=hidden")
  endif (NOT CC_SUPPORTS_VISIBILITY_HIDDEN)
else ()
  message(FATAL_ERROR "Unsupported platform")
endif ()

message(STATUS "Using compiler ${CMAKE_C_COMPILER_ID}")

if (ENABLE_CODECOVERAGE)
  message(STATUS "Running code coverage")

  if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading")
  endif (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")

  if (NOT DEFINED CODECOV_OUTPUTFILE)
    set(CODECOV_OUTPUTFILE cmake_coverage.output)
  endif (NOT DEFINED CODECOV_OUTPUTFILE)

  if (NOT DEFINED CODECOV_HTMLOUTPUTDIR)
    set(CODECOV_HTMLOUTPUTDIR coverage_results)
  endif (NOT DEFINED CODECOV_HTMLOUTPUTDIR)

  find_program(CODECOV_GCOV gcov)
  find_program(CODECOV_LCOV lcov)
  add_definitions(-fprofile-arcs -ftest-coverage)
  link_libraries(gcov)
  set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} --coverage -fprofile-arcs)
  add_custom_target(coverage_init ALL ${CODECOV_LCOV} --base-directory . --directory ${CMAKE_BINARY_DIR} --output-file ${CODECOV_OUTPUTFILE} --capture --initial)

endif (ENABLE_CODECOVERAGE)

# Search paths for Postgres binaries
if (WIN32)
  find_path(PG_PATH
    bin/postgres
    HINTS
    "C:/PostgreSQL"
    "C:/Program Files/PostgreSQL"
    PATH_SUFFIXES
    bin
    10/bin
    96/bin
    pg96/bin
    DOC
    "The path to a PostgreSQL installation")
endif (WIN32)

if (UNIX)
  find_path(PG_PATH
    bin/postgres
    HINTS
    $ENV{HOME}
    /opt/local/pgsql
    /usr/local/pgsql
    /usr/lib/postgresql
    PATH_SUFFIXES
    bin
    10/bin
    9.6/bin
    96/bin
    pg96/bin
    DOC
    "The path to a PostgreSQL installation")
endif (UNIX)

find_program(PG_CONFIG pg_config
  HINTS
  ${PG_PATH}
  PATH_SUFFIXES
  bin
  DOC
  "The path to the pg_config of the PostgreSQL version to compile against"
  REQUIRED)

if (NOT PG_CONFIG)
  message(FATAL_ERROR "Unable to find 'pg_config'")
endif ()

find_package(Git)

message(STATUS "Using pg_config ${PG_CONFIG}")

# Check PostgreSQL version
execute_process(
  COMMAND ${PG_CONFIG} --version
  OUTPUT_VARIABLE PG_VERSION_STRING
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if (NOT ${PG_VERSION_STRING} MATCHES "^PostgreSQL[ ]+([0-9]+)\\.([0-9]+)(\\.([0-9]+))*")
  message(FATAL_ERROR "Could not parse PostgreSQL version ${PG_VERSION_STRING}")
endif ()

set(PG_VERSION_MAJOR ${CMAKE_MATCH_1})
set(PG_VERSION_MINOR ${CMAKE_MATCH_2})
set(PG_VERSION_PATCH ${CMAKE_MATCH_4})

if (NOT ${PG_VERSION_PATCH} OR ${PG_VERSION_PATCH} EQUAL "")
  set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}")
else ()
  set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}.${PG_VERSION_PATCH}")
endif ()

message(STATUS "Compiling against PostgreSQL version ${PG_VERSION}")

if ((${PG_VERSION} VERSION_LESS "9.6")
    OR (${PG_VERSION} VERSION_EQUAL "11")
    OR (${PG_VERSION} VERSION_GREATER "11"))
  message(FATAL_ERROR "TimescaleDB only supports PostgreSQL 9.6 or 10")
endif ()

# Get PostgreSQL configuration from pg_config
execute_process(
  COMMAND ${PG_CONFIG} --includedir
  OUTPUT_VARIABLE PG_INCLUDEDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --includedir-server
  OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --libdir
  OUTPUT_VARIABLE PG_LIBDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --pkglibdir
  OUTPUT_VARIABLE PG_PKGLIBDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --sharedir
  OUTPUT_VARIABLE PG_SHAREDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --bindir
  OUTPUT_VARIABLE PG_BINDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --cppflags
  OUTPUT_VARIABLE PG_CPPFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --cflags
  OUTPUT_VARIABLE PG_CFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --ldflags
  OUTPUT_VARIABLE PG_LDFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND ${PG_CONFIG} --libs
  OUTPUT_VARIABLE PG_LIBS
  OUTPUT_STRIP_TRAILING_WHITESPACE)

find_path(PG_SOURCE_DIR
  src/include/pg_config.h.in
  HINTS
  $ENV{HOME}
  $ENV{HOME}/projects
  $ENV{HOME}/Projects
  $ENV{HOME}/development
  $ENV{HOME}/Development
  $ENV{HOME}/workspace
  PATH_SUFFIXES
  postgres
  postgresql
  pgsql
  DOC
  "The path to the PostgreSQL source tree")

if (PG_SOURCE_DIR)
  message(STATUS "Found PostgreSQL source in ${PG_SOURCE_DIR}")
endif (PG_SOURCE_DIR)

set(EXT_CONTROL_FILE ${PROJECT_NAME}.control)
configure_file(${EXT_CONTROL_FILE}.in ${EXT_CONTROL_FILE})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_CONTROL_FILE}
  DESTINATION "${PG_SHAREDIR}/extension")

find_program(PGINDENT pgindent
  HINTS ${PG_SOURCE_DIR}
  PATH_SUFFIXES src/tools/pgindent
  DOC "Format C code according to PostgreSQL standards")

# Configuration for running pgindent
if (PGINDENT)
  message(STATUS "Using pgindent ${PGINDENT}")
  file(WRITE ${CMAKE_BINARY_DIR}/pgindent_excludes "\\..*/\n")

  add_custom_command(OUTPUT typedefs.list
    DEPENDS ${PROJECT_NAME}
    COMMAND sh ${PROJECT_BINARY_DIR}/scripts/generate_typedefs.sh > ${PROJECT_BINARY_DIR}/typedefs.list)
  add_custom_target(pgindent
    COMMAND ${PGINDENT} -typedefs typedefs.list -excludes=${PROJECT_BINARY_DIR}/pgindent_excludes -code-base=${PROJECT_SOURCE_DIR}/src
    COMMAND ${PGINDENT} -typedefs typedefs.list -excludes=${PROJECT_BINARY_DIR}/pgindent_excludes -code-base=${PROJECT_SOURCE_DIR}/test/src
    DEPENDS ${PROJECT_BINARY_DIR}/typedefs.list)
else ()
  message(STATUS "Install pgindent to be able to format C code: https://github.com/postgres/postgres/tree/master/src/tools/pgindent")
endif (PGINDENT)

option(USE_OPENSSL "Enable use of OpenSSL if available" ON)

if (USE_OPENSSL)
  # Try to find a local OpenSSL installation
  include(FindOpenSSL)

  if (NOT OPENSSL_FOUND)
    message(FATAL_ERROR "TimescaleDB requires OpenSSL but it wasn't found")
  endif(NOT OPENSSL_FOUND)

  if (${OPENSSL_VERSION} VERSION_LESS "1.0")
    message(FATAL_ERROR "TimescaleDB requires OpenSSL version 1.0 or greater")
  endif ()

  message(STATUS "Using OpenSSL version ${OPENSSL_VERSION}")
endif (USE_OPENSSL)

if (UNIX)
  add_subdirectory(test)
  add_subdirectory(scripts)
endif (UNIX)

add_subdirectory(sql)
add_subdirectory(src)
