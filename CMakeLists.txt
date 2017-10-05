cmake_minimum_required(VERSION 3.4)

file(READ version.config VERSION_CONFIG)
set(VERSION_REGEX "version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.*[0-9]*)([-]([a-z]+))*\r?\nupdate_from_version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.*[0-9]*)(\r?\n)*$")

if (NOT (${VERSION_CONFIG} MATCHES ${VERSION_REGEX}))
  message(FATAL_ERROR "Cannot read version from version.config")
endif ()

set(VERSION ${CMAKE_MATCH_1})
set(VERSION_MOD ${CMAKE_MATCH_3})
set(UPDATE_FROM_VERSION ${CMAKE_MATCH_4})

if (VERSION_MOD)
  set(PROJECT_VERSION_MOD ${VERSION}-${VERSION_MOD})
else ()
  set(PROJECT_VERSION_MOD ${VERSION})
endif ()

if (NOT CMAKE_BUILD_TYPE)
  # Default to Release builds
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel" FORCE)
endif ()

project(timescaledb VERSION ${VERSION} LANGUAGES C)

message(STATUS "TimescaleDB version ${PROJECT_VERSION_MOD}. Can be updated from version ${UPDATE_FROM_VERSION}")
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

# Search paths for Postgres binaries
if (WIN32)
set(POSTGRES_PATHS
  "C:/PostgreSQL"
  "C:/PostgreSQL/pg96/bin")
endif (WIN32)

if (UNIX)
set(POSTGRES_PATHS
  "/usr/local/pgsql/bin")
endif (UNIX)

find_program(PG_CONFIG pg_config
  PATHS ${POSTGRES_PATHS}
  DOC "The path to the pg_config of the PostgreSQL version to compile against"
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

if (NOT ${PG_VERSION_STRING} MATCHES "^PostgreSQL[ ]+([0-9]+)\\.([0-9]+)\\.([0-9]+)$")
  message(FATAL_ERROR "Could not parse PostgreSQL version ${PG_VERSION}")
endif ()

set(PG_VERSION_MAJOR ${CMAKE_MATCH_1})
set(PG_VERSION_MINOR ${CMAKE_MATCH_2})
set(PG_VERSION_PATCH ${CMAKE_MATCH_3})
set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}.${PG_VERSION_PATCH}")

message(STATUS "Compiling against PostgreSQL version ${PG_VERSION}")

if ((${PG_VERSION} VERSION_LESS "9.6")
    OR (${PG_VERSION} VERSION_EQUAL "10")
    OR (${PG_VERSION} VERSION_GREATER "10"))
  message(FATAL_ERROR "TimescaleDB only supports PostgreSQL 9.6")
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

add_subdirectory(sql)
add_subdirectory(src)

if (UNIX)
  add_subdirectory(test)
endif (UNIX)

set(EXT_CONTROL_FILE ${PROJECT_NAME}.control)
configure_file(${EXT_CONTROL_FILE}.in ${EXT_CONTROL_FILE})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_CONTROL_FILE}
  DESTINATION "${PG_SHAREDIR}/extension")
