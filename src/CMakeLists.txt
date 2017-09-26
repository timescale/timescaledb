set(CMAKE_C_FLAGS_DEBUG "-DUSE_ASSERT_CHECKING=1")

if (UNIX)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIBDIR}")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -L${PG_LIBDIR}")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PG_CFLAGS}")
  set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${PG_CPPFLAGS}")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")
endif (UNIX)

if (APPLE)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -multiply_defined suppress")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -multiply_defined suppress -Wl,-undefined,dynamic_lookup -Wl,-dead_strip_dylibs -bundle_loader ${PG_BINDIR}/postgres")
elseif (WIN32)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /MANIFEST:NO")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} /MANIFEST:NO")
endif (APPLE)

include_directories (${CMAKE_CURRENT_BINARY_DIR} ${PG_INCLUDEDIR} ${PG_INCLUDEDIR_SERVER})

if (WIN32)
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${PG_LIBDIR}/postgres.lib ws2_32.lib")
  set(CMAKE_C_FLAGS "-D_CRT_SECURE_NO_WARNINGS")
  include_directories(${PG_INCLUDEDIR_SERVER}/port/win32)

if (MSVC)
  include_directories(${PG_INCLUDEDIR_SERVER}/port/win32_msvc)
endif (MSVC)
endif (WIN32)

set(HEADERS
  cache.h
  catalog.h
  chunk_constraint.h
  chunk_dispatch.h
  chunk_dispatch_info.h
  chunk_dispatch_plan.h
  chunk_dispatch_state.h
  chunk.h
  chunk_index.h
  chunk_insert_state.h
  compat-endian.h
  compat-msvc-enter.h
  compat-msvc-exit.h
  constraint_aware_append.h
  copy.h
  dimension.h
  dimension_slice.h
  dimension_vector.h
  errors.h
  event_trigger.h
  executor.h
  extension.h
  guc.h
  hypercube.h
  hypertable_cache.h
  hypertable.h
  hypertable_insert.h
  indexing.h
  parse_rewrite.h
  partitioning.h
  planner_utils.h
  process_utility.h
  scanner.h
  subspace_store.h
  tablespace.h
  trigger.h
  utils.h)

set(SOURCES
  agg_bookend.c
  cache.c
  cache_invalidate.c
  catalog.c
  chunk.c
  chunk_constraint.c
  chunk_dispatch.c
  chunk_dispatch_info.c
  chunk_dispatch_plan.c
  chunk_dispatch_state.c
  chunk_index.c
  chunk_insert_state.c
  compat.c
  constraint_aware_append.c
  copy.c
  dimension.c
  dimension_slice.c
  dimension_vector.c
  event_trigger.c
  executor.c
  extension.c
  guc.c
  histogram.c
  hypercube.c
  hypertable.c
  hypertable_cache.c
  hypertable_insert.c
  indexing.c
  init.c
  parse_analyze.c
  parse_rewrite.c
  partitioning.c
  planner.c
  planner_utils.c
  process_utility.c
  scanner.c
  sort_transform.c
  subspace_store.c
  tablespace.c
  trigger.c
  utils.c
  version.c)

configure_file(version.h.in version.h)
set(GITCOMMIT_H ${CMAKE_CURRENT_BINARY_DIR}/gitcommit.h)

add_custom_command(
  OUTPUT ${GITCOMMIT_H}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E echo_append "#define EXT_GIT_COMMIT " > ${GITCOMMIT_H}
  COMMAND ${GIT_EXECUTABLE} describe --abbrev=4 --dirty --always --tags >> ${GITCOMMIT_H} || echo "nocommit" >> ${GITCOMMIT_H}
  COMMENT "Generating gitcommit.h"
  VERBATIM)

find_program(PGINDENT pgindent
  HINTS ${PG_SOURCE_DIR}
  PATH_SUFFIXES src/tools/pgindent
  DOC "Format C code according to PostgreSQL standards")

if (PGINDENT)
  message(STATUS "Using pgindent ${PGINDENT}")
else ()
  message(STATUS "Install pgindent to be able to format C code: https://github.com/postgres/postgres/tree/master/src/tools/pgindent")
endif (PGINDENT)

# Configuration for running pgindent
if (OBJDUMP AND PGINDENT)
  add_custom_command(OUTPUT typedefs.list
    DEPENDS ${PROJECT_NAME}
    COMMAND sh ${CMAKE_BINARY_DIR}/scripts/generate_typedefs.sh > typedefs.list)
  add_custom_target(pgindent
    COMMAND ${PGINDENT} -typedefs typedefs.list -code-base ${CMAKE_SOURCE_DIR}/src
    DEPENDS typedefs.list)
endif (OBJDUMP AND PGINDENT)

add_library(${PROJECT_NAME} MODULE ${SOURCES} ${HEADERS} ${GITCOMMIT_H})

set_target_properties(${PROJECT_NAME} PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}
  DESTINATION ${PG_PKGLIBDIR})

add_subdirectory(loader)
