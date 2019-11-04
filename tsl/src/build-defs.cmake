# Hide symbols by default in shared libraries
set(CMAKE_C_VISIBILITY_PRESET "hidden")

if (UNIX)
  set(CMAKE_C_STANDARD 11)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIBDIR}")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -L${PG_LIBDIR}")
  set(CMAKE_C_FLAGS "${PG_CFLAGS} ${CMAKE_C_FLAGS}")
  set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${PG_CPPFLAGS}")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")
endif()

if(APPLE)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -multiply_defined suppress")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -multiply_defined suppress -Wl,-undefined,dynamic_lookup -bundle_loader ${PG_BINDIR}/postgres")
elseif(WIN32)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /MANIFEST:NO")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} /MANIFEST:NO")
endif()

# PG_LDFLAGS can have strange values if not found, so we just add the
# flags if they are defined.
if(PG_LDFLAGS)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${PG_LDFLAGS}")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${PG_LDFLAGS}")
endif()

include_directories(
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/tsl/src
  ${PROJECT_BINARY_DIR}/src
  ${PROJECT_BINARY_DIR}/tsl/src)

include_directories(SYSTEM ${PG_INCLUDEDIR_SERVER})

# Only Windows and FreeBSD need the base include/ dir instead of include/server/, and including
# both causes problems on Ubuntu where they frequently get out of sync
if (WIN32 OR (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD"))
  include_directories(SYSTEM ${PG_INCLUDEDIR})
endif ()

if (WIN32)
  link_directories(${PROJECT_BINARY_DIR}/src)
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${PG_LIBDIR}/postgres.lib ws2_32.lib Version.lib ${PROJECT_NAME}-${PROJECT_VERSION_MOD}.lib")
  set(CMAKE_C_FLAGS "-D_CRT_SECURE_NO_WARNINGS")
  include_directories(SYSTEM ${PG_INCLUDEDIR_SERVER}/port/win32)

  if (MSVC)
    include_directories(SYSTEM ${PG_INCLUDEDIR_SERVER}/port/win32_msvc)
  endif (MSVC)
endif (WIN32)

# Name of library with test-specific code
set(TSL_TESTS_LIB_NAME ${PROJECT_NAME}-tsl-tests)
