# General variables
set(TSDB_LICENSE "Apache-2.0 and Timescale License")
set(TSDB_RELNO 1 CACHE STRING
  "Release number if there are several releases of the same version")

# These are not really CPack variables (there are no
# component-specific package names), but we name them as if they are.
set(CPACK_LOADER_PACKAGE_NAME
  "timescaledb-loader-postgresql-${PG_VERSION_MAJOR}")
set(CPACK_BASIC_PACKAGE_NAME
  "timescaledb-oss-postgresql-${PG_VERSION_MAJOR}")
set(CPACK_FULL_PACKAGE_NAME
  "timescaledb-postgresql-${PG_VERSION_MAJOR}")

# Generic CPack variables
set(CPACK_COMPONENTS_ALL loader basic full) #Build these components
set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP) #Build one package for each component

set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/Description.txt")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION_MOD})
get_distr_generator(CPACK_GENERATOR)
set(CPACK_PACKAGE_CONTACT "Timescale <hello@timescale.com>")
set(CPACK_PACKAGE_VENDOR "Timescale")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/copyright.full.in
  ${CMAKE_CURRENT_BINARY_DIR}/copyright.full
  @ONLY)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/copyright.basic.in
  ${CMAKE_CURRENT_BINARY_DIR}/copyright.basic
  @ONLY)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/copyright.loader.in
  ${CMAKE_CURRENT_BINARY_DIR}/copyright.loader
  @ONLY)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/copyright.loader
  RENAME copyright
  DESTINATION share/doc/${CPACK_LOADER_PACKAGE_NAME}
  COMPONENT loader)
install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/copyright.basic
  RENAME copyright
  DESTINATION share/doc/${CPACK_BASIC_PACKAGE_NAME}
  COMPONENT basic)
install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/copyright.full
  RENAME copyright
  DESTINATION share/doc/${CPACK_FULL_PACKAGE_NAME}
  COMPONENT full)  

# RPM-specific CPack variables
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_URL ${CMAKE_PROJECT_HOMEPAGE_URL})
set(CPACK_RPM_PACKAGE_GROUP "database")
set(CPACK_RPM_PACKAGE_LICENSE ${TSDB_LICENSE})
set(CPACK_RPM_PACKAGE_RELEASE ${TSDB_RELNO})
set(CPACK_RPM_PACKAGE_RELEASE_DIST ON)

set(CPACK_RPM_LOADER_PACKAGE_NAME ${CPACK_LOADER_PACKAGE_NAME})
set(CPACK_RPM_BASIC_PACKAGE_NAME ${CPACK_BASIC_PACKAGE_NAME})
set(CPACK_RPM_FULL_PACKAGE_NAME ${CPACK_FULL_PACKAGE_NAME})

# Debian-specific CPack variables
set(CPACK_DEB_COMPONENT_INSTALL ON)

## We cannot use component dependencies since the dependency include
## the epoch and the release information, and we just want the release
## information.
set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS OFF)
#set(CPACK_DEBIAN_PACKAGE_DEBUG ON)

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${CMAKE_PROJECT_HOMEPAGE_URL})
set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_PACKAGE_RELEASE "${TSDB_RELNO}${CMAKE_DISTRO_NAME}~${CMAKE_DISTRO_RELEASE}")
set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "timescaledb-tools")

set(CPACK_DEBIAN_LOADER_PACKAGE_NAME ${CPACK_LOADER_PACKAGE_NAME})
set(CPACK_DEBIAN_BASIC_PACKAGE_NAME ${CPACK_BASIC_PACKAGE_NAME})
set(CPACK_DEBIAN_FULL_PACKAGE_NAME ${CPACK_FULL_PACKAGE_NAME})

# We explicitly add the dependency here since we cannot use the
# built-in support for tracking the component dependencies.
tsdb_add_component_dependencies(basic loader)
tsdb_add_component_dependencies(full loader basic)

# We need to fill in these variables before we include CPack
foreach(_comp LOADER BASIC FULL)
  tsdb_get_dependencies(CPACK_DEBIAN_${_comp}_PACKAGE_DEPENDS DEB ${_comp})
endforeach()

foreach(_comp LOADER BASIC FULL)
  tsdb_get_dependencies(CPACK_RPM_${_comp}_PACKAGE_REQUIRES RPM ${_comp})
endforeach()

include(CPack)

# We don't really need the dependencies below, but let's keep them for
# good measure.
cpack_add_component(loader
  DISPLAY_NAME "TimescaleDB Loader"
  DESCRIPTION "The loader for TimescaleDB to load individual versions.")
cpack_add_component(basic
  DISPLAY_NAME "TimescaleDB Basic"
  DEPENDS loader)
cpack_add_component(full
  DISPLAY_NAME "TimescaleDB Full"
  DEPENDS loader basic)

