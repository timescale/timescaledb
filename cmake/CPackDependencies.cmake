# Add a dependency to all components.
function(tsdb_add_dependency NAME VERSION)
  message(STATUS "Adding dependency on ${NAME} version ${VERSION}")
  if(TSDB_DEB_PACKAGE_DEPENDS)
    set(TSDB_DEB_PACKAGE_DEPENDS "${TSDB_DEB_PACKAGE_DEPENDS}, ${NAME} (>= ${VERSION})" PARENT_SCOPE)
  else()
    set(TSDB_DEB_PACKAGE_DEPENDS "${NAME} (>= ${VERSION})" PARENT_SCOPE)
  endif()

  if(TSDB_RPM_PACKAGE_DEPENDS)
    set(TSDB_RPM_PACKAGE_DEPENDS "${TSDB_RPM_PACKAGE_DEPENDS}, ${NAME} >= ${VERSION}" PARENT_SCOPE)
  else()
    set(TSDB_RPM_PACKAGE_DEPENDS "${NAME} >= ${VERSION}" PARENT_SCOPE)
  endif()
endfunction()

# Inspired by the function with the same name in CPackDeb.cmake
function(get_component_package_name VAR GEN COMPONENT)
  string(TOUPPER "${COMPONENT}" _component)
  if(NOT CPACK_${GEN}_${_component}_PACKAGE_NAME)
    message(FATAL_ERROR "Set the package name for component ${_component}")
  endif()
  string(TOLOWER "${CPACK_${GEN}_${_component}_PACKAGE_NAME}" _package)
  set("${VAR}" "${_package}" PARENT_SCOPE)
endfunction()

# Add a dependencies between component.
#
# Note that CMAKE_PROJECT_VERSION was introduced in 3.12, so we're
# using PROJECT_VERSION here instead.
function(tsdb_add_component_dependencies NAME)
  string(TOUPPER "${NAME}" _name)
  foreach(_component ${ARGN})
    get_component_package_name(_debian_name DEBIAN ${_component})
    get_component_package_name(_rpm_name RPM ${_component})
    if(TSDB_DEB_${_name}_PACKAGE_DEPENDS)
      set(TSDB_DEB_${_name}_PACKAGE_DEPENDS
	"${TSDB_DEB_${_name}_PACKAGE_DEPENDS}, ${_debian_name} (>= ${PROJECT_VERSION})" PARENT_SCOPE)
    else()
      set(TSDB_DEB_${_name}_PACKAGE_DEPENDS "${_debian_name} (>= ${PROJECT_VERSION})" PARENT_SCOPE)
    endif()

    if(TSDB_RPM_${_name}_PACKAGE_DEPENDS)
      set(TSDB_RPM_${_name}_PACKAGE_DEPENDS
	"${TSDB_RPM_${_name}_PACKAGE_DEPENDS}, ${_rpm_name} >= ${PROJECT_VERSION}" PARENT_SCOPE)
    else()
      set(TSDB_RPM_${_name}_PACKAGE_DEPENDS "${_rpm_name} >= ${PROJECT_VERSION}" PARENT_SCOPE)
    endif()
  endforeach()
endfunction()

# Get dependencies for a generator and component and store into a
# variable.
function(tsdb_get_dependencies VAR GEN COMP)
  set(_result "${TSDB_${GEN}_PACKAGE_DEPENDS}")
  if(_result)
    if(TSDB_${GEN}_${COMP}_PACKAGE_DEPENDS)
      set(_result "${_result}, ${TSDB_${GEN}_${COMP}_PACKAGE_DEPENDS}")
    endif()
  else()
    set(_result "${TSDB_${GEN}_${COMP}_PACKAGE_DEPENDS}")
  endif()
  set("${VAR}" "${_result}" PARENT_SCOPE)
endfunction()
