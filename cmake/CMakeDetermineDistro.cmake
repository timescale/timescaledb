# Utilities for handling distro-specific things.

# CMAKE_DISTRO_NAME - The specific name of the distro, e.g, 'ubuntu', 'debian'
# CMAKE_DISTRO_RELEASE - The specific release version of the distro
set(CMAKE_DISTRO_NAME)
set(CMAKE_DISTRO_RELEASE)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_program(LSB_RELEASE
    lsb_release
    PATHS /usr/bin /usr/local/bin /usr/local/opt)
  if(NOT LSB_RELEASE)
    message(FATAL_ERROR "Cannot find lsb_release")
  endif()

  execute_process(
    COMMAND ${LSB_RELEASE} -is
    OUTPUT_VARIABLE _identifier
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(TOLOWER "${_identifier}" CMAKE_DISTRO_NAME)
    
  execute_process(
    COMMAND ${LSB_RELEASE} -rs
    OUTPUT_VARIABLE _release
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(CMAKE_DISTRO_RELEASE ${_release})
endif()

message(STATUS "Distribution identifier: ${CMAKE_DISTRO_NAME}")
message(STATUS "Distribution release: ${CMAKE_DISTRO_RELEASE}")

# Variables to map distribution name to generator
set(CPACK_debian_GENERATOR "DEB")
set(CPACK_ubuntu_GENERATOR "DEB")
set(CPACK_centos_GENERATOR "RPM")

macro(get_distr_generator VAR)
  set(${VAR} ${CPACK_${CMAKE_DISTRO_NAME}_GENERATOR})
endmacro()

