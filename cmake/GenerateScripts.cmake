# Functions to generate downgrade scripts

# generate_downgrade_script(<options>)
#
# Create a downgrade script from a source version to a target version. The
# ScriptFiles.cmake manifest is read from the target version's git tag to
# determine which files make up the prolog and epilog.
#
# SOURCE_VERSION <version>
#   Version to generate downgrade script from.
#
# TARGET_VERSION <version>
#   Version to generate downgrade script to.
#
# OUTPUT_DIRECTORY <dir>
#   Output directory for script file. Defaults to CMAKE_CURRENT_BINARY_DIR.
#
# INPUT_DIRECTORY <dir>
#   Input directory for downgrade files. Defaults to CMAKE_CURRENT_SOURCE_DIR.
#
# FILES <file> ...
#   Downgrade-specific files to include between the prolog and epilog.
#
# The output script concatenates:
# 1. Prolog from the target version (header.sql, pre-version-change.sql)
# 2. Downgrade files from the source version
# 3. Epilog from the target version (function defs, source files, post-update)
#
# All files undergo @VARIABLE@ template substitution with MODULE_PATHNAME set
# to the target version's shared library path.
function(generate_downgrade_script)
  set(options)
  set(oneValueArgs SOURCE_VERSION TARGET_VERSION OUTPUT_DIRECTORY
                   INPUT_DIRECTORY FILES)
  cmake_parse_arguments(_downgrade "${options}" "${oneValueArgs}"
                        "${multiValueArgs}" ${ARGN})

  if(NOT _downgrade_OUTPUT_DIRECTORY)
    set(_downgrade_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  endif()

  if(NOT _downgrade_INPUT_DIRECTORY)
    set(_downgrade_INPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  endif()

  foreach(_downgrade_file ${_downgrade_FILES})
    if(NOT EXISTS ${_downgrade_INPUT_DIRECTORY}/${_downgrade_file})
      message(FATAL_ERROR "No downgrade file ${_downgrade_file} found!")
    endif()
  endforeach()

  # Fetch manifest with list of files for the prolog and epilog from the target
  # version.
  git_versioned_get(VERSION ${_downgrade_TARGET_VERSION} FILES
                    ${CMAKE_SOURCE_DIR}/cmake/ScriptFiles.cmake)

  # This will include the variables in this scope, but not in the parent scope
  # so we can read them locally without affecting the parent scope.
  include(
    ${CMAKE_BINARY_DIR}/v${_downgrade_TARGET_VERSION}/cmake/ScriptFiles.cmake)

  set(_downgrade_PRE_FILES "header.sql;${PRE_DOWNGRADE_FILES}")
  set(_downgrade_POST_FILES "${PRE_INSTALL_FUNCTION_FILES};${SOURCE_FILES}" ${SET_POST_UPDATE_STAGE}
                            ${POST_UPDATE_FILES} ${UNSET_UPDATE_STAGE})

  # Fetch epilog from target version.
  git_versioned_get(
    VERSION
    ${_downgrade_TARGET_VERSION}
    FILES
    ${_downgrade_POST_FILES}
    RESULT_FILES
    _epilog_files
    IGNORE_ERRORS)

  # Assemble the full file list: prolog + downgrade files + epilog
  set(_files)
  foreach(_downgrade_file ${_downgrade_PRE_FILES})
    get_filename_component(_downgrade_filename ${_downgrade_file} NAME)
    configure_file(${_downgrade_file} ${_downgrade_INPUT_DIRECTORY}/${_downgrade_filename} COPYONLY)
    list(APPEND _files ${_downgrade_INPUT_DIRECTORY}/${_downgrade_filename})
  endforeach()
  foreach(_downgrade_file ${_downgrade_FILES})
    list(APPEND _files ${_downgrade_INPUT_DIRECTORY}/${_downgrade_file})
  endforeach()
  list(APPEND _files ${_epilog_files})

  # Set template variables for the target version
  set(MODULE_PATHNAME "$libdir/timescaledb-${_downgrade_TARGET_VERSION}")
  set(LOADER_PATHNAME "$libdir/timescaledb")
  set(PROJECT_VERSION_MOD ${PREVIOUS_VERSION})

  # Process all template files and concatenate into the output script
  set(_script timescaledb--${_downgrade_SOURCE_VERSION}--${_downgrade_TARGET_VERSION}.sql)
  set(_output ${_downgrade_OUTPUT_DIRECTORY}/${_script})
  message(STATUS "Generating script ${_script}")

  file(WRITE ${_output} "")
  foreach(_file ${_files})
    configure_file(${_file} ${_file}.gen @ONLY)
    file(READ ${_file}.gen _contents)
    file(APPEND ${_output} "${_contents}")
  endforeach()

  install(FILES ${_output} DESTINATION "${PG_SHAREDIR}/extension")
endfunction()
