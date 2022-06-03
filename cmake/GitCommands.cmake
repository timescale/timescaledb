# Git helper functions and macros

# git_get_versioned(VERSION <version> FILES <path> ...)
#
# Get files by name relative to the current source directory but read it from
# branch VERSION. Note that the version referenced has to exists as a branch,
# otherwise you get an error.
#
# VERSION <version>
#
# Version to read files from. Any object name is accepted as given in
# gitrevision(7), but typically you should use a tag name.
#
# FILES <path> ...
#
# Paths to extract. These are relative or absolute paths to files to extract. If
# relative, the path names are resolved relative the current source directory.
#
# RESULT_FILES <var>
#
# Variable for the list of the full paths of the retrieved files.
#
# IGNORE_ERRORS
#
# Ignore errors when fetching file from previous version. This is currently used
# to ignore missing files, but we should probably be more selective in the
# following versions.
function(git_versioned_get)
  set(options IGNORE_ERRORS)
  set(oneValueArgs VERSION RESULT_FILES)
  set(multiValueArgs FILES)
  cmake_parse_arguments(_git_get "${options}" "${oneValueArgs}"
                        "${multiValueArgs}" ${ARGN})

  execute_process(
    COMMAND git show-ref --verify --quiet refs/tags/${_git_get_VERSION}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE _version_missing)
  if(_version_missing)
    message(
      FATAL_ERROR "Version ${_git_get_VERSION} does not exist in repository.")
  endif()

  set(_result_files)
  foreach(_file ${_git_get_FILES})
    # Build full path, if the path is not absolute.
    if(IS_ABSOLUTE ${_file})
      set(_path "${_file}")
    else()
      set(_path "${CMAKE_CURRENT_SOURCE_DIR}/${_file}")
    endif()

    # Remove root source directory to get path relative to source root. This is
    # necessary for git-show to work correctly and then use that to generate a
    # full path for the version.  (Yeah, we could use ./ before, but this is
    # less clear in the log what file is actually fetched.)
    string(REPLACE ${CMAKE_SOURCE_DIR}/ "" _relpath ${_path})
    set(_fullpath "${CMAKE_BINARY_DIR}/v${_git_get_VERSION}/${_relpath}")
    get_filename_component(_dirname ${_fullpath} DIRECTORY)
    file(MAKE_DIRECTORY ${_dirname})

    # We place the output file next to the final file to avoid cross-device
    # renames but give it a different name since it is created even if the
    # command fails.
    execute_process(
      COMMAND ${GIT_EXECUTABLE} show ${_git_get_VERSION}:${_relpath}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      OUTPUT_FILE "${_fullpath}.part"
      RESULT_VARIABLE _error
      ERROR_VARIABLE _message)

    if(_error)
      if(_git_get_IGNORE_ERRORS)
        string(STRIP "${_message}" _stripped_message)
        message(STATUS "${_stripped_message} (ignored).")
      else()
        message(FATAL_ERROR "${_message}")
      endif()
    else()
      file(RENAME "${_fullpath}.part" "${_fullpath}")
      list(APPEND _result_files "${_fullpath}")
    endif()
  endforeach()
  set(${_git_get_RESULT_FILES}
      ${_result_files}
      PARENT_SCOPE)
endfunction()
