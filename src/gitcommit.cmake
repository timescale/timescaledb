# The commands for generating gitcommit.h need to be executed on every make run
# and not on cmake run to detect branch switches, commit changes or local
# modifications.

if(GIT_FOUND)
  # We use "git describe" to generate the tag. It will find the latest tag and
  # also add some additional information if we are not on the tag.
  execute_process(
    COMMAND ${GIT_EXECUTABLE} describe --dirty --always --tags
    WORKING_DIRECTORY ${SOURCE_DIR}
    OUTPUT_VARIABLE EXT_GIT_COMMIT_TAG
    RESULT_VARIABLE _describe_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Fetch the commit HASH of head (short version) using rev-parse
  execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
    OUTPUT_VARIABLE EXT_GIT_COMMIT_HASH
    WORKING_DIRECTORY ${SOURCE_DIR}
    RESULT_VARIABLE _revparse_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Fetch the date of the head commit
  execute_process(
    COMMAND ${GIT_EXECUTABLE} log -1 --format=%cI
    WORKING_DIRECTORY ${SOURCE_DIR}
    OUTPUT_VARIABLE EXT_GIT_COMMIT_TIME
    RESULT_VARIABLE _log_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Results are non-zero if there were an error
  if(_describe_RESULT
     OR _revparse_RESULT
     OR _log_RESULT)
    message(STATUS "Unable to get git commit information")
  endif()
endif()

file(REMOVE ${OUTPUT_FILE})
configure_file(${INPUT_FILE} ${OUTPUT_FILE})
