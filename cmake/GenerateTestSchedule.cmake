# generate_test_schedul(<output file> ...)
#
# A test schedule is generated for the files in TEST_FILES. The test schedule
# groups the tests into groups of size GROUP_SIZE, with the exceptions of any
# tests mentioned in the list SOLO, which will be in their own test group.
#
# TEST_FILES <file> ...
#
# Test files to generate a test schedule for.
#
# SOLO <test> ...
#
# Names of tests that should be run as solo tests. Note that these are test
# names, not file names.
#
# GROUP_SIZE
#
# Size of each group in the test schedule.
function(generate_test_schedule OUTPUT_FILE)
  set(options)
  set(oneValueArgs GROUP_SIZE)
  set(multiValueArgs TEST_FILES SOLO)
  cmake_parse_arguments(_generate "${options}" "${oneValueArgs}"
                        "${multiValueArgs}" ${ARGN})

  # Depending on the configuration we might end up
  # with an empty schedule here. On older cmake versions < 3.14
  # trying to sort an empty list will produce an error, so
  # we check for empty list here.
  if(_generate_TEST_FILES)
    list(SORT _generate_TEST_FILES)
  endif()
  file(REMOVE ${OUTPUT_FILE})

  # We put the solo tests in the test file first. Note that we do not generate
  # groups for solo tests that are not in the list of test files.
  foreach(_solo ${_generate_SOLO})
    if("${_solo}.sql" IN_LIST _generate_TEST_FILES)
      file(APPEND ${OUTPUT_FILE} "test: ${_solo}\n")
    endif()
  endforeach()

  # Generate groups of tests
  set(_members 0)
  foreach(_file ${_generate_TEST_FILES})
    string(REGEX REPLACE "(.+)\.sql" "\\1" _test ${_file})
    if(NOT (_test IN_LIST _generate_SOLO))
      if(_members EQUAL 0)
        file(APPEND ${OUTPUT_FILE} "test: ")
      endif()
      file(APPEND ${OUTPUT_FILE} "${_test} ")
      if(_members LESS _generate_GROUP_SIZE)
        math(EXPR _members "${_members} + 1")
      else()
        set(_members 0)
        file(APPEND ${OUTPUT_FILE} "\n")
      endif()
    endif()
  endforeach()
  file(APPEND ${OUTPUT_FILE} "\n")
endfunction()
