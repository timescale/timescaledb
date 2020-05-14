# CMake targets for generating code coverage reports
#
# Note that the targets in here are not needed to enable code coverage
# on builds. To have builds generate code coverage metadata, one only
# has to enable the --coverage option for the compiler and this is
# done in the top-level CMakeLists.txt so that the option covers all
# build targets in the project.
#
# Given that all dependencies (lcov, genhtml) are installed, and CMake
# is initialized with -DCODECOVERAGE=ON, it should be
# possible to generate a code coverage report by running:
#
# $ make coverage
#
# The report is generated in REPORT_DIR and can be viewed in a web
# browser.

# Find lcov for html output
find_program(LCOV lcov)

if (LCOV)
  # Final tracefile for code coverage
  set(OUTPUT_FILE "timescaledb-codecov.info")

  # Directory where to generate the HTML report
  set(REPORT_DIR "codecov-report")


  # The baseline run needs to run before tests to learn what zero
  # coverage looks like
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}.base
    COMMENT "Generating code coverage base file"
    COMMAND ${LCOV}
    --capture
    --initial # Initial run
    --no-external # Do not include external source files
    --base-directory ${CMAKE_SOURCE_DIR}
    --directory ${CMAKE_BINARY_DIR}
    --output-file ${OUTPUT_FILE}.base
    DEPENDS timescaledb-tsl timescaledb timescaledb-loader)

  add_custom_target(coverage_base
    DEPENDS
    timescaledb-tsl
    timescaledb
    timescaledb-loader
    ${OUTPUT_FILE}.base)

  # Ensure baseline file is generated before tests
  add_dependencies(installcheck coverage_base)

  # The test run needs to run after tests to analyze the test coverage
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}.test
    COMMENT "Generating code coverage test file"
    COMMAND ${LCOV}
    --capture
    --no-external
    --base-directory ${CMAKE_SOURCE_DIR}
    --directory ${CMAKE_BINARY_DIR}
    --output-file ${OUTPUT_FILE}.test
    DEPENDS ${OUTPUT_FILE}.base coverage_base)

  # Make sure coverage_test runs after tests (installcheck) finish
  add_custom_target(coverage_test DEPENDS ${OUTPUT_FILE}.test)
  add_dependencies(installcheck-post-hook coverage_test)

  # Generate the final coverage file by combining the pre and post
  # test tracefiles
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMENT "Generating final code coverage file"
    COMMAND ${LCOV}
    --add-tracefile ${OUTPUT_FILE}.base
    --add-tracefile ${OUTPUT_FILE}.test
    --output-file ${OUTPUT_FILE}
    DEPENDS ${OUTPUT_FILE}.test coverage_test)

  add_custom_target(coverage_final DEPENDS ${OUTPUT_FILE})
  add_dependencies(coverage_final coverage_test)

  # Look for genhtml to produce HTML report. This tool is part of the
  # lcov suite, so should be installed if lcov is installed. Thus,
  # this is an over-cautious check just in case some distributions
  # break these tools up in separate packages.
  find_program(GENHTML genhtml)

  if (GENHTML)
    message(STATUS "Generate a code coverage report using the 'coverage' target after tests have run.")

    add_custom_command(
      OUTPUT ${REPORT_DIR}/index.html
      COMMENT "Generating HTML code coverage report in ${CMAKE_CURRENT_BINARY_DIR}/${REPORT_DIR}"
      COMMAND ${GENHTML}
      --prefix ${CMAKE_SOURCE_DIR}
      --ignore-errors source
      --legend
      --title "TimescaleDB"
      --output-directory ${REPORT_DIR}
      ${OUTPUT_FILE}
      DEPENDS ${OUTPUT_FILE})
    add_custom_target(coverage DEPENDS ${REPORT_DIR}/index.html)
    add_dependencies(coverage coverage_final)

    add_custom_command(
      TARGET coverage
      POST_BUILD
      COMMENT "Open file://${CMAKE_CURRENT_BINARY_DIR}/${REPORT_DIR}/index.html in a browser to view the report")
  else ()
    message(STATUS "Install genhtml to generate code coverage reports")
  endif (GENHTML)
else ()
  message(STATUS "Install lcov to generate code coverage reports")
endif (LCOV)
