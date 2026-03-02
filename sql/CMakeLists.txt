set(INSTALL_FILE ${PROJECT_NAME}--${PROJECT_VERSION_MOD}.sql)

include(ScriptFiles)

# These files represent the modifications that happen in each version, excluding
# new objects or updates to functions. We use them to build a path (update
# script) from every historical version to the current version. Note that not
# all of these files may exist on disk, in case they would have no contents.
# There still needs to be an entry here to build an update script for that
# version. Thus, for every new release, an entry should be added here.
set(MOD_FILES
    updates/2.9.0--2.9.1.sql
    updates/2.9.1--2.9.2.sql
    updates/2.9.2--2.9.3.sql
    updates/2.9.3--2.10.0.sql
    updates/2.10.0--2.10.1.sql
    updates/2.10.1--2.10.2.sql
    updates/2.10.2--2.10.3.sql
    updates/2.10.3--2.11.0.sql
    updates/2.11.0--2.11.1.sql
    updates/2.11.1--2.11.2.sql
    updates/2.11.2--2.12.0.sql
    updates/2.12.0--2.12.1.sql
    updates/2.12.1--2.12.2.sql
    updates/2.12.2--2.13.0.sql
    updates/2.13.0--2.13.1.sql
    updates/2.13.1--2.14.0.sql
    updates/2.14.0--2.14.1.sql
    updates/2.14.1--2.14.2.sql
    updates/2.14.2--2.15.0.sql
    updates/2.15.0--2.15.1.sql
    updates/2.15.1--2.15.2.sql
    updates/2.15.2--2.15.3.sql
    updates/2.15.3--2.16.0.sql
    updates/2.16.0--2.16.1.sql
    updates/2.16.1--2.17.0.sql
    updates/2.17.0--2.17.1.sql
    updates/2.17.1--2.17.2.sql
    updates/2.17.2--2.18.0.sql
    updates/2.18.0--2.18.1.sql
    updates/2.18.1--2.18.2.sql
    updates/2.18.2--2.19.0.sql
    updates/2.19.0--2.19.1.sql
    updates/2.19.1--2.19.2.sql
    updates/2.19.2--2.19.3.sql
    updates/2.19.3--2.20.0.sql
    updates/2.20.0--2.20.1.sql
    updates/2.20.1--2.20.2.sql
    updates/2.20.2--2.20.3.sql
    updates/2.20.3--2.21.0.sql
    updates/2.21.0--2.21.1.sql
    updates/2.21.1--2.21.2.sql
    updates/2.21.2--2.21.3.sql
    updates/2.21.3--2.21.4.sql
    updates/2.21.4--2.22.0.sql
    updates/2.22.0--2.22.1.sql
    updates/2.22.1--2.23.0.sql
    updates/2.23.0--2.23.1.sql
    updates/2.23.1--2.24.0.sql
    updates/2.24.0--2.25.0.sql
    updates/2.25.0--2.25.1.sql)

# The downgrade file to generate a downgrade script for the current version, as
# specified in version.config
set(CURRENT_REV_FILE reverse-dev.sql)

set(MODULE_PATHNAME "$libdir/timescaledb-${PROJECT_VERSION_MOD}")
set(LOADER_PATHNAME "$libdir/timescaledb")

set(TS_MODULE_PATHNAME
    ${MODULE_PATHNAME}
    PARENT_SCOPE)

set(TSL_MODULE_PATHNAME
    "$libdir/timescaledb-tsl-${PROJECT_VERSION_MOD}"
    PARENT_SCOPE)

if(NOT GENERATE_DOWNGRADE_SCRIPT)
  message(
    STATUS "Not generating downgrade script: downgrade generation disabled.")
elseif(NOT GIT_FOUND)
  message(STATUS "Not generating downgrade script: Git not available.")
else()
  generate_downgrade_script(
    SOURCE_VERSION
    ${PROJECT_VERSION_MOD}
    TARGET_VERSION
    ${PREVIOUS_VERSION}
    INPUT_DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/updates
    FILES
    ${CURRENT_REV_FILE})
endif()

# Function to replace @MODULE_PATHNAME@ in source files, producing an output
# file in the build dir
function(version_files SRC_FILE_LIST OUTPUT_FILE_LIST)
  set(result "")
  foreach(unversioned_file ${SRC_FILE_LIST})
    set(versioned_file ${unversioned_file})
    list(APPEND result ${CMAKE_CURRENT_BINARY_DIR}/${versioned_file})
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${unversioned_file})
      configure_file(${unversioned_file} ${versioned_file} @ONLY)
    endif()
  endforeach(unversioned_file)
  set(${OUTPUT_FILE_LIST}
      "${result}"
      PARENT_SCOPE)
endfunction()

# Function to replace @VARIABLE@ in source files, producing an output file in
# the build dir.  Unlike the previous function we need to build a versioned file
# for every version we create an update script for.
function(version_check_file SRC_FILE_LIST OUTPUT_FILE_LIST)
  set(result "")
  foreach(unversioned_file ${SRC_FILE_LIST})
    set(versioned_file ${unversioned_file}-${START_VERSION})
    list(APPEND result ${CMAKE_CURRENT_BINARY_DIR}/${versioned_file})
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${unversioned_file})
      configure_file(${unversioned_file} ${versioned_file} @ONLY)
    endif()
  endforeach(unversioned_file)
  set(${OUTPUT_FILE_LIST}
      "${result}"
      PARENT_SCOPE)
endfunction()

# Create versioned files (replacing MODULE_PATHNAME) in the build directory of
# all our source files
version_files("${PRE_UPDATE_FILES}" PRE_UPDATE_FILES_VERSIONED)
version_files("${POST_UPDATE_FILES}" POST_UPDATE_FILES_VERSIONED)
version_files("${PRE_INSTALL_SOURCE_FILES}" PRE_INSTALL_SOURCE_FILES_VERSIONED)
version_files("${PRE_INSTALL_FUNCTION_FILES}"
              PRE_INSTALL_FUNCTION_FILES_VERSIONED)
version_files("${SOURCE_FILES}" SOURCE_FILES_VERSIONED)
version_files("${MOD_FILES}" MOD_FILES_VERSIONED)
version_files("updates/latest-dev.sql" LASTEST_MOD_VERSIONED)
version_files("notice.sql" NOTICE_FILE)

# Function to concatenate all files in SRC_FILE_LIST into file OUTPUT_FILE
function(cat_files SRC_FILE_LIST OUTPUT_FILE)
  if(WIN32)
    set("SRC_ARG" "-DSRC_FILE_LIST=${SRC_FILE_LIST}")
  else()
    set("SRC_ARG" "'-DSRC_FILE_LIST=${SRC_FILE_LIST}'")
  endif()
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE_LIST}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} "${SRC_ARG}" "-DOUTPUT_FILE=${OUTPUT_FILE}" -P
            cat.cmake
    COMMENT "Generating ${OUTPUT_FILE}")
endfunction()

# Concatenate all files in SRC_FILE_LIST into file OUTPUT_FILE Additionally
# strips out `OR REPLACE` from the generated file as this would allow a
# privilege escalation attack when present in the CREATE script
function(cat_create_files SRC_FILE_LIST OUTPUT_FILE)
  if(WIN32)
    set("SRC_ARG" "-DSRC_FILE_LIST=${SRC_FILE_LIST}")
  else()
    set("SRC_ARG" "'-DSRC_FILE_LIST=${SRC_FILE_LIST}'")
  endif()
  if(WIN32)
    add_custom_command(
      OUTPUT ${OUTPUT_FILE}
      DEPENDS ${SRC_FILE_LIST}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMAND ${CMAKE_COMMAND} "${SRC_ARG}" "-DOUTPUT_FILE=${OUTPUT_FILE}"
              "-DSTRIP_REPLACE=ON" -P cat.cmake
      COMMENT "Generating ${OUTPUT_FILE}")
  else()
    add_custom_command(
      OUTPUT ${OUTPUT_FILE}
      DEPENDS ${SRC_FILE_LIST}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMAND ${CMAKE_COMMAND} "'-DSRC_FILE_LIST=${SRC_FILE_LIST}'"
              "-DOUTPUT_FILE=${OUTPUT_FILE}" "-DSTRIP_REPLACE=ON" -P cat.cmake
      COMMENT "Generating ${OUTPUT_FILE}")
  endif()
endfunction()

# Generate the extension file used with CREATE EXTENSION
cat_create_files(
  "${PRE_INSTALL_SOURCE_FILES_VERSIONED};${SOURCE_FILES_VERSIONED};${NOTICE_FILE}"
  ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE})
add_custom_target(sqlfile ALL
                  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE})

# Generate the update files used with ALTER EXTENSION <name> UPDATE
set(MOD_FILE_REGEX
    "([0-9]+\\.[0-9]+\\.*[0-9]+[-a-z0-9]*)--([0-9]+\\.[0-9]+\\.*[0-9]+[-a-z0-9]*).sql"
)

# We'd like to process the updates in reverse (descending) order
if(EXISTS
   "${CMAKE_CURRENT_SOURCE_DIR}/updates/${PREVIOUS_VERSION}--${PROJECT_VERSION_MOD}.sql"
)
  set(MOD_FILES_LIST ${MOD_FILES_VERSIONED})
else()
  set(MOD_FILES_LIST
      "${MOD_FILES_VERSIONED};updates/${PREVIOUS_VERSION}--${PROJECT_VERSION_MOD}.sql"
  )
endif()

list(REVERSE MOD_FILES_LIST)

# Variable that will hold the list of update scripts from every previous version
# to the current version
set(UPDATE_SCRIPTS "")

# A list of current modfiles. We append to this list for every previous version
# that moves us further away from the current version, thus making the update
# path longer as we move back in history
set(CURR_MOD_FILES "${LASTEST_MOD_VERSIONED}")

# Now loop through the modfiles and generate the update files
foreach(transition_mod_file ${MOD_FILES_LIST})

  if(NOT (${transition_mod_file} MATCHES ${MOD_FILE_REGEX}))
    message(FATAL_ERROR "Cannot parse update file name ${transition_mod_file}")
  endif()

  set(START_VERSION ${CMAKE_MATCH_1})
  set(END_VERSION ${CMAKE_MATCH_2})
  set(PRE_FILES ${PRE_UPDATE_FILES_VERSIONED})
  set(POST_FILES_PROCESSED ${POST_UPDATE_FILES_VERSIONED}.processed)
  cat_files(
    "${SET_POST_UPDATE_STAGE};${POST_UPDATE_FILES_VERSIONED};${UNSET_UPDATE_STAGE}"
    ${POST_FILES_PROCESSED})
  # Check for version-specific update code with fixes
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/updates/${START_VERSION}.sql)
    version_files("updates/${START_VERSION}.sql" ORIGIN_MOD_FILE)
    list(APPEND PRE_FILES ${ORIGIN_MOD_FILE})
  endif()

  version_check_file("updates/version_check.sql" VERSION_CHECK_VERSIONED)
  list(PREPEND PRE_FILES "header.sql;${VERSION_CHECK_VERSIONED}")

  # There might not have been any changes in the modfile, in which case the
  # modfile need not be present
  if(EXISTS ${transition_mod_file})
    # Prepend the modfile as we are moving through the versions in descending
    # order
    list(INSERT CURR_MOD_FILES 0 ${transition_mod_file})
  endif()

  set(UPDATE_SCRIPT
      ${CMAKE_CURRENT_BINARY_DIR}/timescaledb--${START_VERSION}--${PROJECT_VERSION_MOD}.sql
  )
  list(APPEND UPDATE_SCRIPTS ${UPDATE_SCRIPT})
  if(CURR_MOD_FILES)
    cat_files(
      "${PRE_FILES};${CURR_MOD_FILES};${PRE_INSTALL_FUNCTION_FILES_VERSIONED};${SOURCE_FILES_VERSIONED};${POST_FILES_PROCESSED}"
      ${UPDATE_SCRIPT})
  else()
    cat_files(
      "${PRE_FILES};${PRE_INSTALL_FUNCTION_FILES_VERSIONED};${SOURCE_FILES_VERSIONED};${POST_FILES_PROCESSED}"
      ${UPDATE_SCRIPT})
  endif()

endforeach(transition_mod_file)

add_custom_target(sqlupdatescripts ALL DEPENDS ${UPDATE_SCRIPTS})

# Install target for the extension file and update scripts
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE} ${UPDATE_SCRIPTS}
        DESTINATION "${PG_SHAREDIR}/extension")
