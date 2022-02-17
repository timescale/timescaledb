set(INSTALL_FILE ${PROJECT_NAME}--${PROJECT_VERSION_MOD}.sql)

include(ScriptFiles)

# These files represent the modifications that happen in each version, excluding
# new objects or updates to functions. We use them to build a path (update
# script) from every historical version to the current version. Note that not
# all of these files may exist on disk, in case they would have no contents.
# There still needs to be an entry here to build an update script for that
# version. Thus, for every new release, an entry should be added here.
set(MOD_FILES
    updates/1.7.0--1.7.1.sql
    updates/1.7.1--1.7.2.sql
    updates/1.7.2--1.7.3.sql
    updates/1.7.3--1.7.4.sql
    updates/1.7.4--1.7.5.sql
    updates/1.7.5--2.0.0-rc1.sql
    updates/2.0.0-rc1--2.0.0-rc2.sql
    updates/2.0.0-rc2--2.0.0-rc3.sql
    updates/2.0.0-rc3--2.0.0-rc4.sql
    updates/2.0.0-rc4--2.0.0.sql
    updates/2.0.0--2.0.1.sql
    updates/2.0.1--2.0.2.sql
    updates/2.0.2--2.1.0.sql
    updates/2.1.0--2.1.1.sql
    updates/2.1.1--2.2.0.sql
    updates/2.2.0--2.2.1.sql
    updates/2.2.1--2.3.0.sql
    updates/2.3.0--2.3.1.sql
    updates/2.3.1--2.4.0.sql
    updates/2.4.0--2.4.1.sql
    updates/2.4.1--2.4.2.sql
    updates/2.4.2--2.5.0.sql
    updates/2.5.0--2.5.1.sql
    updates/2.5.1--2.5.2.sql
    updates/2.5.2--2.6.0.sql)

# The downgrade file to generate a downgrade script for the current version, as
# specified in version.config
set(CURRENT_REV_FILE reverse-dev.sql)
# Files for generating old downgrade scripts. This should only include files for
# downgrade to from one version to its previous version since we do not support
# skipping versions when downgrading.
set(OLD_REV_FILES
    2.3.1--2.3.0.sql
    2.4.0--2.3.1.sql
    2.4.1--2.4.0.sql
    2.4.2--2.4.1.sql
    2.5.0--2.4.2.sql
    2.5.1--2.5.0.sql
    2.5.2--2.5.1.sql
    2.6.0--2.5.2.sql)

set(MODULE_PATHNAME "$libdir/timescaledb-${PROJECT_VERSION_MOD}")
set(LOADER_PATHNAME "$libdir/timescaledb")

set(TS_MODULE_PATHNAME
    ${MODULE_PATHNAME}
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
    ${DOWNGRADE_TO_VERSION}
    INPUT_DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/updates
    FILES
    ${CURRENT_REV_FILE})
  if(GENERATE_OLD_DOWNGRADE_SCRIPTS)
    foreach(_downgrade_file ${OLD_REV_FILES})
      if(${_downgrade_file} MATCHES
         "([0-9]+\\.[0-9]+\\.*[0-9]*)--([0-9]+\\.[0-9]+\\.*[0-9]*).sql")
        set(_source_version ${CMAKE_MATCH_1})
        set(_target_version ${CMAKE_MATCH_2})
        generate_downgrade_script(
          SOURCE_VERSION
          ${_source_version}
          TARGET_VERSION
          ${_target_version}
          INPUT_DIRECTORY
          ${CMAKE_CURRENT_SOURCE_DIR}/updates
          FILES
          ${_downgrade_file})
      else()
        message(FATAL_ERROR "${_downgrade_file} is not a downgrade file")
      endif()
    endforeach()
  endif()
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
   "${CMAKE_CURRENT_SOURCE_DIR}/updates/${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql"
)
  set(MOD_FILES_LIST ${MOD_FILES_VERSIONED})
else()
  set(MOD_FILES_LIST
      "${MOD_FILES_VERSIONED};updates/${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql"
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
    message(FATAL_ERROR "Cannot parse update file name ${mod_file}")
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
