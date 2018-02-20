set(EXT_SQL_FILE ${PROJECT_NAME}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_FILE ${PROJECT_NAME}--${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_PRE_FILE updates/pre-${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_POST_FILE updates/post-${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)

#These files called before anything else
set(PRE_UPDATE_FILES
  updates/pre-global.sql
  #trigger functions need to go first to associate them with new .so before they are called
  trigger_functions.sql
)

set(PRE_INSTALL_FILES
  schemas.sql
  trigger_functions.sql
)

set(COMMON_FILES
  tables.sql
  permissions.sql
  chunk.sql
  ddl_internal.sql
  util_time.sql
  util_internal_table_ddl.sql
  chunk_constraint.sql
  partitioning.sql
  schema_info.sql
  ddl_api.sql
  ddl_triggers.sql
  bookend.sql
  time_bucket.sql
  version.sql
  cache_functions.sql
  size_utils.sql
  histogram.sql
  cache.sql
)

set(EXT_SQL_EXTRA_FILES
  timescaledb--0.1.0--0.2.0.sql
  timescaledb--0.2.0--0.3.0.sql
  timescaledb--0.3.0--0.4.0.sql
  timescaledb--0.4.0--0.4.1.sql
  timescaledb--0.4.1--0.4.2.sql
  timescaledb--0.4.2--0.5.0.sql
  timescaledb--0.5.0--0.6.0.sql
  timescaledb--0.6.0--0.6.1.sql
  timescaledb--0.6.1--0.7.1.sql
  timescaledb--0.7.1--0.8.0.sql
)

#TODO handle these manually.
set(EXT_SQL_SPECIAL_FILES
    timescaledb--0.6.1--0.7.0.sql
    timescaledb--0.7.0--0.7.1.sql
)

set(SQL_FILES ${PRE_INSTALL_FILES} ${COMMON_FILES})
set(UPDATE_FILE_LIST ${PRE_UPDATE_FILES} ${EXT_SQL_UPDATE_PRE_FILE} ${COMMON_FILES} ${EXT_SQL_UPDATE_POST_FILE})

#replace MODULE PATHNAME for files making up EXT_SQL_FILE (timescaledb--version.sql) and EXT_SQL_UPDATE_FILE (timescaledb--previous_version--version.sql)
set(MODULE_PATHNAME "$libdir/timescaledb-${PROJECT_VERSION_MOD}")

function(version_files SRC_FILE_LIST OUTPUT_FILE_LIST)
  set(result "")
  foreach(unversioned_file ${SRC_FILE_LIST})
    set(versioned_file ${unversioned_file}.${PROJECT_VERSION_MOD})
    configure_file(${unversioned_file} ${versioned_file} @ONLY)
    list(APPEND result ${CMAKE_CURRENT_BINARY_DIR}/${versioned_file})
  endforeach(unversioned_file)
  set(${OUTPUT_FILE_LIST} "${result}" PARENT_SCOPE)
endfunction()

version_files("${UPDATE_FILE_LIST}" UPDATE_FILE_LIST_VERSIONED)
version_files("${SQL_FILES}" SQL_FILES_VERSIONED)

#function to concatenate all files in SRC_FILE_LIST into file OUTPUT_FILE
function(cat_files SRC_FILE_LIST OUTPUT_FILE)
  if (WIN32)
    # Make list of files into string of files separated by "+"
    # to make Windows copy concatenate them

    file(TO_NATIVE_PATH "${SRC_FILE_LIST}" SRC_FILE_LIST_NATIVE)
    string(REPLACE ";" ";+" SQL_LIST_JOINED "${SRC_FILE_LIST_NATIVE}")

    file(TO_NATIVE_PATH "${OUTPUT_FILE}" OUTPUT_FILE_NATIVE)

    set(CAT_CMD copy /B /y ${SQL_LIST_JOINED} "\"${OUTPUT_FILE_NATIVE}\"" >NUL)
  else ()
    set(CAT_CMD cat ${SRC_FILE_LIST} > ${OUTPUT_FILE})
  endif ()
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE_LIST}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${CAT_CMD}
    COMMENT "Generating ${OUTPUT_FILE}"
  )
endfunction()

cat_files("${SQL_FILES_VERSIONED}" ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE})
add_custom_target(sqlfile ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE})

cat_files("${UPDATE_FILE_LIST_VERSIONED}" ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE})
add_custom_target(sqlupdatefile ALL DEPENDS  ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE})

#This generates the update file without MODULE_PATHNAME replaced. This file should be copied for the release.
cat_files("${UPDATE_FILE_LIST}" ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}.release)
add_custom_target(sqlreleaseupdate ALL DEPENDS  ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}.release)

#You want to create timescaledb-old_version--current_version for all possible old_versions.
#This is done for 2 reasons:
# 1) Each of the update files should point to $libdir/timescaledb-current_version. Since you cannot guarantee that
#    Previous .so for each intermediate version has been installed.
# 2) You don't want intermediate version updates installed without the .so. For example, if you have versions 1,2,3
#    and you are installing version 3, you want the upgrade files 1--3, 2--3 but not 1--2 because if you have 1--2
#    then a user could do ALTER EXTENSION timescaledb UPDATE TO 2. But the .so for version 2 may not be installed.
#
# Note that we build these update files by concatenating intermediate files. So we should have the intermediate update
# files of 1--2, 2--3. From these we build the actual update scripts 1--3, 2--3.

#replace MODULE_PATHNAME in all the intermediate update files to point to $libdir/timescaledb-current_version
#note that intermediate update files are never installed/packaged. Rather, they are only used to build the
#update scripts below.
set(INTERMEDIATE_UPDATE_VERSIONED "")
foreach(intermediate_update_file ${EXT_SQL_EXTRA_FILES})
  set(intermediate_update_file_versioned ${intermediate_update_file}.${PROJECT_VERSION_MOD})
  configure_file(${intermediate_update_file} ${intermediate_update_file_versioned} @ONLY)
  list(APPEND INTERMEDIATE_UPDATE_VERSIONED ${CMAKE_CURRENT_BINARY_DIR}/${intermediate_update_file_versioned})
endforeach(intermediate_update_file)

#we expect INTERMEDIATE_UPDATE_VERSIONED to be in order. For example, it should be 1--2, 2--3.
set(INTERMEDIATE_FILE_REGEX "${CMAKE_CURRENT_BINARY_DIR}/timescaledb--([0-9]+\\.[0-9]+\\.*[0-9]*)--([0-9]+\\.[0-9]+\\.*[0-9]*).sql.${PROJECT_VERSION_MOD}")
#build the actual update scripts for historic versions.
set(PREVIOUS_UPDATE_SCRIPTS "")
while(INTERMEDIATE_UPDATE_VERSIONED) #while list not empty
  #start with the first (oldest) file in the list and create an update for that version to current version
  list(GET INTERMEDIATE_UPDATE_VERSIONED 0 FIRST_INTERMEDIATE_FILE)
  if (NOT (${FIRST_INTERMEDIATE_FILE} MATCHES ${INTERMEDIATE_FILE_REGEX}))
    message(FATAL_ERROR "Cannot parse update file name ${FIRST_INTERMEDIATE_FILE}")
  endif ()

  set(START_VERSION ${CMAKE_MATCH_1})
  set(PREV_UPDATE_FILE "${PROJECT_NAME}--${START_VERSION}--${PROJECT_VERSION_MOD}.sql")

  #the files that go into this update are all the intermediate files + latest update
  set(UPDATE_FILES ${INTERMEDIATE_UPDATE_VERSIONED} ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE})

  cat_files("${UPDATE_FILES}" ${CMAKE_CURRENT_BINARY_DIR}/${PREV_UPDATE_FILE})
  list(APPEND PREVIOUS_UPDATE_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/${PREV_UPDATE_FILE})

  #drop the head and start again
  list(REMOVE_AT INTERMEDIATE_UPDATE_VERSIONED 0)
endwhile(INTERMEDIATE_UPDATE_VERSIONED)

add_custom_target(sqlprevupdatescripts ALL DEPENDS ${PREVIOUS_UPDATE_SCRIPTS})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE}
  ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}
  ${PREVIOUS_UPDATE_SCRIPTS}
  DESTINATION "${PG_SHAREDIR}/extension")
