set(EXT_SQL_FILE ${PROJECT_NAME}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_FILE ${PROJECT_NAME}--${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_PRE_FILE updates/pre-${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)
set(EXT_SQL_UPDATE_POST_FILE updates/post-${UPDATE_FROM_VERSION}--${PROJECT_VERSION_MOD}.sql)

set(SQL_FILES
  schemas.sql
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
  tablespace.sql
  bookend.sql
  time_bucket.sql
  version.sql
  cache_functions.sql
  size_utils.sql
  histogram.sql
  cache.sql)

set(EXT_SQL_EXTRA_FILES
  timescaledb--0.1.0--0.2.0.sql
  timescaledb--0.2.0--0.3.0.sql
  timescaledb--0.3.0--0.4.0.sql
  timescaledb--0.4.0--0.4.1.sql
  timescaledb--0.4.1--0.4.2.sql
  timescaledb--0.4.2--0.5.0.sql
  timescaledb--0.5.0--0.6.0.sql
  timescaledb--0.6.0--0.6.1.sql
  timescaledb--0.6.1--0.7.0.sql
  timescaledb--0.6.1--0.7.1.sql
  timescaledb--0.7.0--0.7.1.sql
  timescaledb--0.7.1--0.8.0.sql)

if (WIN32)
  # Make list of files into string of files separated by "+"
  # to make Windows copy concatenate them
  string(REPLACE ";" ";+" SQL_FILES_JOINED "${SQL_FILES}")
  # Windows copy command requires backslashes for relative paths
  string(REPLACE "/" "\\" EXT_SQL_UPDATE_PRE_FILE "${EXT_SQL_UPDATE_PRE_FILE}")
  string(REPLACE "/" "\\" EXT_SQL_UPDATE_POST_FILE "${EXT_SQL_UPDATE_POST_FILE}")

  set(CAT_SQL_FILE_CMD copy /B /y ${SQL_FILES_JOINED} "\"${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE}\"" >NUL)
  set(CAT_SQL_UPDATE_FILE_CMD copy /B /y ${EXT_SQL_UPDATE_PRE_FILE} + ${SQL_FILES_JOINED} + ${EXT_SQL_UPDATE_POST_FILE} "\"${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}\"" >NUL)
else ()
  set(CAT_SQL_FILE_CMD cat ${SQL_FILES} > ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE})
  set(CAT_SQL_UPDATE_FILE_CMD cat ${EXT_SQL_UPDATE_PRE_FILE} ${SQL_FILES} ${EXT_SQL_UPDATE_POST_FILE} > ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE})
endif ()

# Command and target for the SQL file
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE}
  DEPENDS ${SQL_FILES}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CAT_SQL_FILE_CMD}
  COMMENT "Generating ${EXT_SQL_FILE}")

add_custom_target(sqlfile ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE})

# Command and target for the update SQL file
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}
  DEPENDS ${SQL_FILES} ${EXT_SQL_UPDATE_PRE_FILE} ${EXT_SQL_UPDATE_POST_FILE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CAT_SQL_UPDATE_FILE_CMD}
  COMMENT "Generating ${EXT_SQL_UPDATE_FILE}")

add_custom_target(sqlupdatefile ALL DEPENDS ${EXT_SQL_UPDATE_FILE})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_FILE}
  ${CMAKE_CURRENT_BINARY_DIR}/${EXT_SQL_UPDATE_FILE}
  ${EXT_SQL_EXTRA_FILES}
  DESTINATION "${PG_SHAREDIR}/extension")
