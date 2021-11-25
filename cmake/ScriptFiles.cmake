# File defining all variables used to generate script files.
#
# This is needed for the downgrade script since files can be added and removed
# and it is necessary to get a list of all files available for a specific
# version.
#
# We only care about files that are part of generating the prolog or epilog for
# the update scripts, to the actual versioned files are not necessary to put
# here.

# Source files that define the schemas and tables for our metadata
set(PRE_INSTALL_SOURCE_FILES
    pre_install/schemas.sql # Must be first
    pre_install/types.pre.sql
    pre_install/types.functions.sql
    pre_install/types.post.sql # Must be before tables.sql
    pre_install/tables.sql
    pre_install/cache.sql
    pre_install/insert_data.sql
    pre_install/fdw_functions.sql
    pre_install/timescaledb_fdw.sql)

# Source files that define functions and need to be rerun in update
set(PRE_INSTALL_FUNCTION_FILES
    pre_install/types.functions.sql
    pre_install/fdw_functions.sql)

# The rest of the source files defining mostly functions
set(SOURCE_FILES
    hypertable.sql
    chunk.sql
    data_node.sql
    ddl_internal.sql
    ddl_experimental.sql
    util_time.sql
    util_internal_table_ddl.sql
    chunk_constraint.sql
    hypertable_constraint.sql
    partitioning.sql
    schema_info.sql
    ddl_api.sql
    ddl_triggers.sql
    bookend.sql
    time_bucket.sql
    time_bucket_ng.sql
    version.sql
    size_utils.sql
    histogram.sql
    bgw_scheduler.sql
    metadata.sql
    dist_internal.sql
    views.sql
    views_experimental.sql
    gapfill.sql
    maintenance_utils.sql
    partialize_finalize.sql
    restoring.sql
    job_api.sql
    policy_api.sql
    policy_internal.sql
    cagg_utils.sql)

if(USE_TELEMETRY)
  list(APPEND SOURCE_FILES with_telemetry.sql)
else()
  list(APPEND SOURCE_FILES without_telemetry.sql)
endif()

# These files need to be last in the scripts.
list(APPEND SOURCE_FILES 
  bgw_startup.sql)

# These files should be pre-pended to update scripts so that they are executed
# before anything else during updates
set(PRE_UPDATE_FILES updates/pre-update.sql)

# The POST_UPDATE_FILES should be executed as the last part of the update
# script. sets state for executing POST_UPDATE_FILES during ALTER EXTENSION
set(SET_POST_UPDATE_STAGE updates/set_post_update_stage.sql)
set(UNSET_UPDATE_STAGE updates/unset_update_stage.sql)
set(POST_UPDATE_FILES updates/post-update.sql)
