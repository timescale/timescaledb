/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CROSS_MODULE_FN_H
#define TIMESCALEDB_CROSS_MODULE_FN_H

#include <postgres.h>
#include <c.h>
#include <postgres.h>
#include <fmgr.h>
#include <commands/event_trigger.h>

#include <utils/timestamp.h>
#include <utils/jsonb.h>

#include <optimizer/planner.h>

#include "export.h"
#include "bgw/job.h"
#include "process_utility.h"

/*
 * To define a cross-module function add it to this struct, add a default
 * version in to ts_cm_functions_default cross_module_fn.c, and the overridden
 * version to tsl_cm_functions tsl/src/init.c.
 * This will allow the function to be called from this codebase as
 *     ts_cm_functions-><function name>
 */

typedef struct CrossModuleFunctions
{
	void (*tsl_license_on_assign)(const char *newval, const void *license);
	bool (*enterprise_enabled_internal)(void);
	bool (*check_tsl_loaded)(void);
	TimestampTz (*license_end_time)(void);
	void (*print_tsl_license_expiration_info_hook)(void);
	void (*module_shutdown_hook)(void);
	void (*add_tsl_license_info_telemetry)(JsonbParseState *parseState);
	bool (*bgw_policy_job_execute)(BgwJob *job);
	Datum (*add_drop_chunks_policy)(PG_FUNCTION_ARGS);
	Datum (*add_reorder_policy)(PG_FUNCTION_ARGS);
	Datum (*remove_drop_chunks_policy)(PG_FUNCTION_ARGS);
	Datum (*remove_reorder_policy)(PG_FUNCTION_ARGS);
	void (*create_upper_paths_hook)(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *);
	PGFunction gapfill_marker;
	PGFunction gapfill_int16_time_bucket;
	PGFunction gapfill_int32_time_bucket;
	PGFunction gapfill_int64_time_bucket;
	PGFunction gapfill_date_time_bucket;
	PGFunction gapfill_timestamp_time_bucket;
	PGFunction gapfill_timestamptz_time_bucket;
	PGFunction alter_job_schedule;
	PGFunction reorder_chunk;
	void (*ddl_command_start)(ProcessUtilityArgs *args);
	void (*ddl_command_end)(EventTriggerData *command);
	void (*sql_drop)(List *dropped_objects);
	PGFunction partialize_agg;
	PGFunction finalize_agg_sfunc;
	PGFunction finalize_agg_ffunc;
	bool (*process_cagg_viewstmt)(ViewStmt *stmt, const char *query_string, void *pstmt);
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif /* TIMESCALEDB_CROSS_MODULE_FN_H */
