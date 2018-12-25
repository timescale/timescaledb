/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_CROSS_MODULE_FN_H
#define TIMESCALEDB_CROSS_MODULE_FN_H

#include <postgres.h>
#include <c.h>
#include <postgres.h>
#include <fmgr.h>

#include <utils/timestamp.h>
#include <utils/jsonb.h>

#include <optimizer/planner.h>

#include "export.h"
#include "bgw/job.h"

/*
 * To define a cross-module function add it to this struct, add a default
 * version in to ts_cm_functions_default cross_module_fn.c, and the overriden
 * version to tsl_cm_functions tsl/src/init.c.
 * This will allow the function to be called from this codebase as
 *     ts_cm_functions-><function name>
 */

typedef struct CrossModuleFunctions
{
	void		(*tsl_license_on_assign) (const char *newval, const void *license);
	bool		(*enterprise_enabled_internal) (void);
	bool		(*check_tsl_loaded) (void);
	void		(*module_shutdown) (void);
	void		(*add_tsl_license_info_telemetry) (JsonbParseState *parseState);
	bool		(*bgw_policy_job_execute) (BgwJob *job);
	Datum		(*add_drop_chunks_policy) (PG_FUNCTION_ARGS);
	Datum		(*add_reorder_policy) (PG_FUNCTION_ARGS);
	Datum		(*remove_drop_chunks_policy) (PG_FUNCTION_ARGS);
	Datum		(*remove_reorder_policy) (PG_FUNCTION_ARGS);
	void		(*create_upper_paths_hook) (PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *);
	PGFunction	gapfill_marker;
	PGFunction	gapfill_int16_time_bucket;
	PGFunction	gapfill_int32_time_bucket;
	PGFunction	gapfill_int64_time_bucket;
	PGFunction	gapfill_date_time_bucket;
	PGFunction	gapfill_timestamp_time_bucket;
	PGFunction	gapfill_timestamptz_time_bucket;
	PGFunction	alter_policy_schedule;
	PGFunction	reorder_chunk;
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif							/* TIMESCALEDB_CROSS_MODULE_FN_H */
