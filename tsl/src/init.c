/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#include <postgres.h>
#include <fmgr.h>

#include <export.h>
#include <cross_module_fn.h>
#include "planner.h"
#include "time_bucket.h"
#include "gapfill/gapfill.h"

#include "license.h"
#include "reorder.h"
#include "telemetry.h"
#include "bgw_policy/job.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/drop_chunks_api.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static void tsl_module_shutdown(void);
static bool enterprise_enabled_internal(void);
static bool check_tsl_loaded(void);

/*
 * Cross module function initialization.
 *
 * During module start we set ts_cm_functions to point at the tsl version of the
 * function registry.
 *
 * NOTE: To ensure that your cross-module function has a correct default, you
 * must also add it to ts_cm_functions_default in cross_module_fn.c in the
 * Apache codebase.
 */
CrossModuleFunctions tsl_cm_functions = {
	.tsl_license_on_assign = tsl_license_on_assign,
	.enterprise_enabled_internal = enterprise_enabled_internal,
	.check_tsl_loaded = check_tsl_loaded,
	.tsl_module_shutdown = tsl_module_shutdown,
	.add_tsl_license_info_telemetry = tsl_telemetry_add_license_info,
	.bgw_policy_job_execute = tsl_bgw_policy_job_execute,
	.add_drop_chunks_policy = drop_chunks_add_policy,
	.add_reorder_policy = reorder_add_policy,
	.remove_drop_chunks_policy = drop_chunks_remove_policy,
	.remove_reorder_policy = reorder_remove_policy,
	.create_upper_paths_hook = tsl_create_upper_paths_hook,
	.gapfill_marker = gapfill_marker,
	.gapfill_int16_time_bucket = ts_int16_bucket,
	.gapfill_int32_time_bucket = ts_int32_bucket,
	.gapfill_int64_time_bucket = ts_int64_bucket,
	.gapfill_date_time_bucket = ts_date_bucket,
	.gapfill_timestamp_time_bucket = ts_timestamp_bucket,
	.gapfill_timestamptz_time_bucket = ts_timestamptz_bucket,
	.alter_policy_schedule = bgw_policy_alter_policy_schedule,
	.reorder_chunk = tsl_reorder_chunk,
};

TS_FUNCTION_INFO_V1(ts_module_init);
/*
 * Module init function, sets ts_cm_functions to point at tsl_cm_functions
 */
PGDLLEXPORT Datum
ts_module_init(PG_FUNCTION_ARGS)
{
	elog(WARNING, "starting TimescaleDB code that requires the Timescale License");

	ts_cm_functions = &tsl_cm_functions;

	PG_RETURN_BOOL(true);
}

/*
 * Currently we disallow shutting down this submodule in a live session,
 * but if we did, this would be the function we'd use.
 */
static void
tsl_module_shutdown(void)
{
	elog(WARNING, "shutting down timescaledb TSL library");
	ts_cm_functions = &ts_cm_functions_default;
}

/* Informative functions */

static bool
enterprise_enabled_internal(void)
{
	return license_enterprise_enabled();
}

static bool
check_tsl_loaded(void)
{
	return true;
}
