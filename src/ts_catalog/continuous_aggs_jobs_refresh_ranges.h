/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "export.h"

extern TSDLLEXPORT void ts_cagg_jobs_refresh_ranges_insert(int32 materialization_id,
														   int64 start_range, int64 end_range,
														   int32 job_id, int32 pid);
extern TSDLLEXPORT void
ts_cagg_jobs_refresh_ranges_delete_by_materialization_id(int32 materialization_id);
extern TSDLLEXPORT void ts_cagg_jobs_refresh_ranges_delete_by_pid(int32 materialization_id,
																  int32 pid);
