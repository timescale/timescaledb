/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_JOB_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_JOB_H
#include <postgres.h>
#include <c.h>

#include "bgw/job.h"

int32 ts_continuous_agg_job_add(int32 raw_table_id, int64 bucket_width, Interval *refresh_interval);

int32 ts_continuous_agg_job_find_materializtion_by_job_id(int32 job_id);

#endif
