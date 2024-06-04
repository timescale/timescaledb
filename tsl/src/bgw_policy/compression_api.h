/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

/* User-facing API functions */
extern Datum policy_compression_add(PG_FUNCTION_ARGS);
extern Datum policy_compression_remove(PG_FUNCTION_ARGS);

extern Datum policy_recompression_proc(PG_FUNCTION_ARGS);
extern Datum policy_compression_check(PG_FUNCTION_ARGS);

int32 policy_compression_get_hypertable_id(const Jsonb *config);
int32 policy_compression_get_maxchunks_per_job(const Jsonb *config);
int64 policy_recompression_get_recompress_after_int(const Jsonb *config);
Interval *policy_recompression_get_recompress_after_interval(const Jsonb *config);

Datum policy_compression_add_internal(Oid user_rel_oid, Datum compress_after_datum,
									  Oid compress_after_type, Interval *created_before,
									  Interval *default_schedule_interval,
									  bool user_defined_schedule_interval, bool if_not_exists,
									  bool fixed_schedule, TimestampTz initial_start,
									  const char *timezone, const char *compress_using);
bool policy_compression_remove_internal(Oid user_rel_oid, bool if_exists);
