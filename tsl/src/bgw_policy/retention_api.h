/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

/* User-facing API functions */
extern Datum policy_retention_add(PG_FUNCTION_ARGS);
extern Datum policy_retention_proc(PG_FUNCTION_ARGS);
extern Datum policy_retention_check(PG_FUNCTION_ARGS);
extern Datum policy_retention_remove(PG_FUNCTION_ARGS);

int64 policy_retention_get_drop_after_int(const Jsonb *config);
Interval *policy_retention_get_drop_after_interval(const Jsonb *config);
Interval *policy_retention_get_drop_created_before_interval(const Jsonb *config);

Datum policy_retention_add_internal(Oid ht_oid, Oid window_type, Datum window_datum,
									Interval *created_before, Interval default_schedule_interval,
									bool if_not_exists, bool fixed_schedule,
									TimestampTz initial_start, const char *timezone);
Datum policy_retention_remove_internal(Oid table_oid, bool if_exists);
