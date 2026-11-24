/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/timestamp.h>

/* User-facing API functions */
extern Datum policy_compaction_add(PG_FUNCTION_ARGS);
extern Datum policy_compaction_remove(PG_FUNCTION_ARGS);
extern Datum policy_compaction_check(PG_FUNCTION_ARGS);

Datum policy_compaction_add_internal(Oid ht_oid, bool if_not_exists,
									 Interval *user_schedule_interval, TimestampTz initial_start,
									 bool fixed_schedule, text *timezone, int32 max_chunks,
									 int32 max_batches, Interval *inactive_for);
bool policy_compaction_remove_internal(Oid hypertable_oid, bool if_exists);
