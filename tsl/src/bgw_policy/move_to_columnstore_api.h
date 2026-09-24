/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/timestamp.h>

#include "hypertable.h"

/* User-facing API functions */
extern Datum policy_move_to_columnstore_add(PG_FUNCTION_ARGS);
extern Datum policy_move_to_columnstore_remove(PG_FUNCTION_ARGS);
extern Datum policy_move_to_columnstore_check(PG_FUNCTION_ARGS);

Datum policy_move_to_columnstore_add_internal(Oid user_rel_oid, Interval *default_schedule_interval,
											  bool if_not_exists, bool fixed_schedule,
											  TimestampTz initial_start, const char *timezone,
											  int32 max_chunks, bool allow_blocking_compression);
bool policy_move_to_columnstore_remove_internal(Oid hypertable_oid, bool if_exists);
