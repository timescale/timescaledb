/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

/* User-facing API functions */
extern Datum policy_reorder_add(PG_FUNCTION_ARGS);
extern Datum policy_reorder_remove(PG_FUNCTION_ARGS);
extern Datum policy_reorder_proc(PG_FUNCTION_ARGS);
extern Datum policy_reorder_check(PG_FUNCTION_ARGS);

extern char *policy_reorder_get_index_name(const Jsonb *config);
