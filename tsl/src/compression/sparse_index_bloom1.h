/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "postgres.h"

Datum bloom1_contains(PG_FUNCTION_ARGS);

Datum bloom1_contains_any(PG_FUNCTION_ARGS);

PGFunction bloom1_get_hash_function(Oid type, FmgrInfo **finfo);
