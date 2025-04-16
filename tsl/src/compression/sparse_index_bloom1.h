/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "postgres.h"

Datum tsl_bloom1_matches(PG_FUNCTION_ARGS);

PGFunction bloom1_get_hash_function(Oid type);
