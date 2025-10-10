/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "postgres.h"

Datum bloom1_contains(PG_FUNCTION_ARGS);

PGFunction bloom1_get_hash_function(Oid type, FmgrInfo **finfo);

/*
 * Confusingly, the column name prefix is now "bloom2" because we had to disable
 * the old bloom filter index globally on cloud because of broken cloud builds
 * leading to wrong hashing.
 */
#define BLOOM1_COLUMN_PREFIX "bloom2"
