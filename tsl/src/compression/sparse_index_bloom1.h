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
 * Confusingly, the column name prefix is now "bl2". We had two possible
 * hashes depending on the build configuration, which were incompatible with
 * each other. This led to false negatives if a database was updated to a
 * different build of extension. Now these also have different prefixes.
 * The bloom filter is still constructed according to the "bloom1" rules.
 */
#ifdef TS_USE_UMASH
#define BLOOM1_COLUMN_PREFIX "bl2u"
#else
#define BLOOM1_COLUMN_PREFIX "bl2p"
#endif
