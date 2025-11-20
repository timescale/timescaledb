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

extern char const *bloom1_column_prefix;

/*
 * We used to have two possible hashes depending on the build configuration,
 * which were incompatible with each other. They both used the "bloom1" column
 * prefix. This could lead to false negatives if a database was updated to a
 * different build of the TimescaleDB extension. Now these hashing configuration
 * use different prefixes. The bloom filter is still constructed according to
 * the "bloom1" rules.
 */
#ifdef TS_USE_UMASH
#define default_bloom1_column_prefix "bloomh"
#else
#define default_bloom1_column_prefix "bloomg"
#endif
