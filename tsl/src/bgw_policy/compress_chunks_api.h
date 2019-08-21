/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_BGW_POLICY_COMPRESS_CHUNKS_API_H
#define TIMESCALEDB_TSL_BGW_POLICY_COMPRESS_CHUNKS_API_H

#include <postgres.h>

/* User-facing API functions */
extern Datum compress_chunks_add_policy(PG_FUNCTION_ARGS);
extern Datum compress_chunks_remove_policy(PG_FUNCTION_ARGS);
#endif /* TIMESCALEDB_TSL_BGW_POLICY_COMPRESS_CHUNKS_API_H */
