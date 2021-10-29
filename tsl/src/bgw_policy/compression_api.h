/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_BGW_POLICY_COMPRESSION_API_H
#define TIMESCALEDB_TSL_BGW_POLICY_COMPRESSION_API_H

#include <postgres.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

/* User-facing API functions */
extern Datum policy_compression_add(PG_FUNCTION_ARGS);
extern Datum policy_compression_remove(PG_FUNCTION_ARGS);

extern Datum policy_recompression_proc(PG_FUNCTION_ARGS);

int32 policy_compression_get_hypertable_id(const Jsonb *config);
int64 policy_compression_get_compress_after_int(const Jsonb *config);
Interval *policy_compression_get_compress_after_interval(const Jsonb *config);
bool policy_compression_get_recompress(const Jsonb *config);
int32 policy_compression_get_maxchunks_per_job(const Jsonb *config);
int64 policy_recompression_get_recompress_after_int(const Jsonb *config);
Interval *policy_recompression_get_recompress_after_interval(const Jsonb *config);
bool policy_compression_get_verbose_log(const Jsonb *config);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_COMPRESSION_API_H */
