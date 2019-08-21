/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_BGW_POLICY_COMPRESS_CHUNKS_H
#define TIMESCALEDB_BGW_POLICY_COMPRESS_CHUNKS_H

#include "catalog.h"
#include "export.h"

typedef struct BgwPolicyCompressChunks
{
	FormData_bgw_policy_compress_chunks fd;
} BgwPolicyCompressChunks;

extern TSDLLEXPORT BgwPolicyCompressChunks *ts_bgw_policy_compress_chunks_find_by_job(int32 job_id);
extern TSDLLEXPORT BgwPolicyCompressChunks *
ts_bgw_policy_compress_chunks_find_by_hypertable(int32 hypertable_id);
extern TSDLLEXPORT void ts_bgw_policy_compress_chunks_insert(BgwPolicyCompressChunks *policy);
extern TSDLLEXPORT bool ts_bgw_policy_compress_chunks_delete(int32 job_id);
#endif /* TIMESCALEDB_BGW_POLICY_COMPRESS_CHUNKS_H */
