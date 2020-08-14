/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_BGW_POLICY_JOB_H
#define TIMESCALEDB_TSL_BGW_POLICY_JOB_H

#include <c.h>

#include <bgw/job.h>
#include <hypertable.h>

#include "bgw_policy/chunk_stats.h"

/* Reorder function type. Necessary for testing */
typedef void (*reorder_func)(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
							 Oid destination_tablespace, Oid index_tablespace);

/* Functions exposed only for testing */
extern bool policy_reorder_execute(int32 job_id, Jsonb *config);
extern bool policy_retention_execute(int32 job_id, Jsonb *config);
extern bool policy_refresh_cagg_execute(int32 job_id, Jsonb *config);
extern bool policy_compression_execute(int32 job_id, Jsonb *config);
extern bool job_execute(BgwJob *job);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_JOB_H */
