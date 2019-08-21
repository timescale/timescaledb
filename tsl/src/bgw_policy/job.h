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
extern bool execute_reorder_policy(BgwJob *job, reorder_func reorder, bool fast_continue);
extern bool execute_drop_chunks_policy(int32 job_id);
extern bool execute_compress_chunks_policy(BgwJob *job);
extern bool tsl_bgw_policy_job_execute(BgwJob *job);
extern Datum bgw_policy_alter_job_schedule(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_JOB_H */
