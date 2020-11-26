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

#include "continuous_aggs/materialize.h"
#include "bgw_policy/chunk_stats.h"

/* Reorder function type. Necessary for testing */
typedef void (*reorder_func)(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
							 Oid destination_tablespace, Oid index_tablespace);

/* Functions exposed only for testing */
extern bool policy_reorder_execute(int32 job_id, Jsonb *config);
extern bool policy_retention_execute(int32 job_id, Jsonb *config);
extern bool policy_refresh_cagg_execute(int32 job_id, Jsonb *config);
extern bool policy_compression_execute(int32 job_id, Jsonb *config);
extern void policy_reorder_read_config(int32 job_id, Jsonb *config, Hypertable **hypertable,
									   Oid *index_relid);
extern void policy_retention_read_config(int32 job_id, Jsonb *config, Oid *object_relid,
										 Datum *boundary, Datum *boundary_type);
extern void policy_refresh_cagg_read_config(int32 job_id, Jsonb *config,
											InternalTimeRange *refresh_window,
											ContinuousAgg **cagg);
extern void policy_compression_read_config(int32 job_id, Jsonb *config, bool show_notice,
										   Dimension **dim, int32 *chunk_id);
extern bool job_execute(BgwJob *job);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_JOB_H */
