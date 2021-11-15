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

#include "cache.h"
#include "continuous_aggs/materialize.h"
#include "bgw_policy/chunk_stats.h"

/* Add config keys common across job types here */
#define CONFIG_KEY_VERBOSE_LOG "verbose_log" /*used only by compression now*/

typedef struct PolicyReorderData
{
	Hypertable *hypertable;
	Oid index_relid;
} PolicyReorderData;

typedef struct PolicyRetentionData
{
	Oid object_relid;
	Datum boundary;
	Datum boundary_type;
} PolicyRetentionData;

typedef struct PolicyContinuousAggData
{
	InternalTimeRange refresh_window;
	ContinuousAgg *cagg;
} PolicyContinuousAggData;

typedef struct PolicyCompressionData
{
	Hypertable *hypertable;
	Cache *hcache;
} PolicyCompressionData;

/* Reorder function type. Necessary for testing */
typedef void (*reorder_func)(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
							 Oid destination_tablespace, Oid index_tablespace);

/* Functions exposed only for testing */
extern bool policy_reorder_execute(int32 job_id, Jsonb *config);
extern bool policy_retention_execute(int32 job_id, Jsonb *config);
extern bool policy_refresh_cagg_execute(int32 job_id, Jsonb *config);
extern bool policy_recompression_execute(int32 job_id, Jsonb *config);
extern void policy_reorder_read_and_validate_config(Jsonb *config, PolicyReorderData *policy_data);
extern void policy_retention_read_and_validate_config(Jsonb *config,
													  PolicyRetentionData *policy_data);
extern void policy_refresh_cagg_read_and_validate_config(Jsonb *config,
														 PolicyContinuousAggData *policy_data);
extern void policy_compression_read_and_validate_config(Jsonb *config,
														PolicyCompressionData *policy_data);
extern void policy_recompression_read_and_validate_config(Jsonb *config,
														  PolicyCompressionData *policy_data);
extern bool job_execute(BgwJob *job);
extern void job_config_check(Name proc_schema, Name proc_name, Jsonb *config);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_JOB_H */
