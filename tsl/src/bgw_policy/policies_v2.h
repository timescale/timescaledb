/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "dimension.h"
#include <bgw_policy/compression_api.h>
#include <bgw_policy/continuous_aggregate_api.h>
#include <bgw_policy/retention_api.h>
#include <continuous_aggs/materialize.h>
#include <utils/jsonb.h>

#define POLICY_REFRESH_CAGG_PROC_NAME "policy_refresh_continuous_aggregate"
#define POLICY_REFRESH_CAGG_CHECK_NAME "policy_refresh_continuous_aggregate_check"
#define POL_REFRESH_CONF_KEY_MAT_HYPERTABLE_ID "mat_hypertable_id"
#define POL_REFRESH_CONF_KEY_START_OFFSET "start_offset"
#define POL_REFRESH_CONF_KEY_END_OFFSET "end_offset"

#define POLICY_COMPRESSION_PROC_NAME "policy_compression"
#define POLICY_COMPRESSION_CHECK_NAME "policy_compression_check"
#define POL_COMPRESSION_CONF_KEY_HYPERTABLE_ID "hypertable_id"
#define POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER "compress_after"
#define POL_COMPRESSION_CONF_KEY_MAXCHUNKS_TO_COMPRESS "maxchunks_to_compress"
#define POL_COMPRESSION_CONF_KEY_COMPRESS_CREATED_BEFORE "compress_created_before"
#define POL_COMPRESSION_CONF_KEY_COMPRESS_USING "compress_using"

#define POLICY_RECOMPRESSION_PROC_NAME "policy_recompression"
#define POL_RECOMPRESSION_CONF_KEY_RECOMPRESS_AFTER "recompress_after"

#define POLICY_RETENTION_PROC_NAME "policy_retention"
#define POLICY_RETENTION_CHECK_NAME "policy_retention_check"
#define POL_RETENTION_CONF_KEY_HYPERTABLE_ID "hypertable_id"
#define POL_RETENTION_CONF_KEY_DROP_AFTER "drop_after"
#define POL_RETENTION_CONF_KEY_DROP_CREATED_BEFORE "drop_created_before"

#define SHOW_POLICY_KEY_HYPERTABLE_ID "hypertable_id"
#define SHOW_POLICY_KEY_POLICY_NAME "policy_name"
#define SHOW_POLICY_KEY_REFRESH_INTERVAL "refresh_interval"
#define SHOW_POLICY_KEY_REFRESH_START_OFFSET "refresh_start_offset"
#define SHOW_POLICY_KEY_REFRESH_END_OFFSET "refresh_end_offset"
#define SHOW_POLICY_KEY_COMPRESS_AFTER POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER
#define SHOW_POLICY_KEY_COMPRESS_CREATED_BEFORE POL_COMPRESSION_CONF_KEY_COMPRESS_CREATED_BEFORE
#define SHOW_POLICY_KEY_COMPRESS_INTERVAL "compress_interval"
#define SHOW_POLICY_KEY_DROP_AFTER POL_RETENTION_CONF_KEY_DROP_AFTER
#define SHOW_POLICY_KEY_DROP_CREATED_BEFORE POL_RETENTION_CONF_KEY_DROP_CREATED_BEFORE
#define SHOW_POLICY_KEY_RETENTION_INTERVAL "retention_interval"

#define DEFAULT_RETENTION_SCHEDULE_INTERVAL                                                        \
	{                                                                                              \
		.day = 1                                                                                   \
	}
/*
 * Default scheduled interval for compress jobs = default chunk length.
 * If this is non-timestamp based hypertable, then default is 1 day
 */
#define DEFAULT_COMPRESSION_SCHEDULE_INTERVAL                                                      \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("1 day"), InvalidOid, -1))

#define DEFAULT_REFRESH_SCHEDULE_INTERVAL                                                          \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("1 hour"), InvalidOid, -1))

extern Datum policies_add(PG_FUNCTION_ARGS);
extern Datum policies_remove(PG_FUNCTION_ARGS);
extern Datum policies_remove_all(PG_FUNCTION_ARGS);
extern Datum policies_alter(PG_FUNCTION_ARGS);
extern Datum policies_show(PG_FUNCTION_ARGS);

typedef struct CaggPolicyConfig
{
	Oid partition_type;
	CaggPolicyOffset offset_start;
	CaggPolicyOffset offset_end;
} CaggPolicyConfig;

typedef struct refresh_policy
{
	Interval schedule_interval;
	NullableDatum start_offset;
	NullableDatum end_offset;
	Oid start_offset_type, end_offset_type;
	bool create_policy;
} refresh_policy;

typedef struct compression_policy
{
	Datum compress_after;
	Oid compress_after_type;
	bool create_policy;
	const char *compress_using;
} compression_policy;

typedef struct retention_policy
{
	Datum drop_after;
	Oid drop_after_type;
	bool create_policy;
} retention_policy;

typedef struct policies_info
{
	Oid rel_oid;
	Oid original_HT;
	Oid partition_type;
	refresh_policy *refresh;
	compression_policy *compress;
	retention_policy *retention;
	bool is_alter_policy;
} policies_info;

bool ts_if_offset_is_infinity(Datum arg, Oid argtype, bool is_start);
bool validate_and_create_policies(policies_info all_policies, bool if_exists);
int64 interval_to_int64(Datum interval, Oid type);
