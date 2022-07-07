/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <utils/jsonb.h>
#include "dimension.h"
#include <continuous_aggs/materialize.h>
#include <bgw_policy/compression_api.h>
#include <bgw_policy/continuous_aggregate_api.h>
#include <bgw_policy/retention_api.h>

#define POLICY_REFRESH_CAGG_PROC_NAME "policy_refresh_continuous_aggregate"
#define POLICY_COMPRESSION_PROC_NAME "policy_compression"
#define POLICY_RETENTION_PROC_NAME "policy_retention"
#define CONFIG_KEY_MAT_HYPERTABLE_ID "mat_hypertable_id"
#define CONFIG_KEY_START_OFFSET "start_offset"
#define CONFIG_KEY_END_OFFSET "end_offset"
#define CONFIG_KEY_COMPRESS_AFTER "compress_after"
#define CONFIG_KEY_DROP_AFTER "drop_after"

#define SHOW_POLICY_KEY_HYPERTABLE_ID "hypertable_id"
#define SHOW_POLICY_KEY_POLICY_NAME "policy_name"
#define SHOW_POLICY_KEY_REFRESH_INTERVAL "refresh_interval"
#define SHOW_POLICY_KEY_REFRESH_START_OFFSET "refresh_start_offset"
#define SHOW_POLICY_KEY_REFRESH_END_OFFSET "refresh_end_offset"
#define SHOW_POLICY_KEY_COMPRESS_AFTER CONFIG_KEY_COMPRESS_AFTER
#define SHOW_POLICY_KEY_COMPRESS_INTERVAL "compress_interval"
#define SHOW_POLICY_KEY_DROP_AFTER CONFIG_KEY_DROP_AFTER
#define SHOW_POLICY_KEY_RETENTION_INTERVAL "retention_interval"

#define DEFAULT_RETENTION_SCHEDULE_INTERVAL {.day = 1}
/*
 * Default scheduled interval for compress jobs = default chunk length.
 * If this is non-timestamp based hypertable, then default is 1 day
 */
#define DEFAULT_COMPRESSION_SCHEDULE_INTERVAL                                                                  \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("1 day"), InvalidOid, -1))

#define DEFAULT_REFRESH_SCHEDULE_INTERVAL                                                                  \
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
}refresh_policy;

typedef struct compression_policy
{
	Datum compress_after;
	Oid compress_after_type;
	bool create_policy;
}compression_policy;

typedef struct retention_policy
{
	Datum drop_after;
	Oid drop_after_type;
	bool create_policy;
}retention_policy;

typedef struct policies_info
{
	Oid rel_oid;
	Oid original_HT;
	Oid partition_type;
	refresh_policy *refresh;
	compression_policy *compress;
	retention_policy *retention;
	bool is_alter_policy;
}policies_info;

bool ts_if_offset_is_infinity(Datum arg, Oid argtype, Oid timetype);
bool valdiate_and_create_policies(policies_info all_policies, bool if_exists);
int64 interval_to_int64(Datum interval, Oid type);
int64 offset_to_int64(NullableDatum arg, Oid argtype, Oid partition_type, bool is_start);