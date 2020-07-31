/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "catalog.h"
#include <postgres.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include <hypertable_cache.h>
#include <scan_iterator.h>
#include <jsonb_utils.h>

#include "bgw/job.h"
#include "continuous_aggs/job.h"

#define POLICY_CAGG_PROC_NAME "policy_continuous_aggregate"
#define CONFIG_KEY_MAT_HYPERTABLE_ID "mat_hypertable_id"

/* DEFAULT_SCHEDULE_INTERVAL 12 hours */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("12 hours"), InvalidOid, -1))
/* Default max runtime for a continuous aggregate jobs is unlimited for now */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))
/* Right now, there is an infinite number of retries for continuous aggregate jobs */
#define DEFAULT_MAX_RETRIES -1

/* Get the default schedule interval for a new continuous aggregate.
 *
 * Default scheduled interval for continuous aggregate jobs should be 1/2 of the default
 * chunk length if the length is specified and a time time. Otherwise it is
 * DEFAULT_SCHEDULE_INTERVAL
 */
static Interval *
continuous_agg_job_get_default_schedule_interval(int32 raw_table_id, int64 bucket_width)
{
	Dimension *dim;
	Interval *default_schedule_interval = DEFAULT_SCHEDULE_INTERVAL;
	Hypertable *ht = ts_hypertable_get_by_id(raw_table_id);
	Oid partition_type;

	Assert(ht != NULL);

	/*
	 * Try to see if the hypertable has a specified chunk length for the
	 * default schedule interval
	 */
	dim = hyperspace_get_open_dimension(ht->space, 0);

	partition_type = ts_dimension_get_partition_type(dim);
	if (dim != NULL && IS_TIMESTAMP_TYPE(partition_type))
	{
		default_schedule_interval = DatumGetIntervalP(
			DirectFunctionCall7(make_interval,
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Float8GetDatum((bucket_width * 2) / USECS_PER_SEC)));
	}

	return default_schedule_interval;
}

int32
ts_continuous_agg_job_add(int32 mat_table_id, int32 raw_table_id, int64 bucket_width)
{
	NameData application_name;
	NameData job_type;
	NameData proc_name, proc_schema, owner;
	int32 job_id;

	namestrcpy(&job_type, "continuous_aggregate");
	namestrcpy(&application_name, "Continuous Aggregate Policy");

	Interval *refresh_interval =
		continuous_agg_job_get_default_schedule_interval(raw_table_id, bucket_width);

	namestrcpy(&proc_name, POLICY_CAGG_PROC_NAME);
	namestrcpy(&proc_schema, INTERNAL_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(GetUserId(), false));

	JsonbParseState *parse_state = NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, CONFIG_KEY_MAT_HYPERTABLE_ID, mat_table_id);
	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&job_type,
										refresh_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										refresh_interval,
										&proc_name,
										&proc_schema,
										&owner,
										true,
										mat_table_id,
										config);

	return job_id;
}
