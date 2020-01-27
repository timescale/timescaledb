/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <export.h>

#include <continuous_aggs/materialize.h>
#include <hypertable_cache.h>
#include <utils.h>
#include <time_bucket.h>
#include <continuous_agg.h>

TS_FUNCTION_INFO_V1(ts_run_continuous_agg_materialization);

/* test function to run a continuous aggregate materialization manually */
Datum
ts_run_continuous_agg_materialization(PG_FUNCTION_ARGS)
{
	Oid hypertable = PG_GETARG_OID(0);
	Oid materialization_table = PG_GETARG_OID(1);
	Name partial_view_name = PG_GETARG_NAME(2);
	Datum lowest_modified_value = PG_GETARG_DATUM(3);
	Datum greatest_modified_value = PG_GETARG_DATUM(4);
	Datum bucket_width_datum = PG_GETARG_DATUM(5);
	Name partial_view_schema = PG_GETARG_NAME(6);
	Oid time_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
	int64 bw =
		ts_interval_value_to_internal(bucket_width_datum, get_fn_expr_argtype(fcinfo->flinfo, 5));
	int64 lmv = ts_time_bucket_by_type(bw,
									   ts_time_value_to_internal(lowest_modified_value, time_type),
									   time_type);
	int64 gmv = int64_saturating_add(
		ts_time_bucket_by_type(bw,
							   ts_time_value_to_internal(greatest_modified_value, time_type),
							   time_type),
		bw);
	SchemaAndName partial_view = {
		.schema = partial_view_schema,
		.name = partial_view_name,
	};
	int64 invalidation_threshold;
	int64 completed_threshold;
	Cache *hcache;
	int32 hypertable_id =
		ts_hypertable_cache_get_cache_and_entry(hypertable, false, &hcache)->fd.id;
	int32 materialization_id =
		ts_hypertable_cache_get_entry(hcache, materialization_table, false)->fd.id;
	ts_cache_release(hcache);

	if (partial_view.name == NULL)
		elog(ERROR, "view cannot be NULL");
	invalidation_threshold = invalidation_threshold_get(hypertable_id);
	completed_threshold = ts_continuous_agg_get_completed_threshold(materialization_id);

	if (lmv > completed_threshold)
	{
		/* make empty inval range */
		lmv = 0;
		gmv = 0;
	}
	else if (gmv > completed_threshold)
		gmv = completed_threshold;

	/* since we have only 1 cagg, we can use the hypertable's inv. threshold */
	continuous_agg_execute_materialization(bw,
										   hypertable_id,
										   materialization_id,
										   partial_view,
										   lmv,
										   gmv,
										   invalidation_threshold);

	PG_RETURN_VOID();
}
