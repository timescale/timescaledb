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
	Datum bucket_width = PG_GETARG_INT64(5);
	Name partial_view_schema = PG_GETARG_NAME(6);
	Oid time_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
	int64 lmv = ts_time_value_to_internal(lowest_modified_value, time_type);
	int64 gmv = ts_time_value_to_internal(greatest_modified_value, time_type);
	int64 bw = ts_interval_value_to_internal(bucket_width, get_fn_expr_argtype(fcinfo->flinfo, 5));
	Invalidation invalidation = {
		.lowest_modified_value = lmv,
		.greatest_modified_value = gmv,
	};
	List *invalidations = list_make1(&invalidation);
	SchemaAndName partial_view = {
		.schema = partial_view_schema,
		.name = partial_view_name,
	};
	Cache *hcache = ts_hypertable_cache_pin();
	int32 hypertable_id = ts_hypertable_cache_get_entry(hcache, hypertable)->fd.id;
	int32 materialization_id = ts_hypertable_cache_get_entry(hcache, materialization_table)->fd.id;
	ts_cache_release(hcache);

	if (partial_view.name == NULL)
		elog(ERROR, "view cannot be NULL");

	continuous_agg_execute_materialization(bw,
										   hypertable_id,
										   materialization_id,
										   partial_view,
										   invalidations);

	PG_RETURN_VOID();
}
