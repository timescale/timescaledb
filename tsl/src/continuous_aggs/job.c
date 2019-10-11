/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <c.h>
#include <utils/builtins.h>

#include <hypertable_cache.h>
#include <scan_iterator.h>

#include "bgw/job.h"
#include "continuous_aggs/job.h"

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
ts_continuous_agg_job_add(int32 raw_table_id, int64 bucket_width, Interval *refresh_interval)
{
	NameData application_name;
	NameData job_type;
	int32 job_id;

	namestrcpy(&job_type, "continuous_aggregate");
	namestrcpy(&application_name, "Continuous Aggregate Background Job");

	if (refresh_interval == NULL)
		refresh_interval =
			continuous_agg_job_get_default_schedule_interval(raw_table_id, bucket_width);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&job_type,
										refresh_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										refresh_interval);
	return job_id;
}

int32
ts_continuous_agg_job_find_materializtion_by_job_id(int32 job_id)
{
	int32 materialization_id = -1;
	ScanIterator continuous_aggregate_iter =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	continuous_aggregate_iter.ctx.index =
		catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_JOB_ID_KEY);

	ts_scan_iterator_scan_key_init(&continuous_aggregate_iter,
								   Anum_continuous_agg_job_id_key_job_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(job_id));
	ts_scanner_foreach(&continuous_aggregate_iter)
	{
		HeapTuple tuple = ts_scan_iterator_tuple(&continuous_aggregate_iter);
		Form_continuous_agg form = (Form_continuous_agg) GETSTRUCT(tuple);
		Assert(materialization_id == -1);
		materialization_id = form->mat_hypertable_id;
	}

	return materialization_id;
}
