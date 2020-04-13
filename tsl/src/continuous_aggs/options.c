/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <commands/view.h>
#include <miscadmin.h>
#include <rewrite/rewriteManip.h>
#include <utils/int8.h>
#include <utils/builtins.h>

#include "options.h"
#include "continuous_agg.h"
#include "continuous_aggs/create.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "scan_iterator.h"
#include "job.h"

static inline int64
parse_int_interval(const char *value, int64 min, int64 max, const char *option_name)
{
	int64 result;
	if (!scanint8(value, true, &result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter timescaledb.%s must be an integer for hypertables with integer "
						"time values",
						option_name)));
	if (result < min || result > max)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("timescaledb.%s out of range", option_name)));
	return result;
}

static int64
parse_interval(char *value, Oid column_type, const char *option_name)
{
	Datum interval;
	Oid in_fn;
	Oid typIOParam;

	switch (column_type)
	{
		case INT2OID:
			return parse_int_interval(value, PG_INT16_MIN, PG_INT16_MAX, option_name);
		case INT4OID:
			return parse_int_interval(value, PG_INT32_MIN, PG_INT32_MAX, option_name);
		case INT8OID:
			return parse_int_interval(value, PG_INT64_MIN, PG_INT64_MAX, option_name);
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		case DATEOID:
			getTypeInputInfo(INTERVALOID, &in_fn, &typIOParam);
			Assert(OidIsValid(in_fn));
			interval = OidInputFunctionCall(in_fn, value, typIOParam, -1);
			return ts_interval_value_to_internal(interval, INTERVALOID);
		default:
			elog(ERROR, "unknown time type when parsing timescaledb.%s", option_name);
			pg_unreachable();
	}
}

int64
continuous_agg_parse_refresh_lag(Oid column_type, WithClauseResult *with_clause_options)
{
	char *value;

	Assert(!with_clause_options[ContinuousViewOptionRefreshLag].is_default);

	value = TextDatumGetCString(with_clause_options[ContinuousViewOptionRefreshLag].parsed);

	return parse_interval(value, column_type, "refresh_lag");
}

int64
continuous_agg_parse_ignore_invalidation_older_than(Oid column_type,
													WithClauseResult *with_clause_options)
{
	char *value;
	int64 ret;

	Assert(!with_clause_options[ContinuousViewOptionIgnoreInvalidationOlderThan].is_default);

	value = TextDatumGetCString(
		with_clause_options[ContinuousViewOptionIgnoreInvalidationOlderThan].parsed);

	ret = parse_interval(value, column_type, "ignore_invalidation_older_than");

	if (ret < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg(
					 "parameter timescaledb.ignore_invalidation_older_than must not be negative")));
	return ret;
}

int64
continuous_agg_parse_max_interval_per_job(Oid column_type, WithClauseResult *with_clause_options,
										  int64 bucket_width)
{
	char *value;
	int64 result;

	Assert(!with_clause_options[ContinuousViewOptionMaxIntervalPerRun].is_default);

	value = TextDatumGetCString(with_clause_options[ContinuousViewOptionMaxIntervalPerRun].parsed);

	result = parse_interval(value, column_type, "max_interval_per_job");

	if (result < bucket_width)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter timescaledb.max_interval_per_job must be at least the size "
						"of the time_bucket width")));
	return result;
}

static void
update_refresh_lag(ContinuousAgg *agg, int64 new_lag)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(agg->data.mat_hypertable_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool nulls[Natts_continuous_agg];
		Datum values[Natts_continuous_agg];
		bool repl[Natts_continuous_agg] = { false };
		HeapTuple new;

		heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

		repl[AttrNumberGetAttrOffset(Anum_continuous_agg_refresh_lag)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_refresh_lag)] = Int64GetDatum(new_lag);

		new = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);

		ts_catalog_update(ti->scanrel, new);
		break;
	}
	ts_scan_iterator_close(&iterator);
}

static void
update_materialized_only(ContinuousAgg *agg, bool materialized_only)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(agg->data.mat_hypertable_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool nulls[Natts_continuous_agg];
		Datum values[Natts_continuous_agg];
		bool repl[Natts_continuous_agg] = { false };
		HeapTuple new;

		heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

		repl[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
			BoolGetDatum(materialized_only);

		new = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);

		ts_catalog_update(ti->scanrel, new);
		break;
	}
	ts_scan_iterator_close(&iterator);
}

static void
update_max_interval_per_job(ContinuousAgg *agg, int64 new_max)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(agg->data.mat_hypertable_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool nulls[Natts_continuous_agg];
		Datum values[Natts_continuous_agg];
		bool repl[Natts_continuous_agg] = { false };
		HeapTuple new;

		heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

		repl[AttrNumberGetAttrOffset(Anum_continuous_agg_max_interval_per_job)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_max_interval_per_job)] =
			Int64GetDatum(new_max);

		new = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);

		ts_catalog_update(ti->scanrel, new);
		break;
	}
	ts_scan_iterator_close(&iterator);
}

static void
update_ignore_invalidation_older_than(ContinuousAgg *agg, int64 new_ignore_invalidation_older_than)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(agg->data.mat_hypertable_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool nulls[Natts_continuous_agg];
		Datum values[Natts_continuous_agg];
		bool repl[Natts_continuous_agg] = { false };
		HeapTuple new;

		heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

		repl[AttrNumberGetAttrOffset(Anum_continuous_agg_ignore_invalidation_older_than)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_ignore_invalidation_older_than)] =
			Int64GetDatum(new_ignore_invalidation_older_than);

		new = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);

		ts_catalog_update(ti->scanrel, new);
		break;
	}
	ts_scan_iterator_close(&iterator);
}

void
continuous_agg_update_options(ContinuousAgg *agg, WithClauseResult *with_clause_options)
{
	if (!with_clause_options[ContinuousEnabled].is_default)
		elog(ERROR, "cannot disable continuous aggregates");

	if (!with_clause_options[ContinuousViewOptionMaterializedOnly].is_default)
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *mat_ht =
			ts_hypertable_cache_get_entry_by_id(hcache, agg->data.mat_hypertable_id);
		bool materialized_only =
			DatumGetBool(with_clause_options[ContinuousViewOptionMaterializedOnly].parsed);
		Assert(mat_ht != NULL);

		cagg_update_view_definition(agg, mat_ht, with_clause_options);
		update_materialized_only(agg, materialized_only);
		ts_cache_release(hcache);
	}

	if (!with_clause_options[ContinuousViewOptionRefreshLag].is_default)
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, agg->data.raw_hypertable_id);
		Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
		int64 lag =
			continuous_agg_parse_refresh_lag(ts_dimension_get_partition_type(time_dimension),
											 with_clause_options);
		update_refresh_lag(agg, lag);
		ts_cache_release(hcache);
	}

	if (!with_clause_options[ContinuousViewOptionMaxIntervalPerRun].is_default)
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, agg->data.raw_hypertable_id);
		Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
		int64 max = continuous_agg_parse_max_interval_per_job(ts_dimension_get_partition_type(
																  time_dimension),
															  with_clause_options,
															  agg->data.bucket_width);
		update_max_interval_per_job(agg, max);
		ts_cache_release(hcache);
	}

	if (!with_clause_options[ContinuousViewOptionIgnoreInvalidationOlderThan].is_default)
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, agg->data.raw_hypertable_id);
		Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
		int64 ignore_invalidation_older_than =
			continuous_agg_parse_ignore_invalidation_older_than(ts_dimension_get_partition_type(
																	time_dimension),
																with_clause_options);
		update_ignore_invalidation_older_than(agg, ignore_invalidation_older_than);
		ts_cache_release(hcache);
	}

	if (!with_clause_options[ContinuousViewOptionRefreshInterval].is_default)
	{
		BgwJob *job = ts_bgw_job_find(agg->data.job_id, CurrentMemoryContext, true);
		job->fd.schedule_interval =
			*DatumGetIntervalP(with_clause_options[ContinuousViewOptionRefreshInterval].parsed);
		job->fd.retry_period =
			*DatumGetIntervalP(with_clause_options[ContinuousViewOptionRefreshInterval].parsed);
		ts_bgw_job_update_by_id(agg->data.job_id, job);
	}
	if (!with_clause_options[ContinuousViewOptionCreateGroupIndex].is_default)
	{
		elog(ERROR, "cannot alter create_group_indexes option for continuous aggregates");
	}
}
