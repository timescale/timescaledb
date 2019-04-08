/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/int8.h>
#include <utils/builtins.h>

#include "options.h"
#include "continuous_agg.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "scan_iterator.h"
#include "job.h"

static inline int64
parse_int_lag(const char *value, int64 min, int64 max)
{
	int64 result;
	if (!scanint8(value, true, &result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter timescaledb.refresh_lag must be an integer for "
						"hypertables with integer time values")));
	if (result < min || result > max)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("timescaledb.refresh_lag out of range")));
	return result;
}

int64
continuous_agg_parse_refresh_lag(Oid column_type, WithClauseResult *with_clause_options)
{
	char *value;
	Datum interval;
	Oid in_fn;
	Oid typIOParam;

	Assert(!with_clause_options[ContinuousViewOptionRefreshLag].is_default);

	value = TextDatumGetCString(with_clause_options[ContinuousViewOptionRefreshLag].parsed);

	switch (column_type)
	{
		case INT2OID:
			return parse_int_lag(value, PG_INT16_MIN, PG_INT16_MAX);
		case INT4OID:
			return parse_int_lag(value, PG_INT32_MIN, PG_INT32_MAX);
		case INT8OID:
			return parse_int_lag(value, PG_INT64_MIN, PG_INT64_MAX);
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		case DATEOID:
			getTypeInputInfo(INTERVALOID, &in_fn, &typIOParam);
			Assert(OidIsValid(in_fn));
			interval = OidInputFunctionCall(in_fn, value, typIOParam, -1);
			return ts_interval_value_to_internal(interval, INTERVALOID);
		default:
			elog(ERROR, "unknown time type");
	}
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

void
continuous_agg_update_options(ContinuousAgg *agg, WithClauseResult *with_clause_options)
{
	if (!with_clause_options[ContinuousEnabled].is_default)
		elog(ERROR, "cannot disable continuous aggregates");

	if (!with_clause_options[ContinuousViewOptionRefreshLag].is_default)
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, agg->data.raw_hypertable_id);
		Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
		int64 lag =
			continuous_agg_parse_refresh_lag(time_dimension->fd.column_type, with_clause_options);
		update_refresh_lag(agg, lag);
		ts_cache_release(hcache);
	}

	if (!with_clause_options[ContinuousViewOptionRefreshInterval].is_default)
	{
		BgwJob *job = ts_bgw_job_find(agg->data.job_id, CurrentMemoryContext, true);
		job->fd.schedule_interval =
			*DatumGetIntervalP(with_clause_options[ContinuousViewOptionRefreshInterval].parsed);
		ts_bgw_job_update_by_id(agg->data.job_id, job);
	}
}
