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
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		HeapTuple new_tuple;
		TupleDesc tupdesc = ts_scan_iterator_tupledesc(&iterator);

		heap_deform_tuple(tuple, tupdesc, values, nulls);

		repl[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
			BoolGetDatum(materialized_only);

		new_tuple = heap_modify_tuple(tuple, tupdesc, values, nulls, repl);

		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

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

	if (!with_clause_options[ContinuousViewOptionCreateGroupIndex].is_default)
	{
		elog(ERROR, "cannot alter create_group_indexes option for continuous aggregates");
	}
}
