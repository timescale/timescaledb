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
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>

#include "options.h"
#include "cache.h"
#include "compression_with_clause.h"
#include "ts_catalog/continuous_agg.h"
#include "continuous_aggs/create.h"
#include "compression/create.h"
#include "errors.h"
#include "hypertable_cache.h"
#include "scan_iterator.h"

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
		bool doReplace[Natts_continuous_agg] = { false };
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		HeapTuple new_tuple;
		TupleDesc tupdesc = ts_scan_iterator_tupledesc(&iterator);

		heap_deform_tuple(tuple, tupdesc, values, nulls);

		doReplace[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] = true;
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
			BoolGetDatum(materialized_only);

		new_tuple = heap_modify_tuple(tuple, tupdesc, values, nulls, doReplace);

		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		break;
	}
	ts_scan_iterator_close(&iterator);
}

/*
 * This function is responsible to return a list of column names used in
 * GROUP BY clause of the cagg query. It behaves a bit different depending
 * of the type of the Continuous Aggregate.
 *
 * 1) Partials form (finalized=false)
 *
 *    Retrieve the "user view query" and find the GROUP BY clause and
 *    "time_bucket" clause. Map them to the column names (of mat.hypertable)
 *
 *    Note that the "user view query" has 2 forms:
 *    - with UNION
 *    - without UNION
 *
 *    We have to extract the part of the query that has "finalize_agg" on
 *    the materialized hypertable to find the GROUP BY clauses.
 *    (see continuous_aggs/create.c for more info on the query structure)
 *
 * 2) Finals form (finalized=true) (>= 2.7)
 *
 *    Retrieve the "direct view query" and find the GROUP BY clause and
 *    "time_bucket" clause. We use the "direct view query" because in the
 *    "user view query" we removed the re-aggregation in the part that query
 *    the materialization hypertable so we don't have a GROUP BY clause
 *    anymore.
 *
 *    Get the column name from the GROUP BY clause because all the column
 *    names are the same in all underlying objects (user view, direct view,
 *    partial view and materialization hypertable).
 */
static List *
cagg_find_groupingcols(ContinuousAgg *agg, Hypertable *mat_ht)
{
	List *retlist = NIL;
	ListCell *lc;
	Query *cagg_view_query = ts_continuous_agg_get_query(agg);
	Oid mat_relid = mat_ht->main_table_relid;
	Query *finalize_query;

	/* The view rule has dummy old and new range table entries as the 1st and 2nd entries */
	Assert(list_length(cagg_view_query->rtable) >= 2);
	if (cagg_view_query->setOperations)
	{
		/* This corresponds to the union view.
		 * the 3rd RTE entry has the SELECT 1 query from the union view. */
		RangeTblEntry *finalize_query_rte = lthird(cagg_view_query->rtable);
		if (finalize_query_rte->rtekind != RTE_SUBQUERY)
			ereport(ERROR,
					(errcode(ERRCODE_TS_UNEXPECTED),
					 errmsg("unexpected rte type for view %d", finalize_query_rte->rtekind)));

		finalize_query = finalize_query_rte->subquery;
	}
	else
	{
		finalize_query = cagg_view_query;
	}

	foreach (lc, finalize_query->groupClause)
	{
		SortGroupClause *cagg_gc = (SortGroupClause *) lfirst(lc);
		TargetEntry *cagg_tle = get_sortgroupclause_tle(cagg_gc, finalize_query->targetList);

		if (ContinuousAggIsFinalized(agg))
		{
			/* "resname" is the same as "mat column names" in the finalized version */
			if (!cagg_tle->resjunk && cagg_tle->resname)
				retlist = lappend(retlist, cagg_tle->resname);
		}
		else
		{
			/* groupby clauses are columns from the mat hypertable */
			Var *mat_var = castNode(Var, cagg_tle->expr);
			retlist = lappend(retlist, get_attname(mat_relid, mat_var->varattno, false));
		}
	}
	return retlist;
}

/* get the compression parameters for cagg. The parameters are
 * derived from the cagg view definition.
 * Computes:
 * compress_segmentby = GROUP BY columns from cagg query
 * compress_orderby = time_bucket column from cagg query
 */
static List *
cagg_get_compression_params(ContinuousAgg *agg, Hypertable *mat_ht)
{
	List *defelems = NIL;
	const Dimension *mat_ht_dim = hyperspace_get_open_dimension(mat_ht->space, 0);
	const char *mat_ht_timecolname = NameStr(mat_ht_dim->fd.column_name);
	DefElem *ordby = makeDefElemExtended("timescaledb",
										 "compress_orderby",
										 (Node *) makeString((char *) mat_ht_timecolname),
										 DEFELEM_UNSPEC,
										 -1);
	defelems = lappend(defelems, ordby);
	List *grp_colnames = cagg_find_groupingcols(agg, mat_ht);
	if (grp_colnames)
	{
		ListCell *lc;
		/* we have column names. they are guaranteed to be at most
		 * NAMEDATALEN
		 */
		int seglen = ((NAMEDATALEN + 1) * list_length(grp_colnames)) + 1;
		char *segmentby = (char *) palloc(seglen);
		int segidx = 0;
		foreach (lc, grp_colnames)
		{
			int collen;
			char *grpcol = (char *) lfirst(lc);
			/* skip time dimension col if it appears in group-by list */
			if (namestrcmp((Name) & (mat_ht_dim->fd.column_name), grpcol) == 0)
				continue;
			if (segidx > 0 && (seglen - segidx) > 1)
			{
				strlcpy(segmentby + segidx, ",", 2);
				segidx = segidx + 1;
			}
			collen = strlen(grpcol);
			if (seglen - segidx > collen)
			{
				strlcpy(segmentby + segidx, grpcol, collen + 1);
				segidx = segidx + collen;
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("%s not enough space to copy segment by column (%d %d %d)",
								__func__,
								seglen,
								segidx,
								collen)));
			}
		}
		if (segidx != 0)
		{
			DefElem *segby;
			segmentby[segidx] = '\0';
			segby = makeDefElemExtended("timescaledb",
										"compress_segmentby",
										(Node *) makeString(segmentby),
										DEFELEM_UNSPEC,
										-1);
			defelems = lappend(defelems, segby);
		}
	}

	return defelems;
}

/* enable/disable compression on continuous aggregate */
static void
cagg_alter_compression(ContinuousAgg *agg, Hypertable *mat_ht, bool compress_enable)
{
	List *defelems = NIL;
	Assert(mat_ht != NULL);
	if (compress_enable)
		defelems = cagg_get_compression_params(agg, mat_ht);

	DefElem *enable = makeDefElemExtended("timescaledb",
										  "compress",
										  compress_enable ? (Node *) makeString("true") :
															(Node *) makeString("false"),
										  DEFELEM_UNSPEC,
										  -1);
	defelems = lappend(defelems, enable);

	WithClauseResult *with_clause_options = ts_compress_hypertable_set_clause_parse(defelems);
	AlterTableCmd alter_cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_SetRelOptions,
		.def = (Node *) defelems,
	};

	tsl_process_compress_table(&alter_cmd, mat_ht, with_clause_options);
}

void
continuous_agg_update_options(ContinuousAgg *agg, WithClauseResult *with_clause_options)
{
	if (!with_clause_options[ContinuousEnabled].is_default)
		elog(ERROR, "cannot disable continuous aggregates");

	if (!with_clause_options[ContinuousViewOptionMaterializedOnly].is_default)
	{
		bool materialized_only =
			DatumGetBool(with_clause_options[ContinuousViewOptionMaterializedOnly].parsed);

		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *mat_ht =
			ts_hypertable_cache_get_entry_by_id(hcache, agg->data.mat_hypertable_id);

		if (materialized_only == agg->data.materialized_only)
		{
			/* nothing changed, so just return */
			ts_cache_release(hcache);
			return;
		}

		Assert(mat_ht != NULL);

		cagg_flip_realtime_view_definition(agg, mat_ht);
		update_materialized_only(agg, materialized_only);
		ts_cache_release(hcache);
	}
	if (!with_clause_options[ContinuousViewOptionCompress].is_default)
	{
		bool compress_enable =
			DatumGetBool(with_clause_options[ContinuousViewOptionCompress].parsed);
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *mat_ht =
			ts_hypertable_cache_get_entry_by_id(hcache, agg->data.mat_hypertable_id);
		Assert(mat_ht != NULL);

		cagg_alter_compression(agg, mat_ht, compress_enable);
		ts_cache_release(hcache);
	}
	if (!with_clause_options[ContinuousViewOptionCreateGroupIndex].is_default)
	{
		elog(ERROR, "cannot alter create_group_indexes option for continuous aggregates");
	}
	if (!with_clause_options[ContinuousViewOptionFinalized].is_default)
	{
		elog(ERROR, "cannot alter finalized option for continuous aggregates");
	}
}
