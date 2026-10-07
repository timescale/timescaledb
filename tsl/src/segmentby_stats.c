/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Planner statistics for segmentby columns of compressed chunks.
 *
 * The rows of a compressed chunk are stored in its compressed relation, so
 * ANALYZE finds the chunk and the hypertable empty and stores no statistics
 * for them. Without statistics the planner assumes 200 distinct values, which
 * badly underestimates the number of groups for GROUP BY on a segmentby
 * column with many values.
 *
 * Each compressed row holds one segmentby value, so the compressed relation's
 * statistics for that column describe the same set of values. We pass the
 * number of distinct values and the null fraction on to the planner. The
 * statistics tuple we build has no MCV list or histogram.
 */
#include <postgres.h>

#include <access/htup_details.h>
#include <access/table.h>
#include <catalog/pg_class.h>
#include <catalog/pg_statistic.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "chunk.h"
#include "hypertable.h"
#include "planner/planner.h"
#include "segmentby_stats.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/compression_settings.h"

typedef struct SegmentbyStats
{
	double ndistinct; /* absolute number of distinct values */
	float4 nullfrac;
	int32 width;
} SegmentbyStats;

static RelOptInfo *
find_rel_for_rte(PlannerInfo *root, RangeTblEntry *rte, Index *rti)
{
	if (root->simple_rte_array == NULL || root->simple_rel_array == NULL)
	{
		return NULL;
	}

	for (Index i = 1; i < (Index) root->simple_rel_array_size; i++)
	{
		if (root->simple_rte_array[i] == rte)
		{
			*rti = i;
			return root->simple_rel_array[i];
		}
	}

	return NULL;
}

/*
 * Read the statistics of a segmentby column from the compressed relation of a
 * fully compressed chunk. Returns false if the chunk has rows outside the
 * compressed relation, the column is not a segmentby column, or the
 * compressed relation has not been analyzed.
 */
static bool
chunk_segmentby_stats(const Chunk *chunk, const char *colname, SegmentbyStats *out)
{
	if (chunk == NULL || !ts_chunk_is_compressed(chunk) || ts_chunk_is_partial(chunk))
	{
		return false;
	}

	CompressionSettings *settings = ts_compression_settings_get(chunk->fd.relid);
	if (settings == NULL || !OidIsValid(settings->fd.compress_relid) ||
		!ts_array_is_member(settings->fd.segmentby, colname))
	{
		return false;
	}

	Oid compress_relid = settings->fd.compress_relid;
	AttrNumber compressed_attno = get_attnum(compress_relid, colname);
	if (compressed_attno == InvalidAttrNumber)
	{
		return false;
	}

	HeapTuple stats_tuple = SearchSysCache3(STATRELATTINH,
											ObjectIdGetDatum(compress_relid),
											Int16GetDatum(compressed_attno),
											BoolGetDatum(false));
	if (!HeapTupleIsValid(stats_tuple))
	{
		return false;
	}

	Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(stats_tuple);
	double ndistinct = stats->stadistinct;
	out->nullfrac = stats->stanullfrac;
	out->width = stats->stawidth;
	ReleaseSysCache(stats_tuple);

	/*
	 * A negative value is a fraction of the compressed row count. Turn it into
	 * an absolute count, because the chunk has a different row count.
	 */
	if (ndistinct < 0)
	{
		HeapTuple class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(compress_relid));
		if (!HeapTupleIsValid(class_tuple))
		{
			return false;
		}
		float4 reltuples = ((Form_pg_class) GETSTRUCT(class_tuple))->reltuples;
		ReleaseSysCache(class_tuple);

		if (reltuples <= 0)
		{
			return false;
		}
		ndistinct = -ndistinct * reltuples;
	}

	/* Zero means unknown */
	if (ndistinct <= 0)
	{
		return false;
	}

	out->ndistinct = ndistinct;
	return true;
}

/*
 * Combine the chunks of a hypertable. The same segmentby values usually repeat
 * in every chunk, so take the largest per-chunk count rather than the sum.
 */
static bool
hypertable_segmentby_stats(PlannerInfo *root, Index rti, const char *colname, SegmentbyStats *out)
{
	ListCell *lc;
	int nchunks = 0;
	double nullfrac_sum = 0;

	memset(out, 0, sizeof(*out));

	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);

		if (appinfo->parent_relid != rti)
		{
			continue;
		}

		RelOptInfo *child = root->simple_rel_array[appinfo->child_relid];
		if (child == NULL || IS_DUMMY_REL(child))
		{
			continue;
		}

		Hypertable *child_ht = NULL;
		if (ts_classify_relation(root, child, &child_ht) != TS_REL_CHUNK_CHILD)
		{
			return false;
		}

		SegmentbyStats chunk_stats;
		if (!chunk_segmentby_stats(ts_planner_chunk_fetch(root, child), colname, &chunk_stats))
		{
			return false;
		}

		out->ndistinct = Max(out->ndistinct, chunk_stats.ndistinct);
		out->width = Max(out->width, chunk_stats.width);
		nullfrac_sum += chunk_stats.nullfrac;
		nchunks++;
	}

	if (nchunks == 0)
	{
		return false;
	}

	out->nullfrac = nullfrac_sum / nchunks;
	return true;
}

static HeapTuple
make_stats_tuple(Oid relid, AttrNumber attnum, bool inherited, const SegmentbyStats *stats)
{
	Datum values[Natts_pg_statistic];
	bool nulls[Natts_pg_statistic];

	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inherited);
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->nullfrac);
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->width);
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum((float4) stats->ndistinct);

	/* No slots: no MCV list, no histogram */
	for (int k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		values[Anum_pg_statistic_stakind1 - 1 + k] = Int16GetDatum(0);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
		nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;
	}

	Relation statrel = table_open(StatisticRelationId, AccessShareLock);
	HeapTuple tuple = heap_form_tuple(RelationGetDescr(statrel), values, nulls);
	table_close(statrel, AccessShareLock);

	return tuple;
}

bool
tsl_get_relation_stats(PlannerInfo *root, RangeTblEntry *rte, AttrNumber attnum,
					   VariableStatData *vardata)
{
	if (root == NULL || rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION ||
		attnum <= 0)
	{
		return false;
	}

	/* Real statistics take precedence */
	if (SearchSysCacheExists3(STATRELATTINH,
							  ObjectIdGetDatum(rte->relid),
							  Int16GetDatum(attnum),
							  BoolGetDatum(rte->inh)))
	{
		return false;
	}

	Index rti = 0;
	RelOptInfo *rel = find_rel_for_rte(root, rte, &rti);
	if (rel == NULL)
	{
		return false;
	}

	Hypertable *ht = NULL;
	TsRelType reltype = ts_classify_relation(root, rel, &ht);
	if (ht == NULL || !TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		return false;
	}

	char *colname = get_attname(rte->relid, attnum, /* missing_ok = */ true);
	if (colname == NULL)
	{
		return false;
	}

	SegmentbyStats stats;
	switch (reltype)
	{
		case TS_REL_CHUNK_STANDALONE:
		case TS_REL_CHUNK_CHILD:
			if (rte->inh ||
				!chunk_segmentby_stats(ts_planner_chunk_fetch(root, rel), colname, &stats))
			{
				return false;
			}
			break;
		case TS_REL_HYPERTABLE:
			if (!rte->inh || !hypertable_segmentby_stats(root, rti, colname, &stats))
			{
				return false;
			}
			break;
		default:
			return false;
	}

	vardata->statsTuple = make_stats_tuple(rte->relid, attnum, rte->inh, &stats);
	vardata->freefunc = heap_freetuple;
	/* The tuple has no column values, only counts */
	vardata->acl_ok = true;

	return true;
}
