/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>

#include <parser/parsetree.h>
#include <commands/extension.h>
#include <commands/defrem.h>
#include <utils/hsearch.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>

#include <extension_constants.h>
#include <planner.h>

#include "cache.h"
#include "chunk.h"
#include "chunk_adaptive.h"
#include "deparse.h"
#include "dimension.h"
#include "errors.h"
#include "estimate.h"
#include "extension.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "option.h"
#include "relinfo.h"
#include "remote/connection.h"
#include "scan_exec.h"
#include "planner.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
/* Note that postgres_fdw sets this to 0.01, but we want to penalize
 * transferring many tuples in order to make it more attractive to push down
 * aggregates and thus transfer/process less tuples. */
#define DEFAULT_FDW_TUPLE_COST 0.08

#define DEFAULT_FDW_FETCH_SIZE 10000

#define DEFAULT_CHUNK_LOOKBACK_WINDOW 10

/*
 * Parse options from the foreign data wrapper and foreign server and apply
 * them to fpinfo. The server options take precedence over the data wrapper
 * ones.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_fdw_and_server_options(TsFdwRelInfo *fpinfo)
{
	ListCell *lc;
	ForeignDataWrapper *fdw = GetForeignDataWrapper(fpinfo->server->fdwid);
	List *options[] = { fdw->options, fpinfo->server->options };
	int i;

	for (i = 0; i < lengthof(options); i++)
	{
		foreach (lc, options[i])
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "fdw_startup_cost") == 0)
				fpinfo->fdw_startup_cost = strtod(defGetString(def), NULL);
			else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
				fpinfo->fdw_tuple_cost = strtod(defGetString(def), NULL);
			else if (strcmp(def->defname, "extensions") == 0)
				fpinfo->shippable_extensions =
					list_concat(fpinfo->shippable_extensions,
								option_extract_extension_list(defGetString(def), false));
			else if (strcmp(def->defname, "fetch_size") == 0)
				fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
		}
	}
}

TsFdwRelInfo *
fdw_relinfo_get(RelOptInfo *rel)
{
	TimescaleDBPrivate *rel_private = rel->fdw_private;
	TsFdwRelInfo *fdw_relation_info = rel_private->fdw_relation_info;

	/*
	 * This function is expected to return either null or a fully initialized
	 * fdw_relation_info struct.
	 */
	Assert(!fdw_relation_info || fdw_relation_info->type != TS_FDW_RELINFO_UNINITIALIZED);

	return fdw_relation_info;
}

TsFdwRelInfo *
fdw_relinfo_alloc_or_get(RelOptInfo *rel)
{
	TimescaleDBPrivate *rel_private = rel->fdw_private;
	if (rel_private == NULL)
	{
		rel_private = ts_create_private_reloptinfo(rel);
	}

	if (rel_private->fdw_relation_info == NULL)
	{
		rel_private->fdw_relation_info = (TsFdwRelInfo *) palloc0(sizeof(TsFdwRelInfo));
	}

	return rel_private->fdw_relation_info;
}

static const double FILL_FACTOR_CURRENT_CHUNK = 0.5;
static const double FILL_FACTOR_HISTORICAL_CHUNK = 1;

static const DimensionSlice *
get_chunk_time_slice(const Chunk *chunk, const Hyperspace *space)
{
	int32 time_dim_id = hyperspace_get_open_dimension(space, 0)->fd.id;
	return ts_hypercube_get_slice_by_dimension_id(chunk->cube, time_dim_id);
}

/*
 * Sums of slices belonging to closed dimensions
 */
static int
get_total_number_of_slices(Hyperspace *space)
{
	int dim_idx;
	int total_slices = 0;

	for (dim_idx = 0; dim_idx < space->num_dimensions; dim_idx++)
	{
		Dimension *dim = &space->dimensions[dim_idx];
		if (IS_CLOSED_DIMENSION(dim))
			total_slices += dim->fd.num_slices;
	}

	return total_slices;
}

/*
 * Estimate fill factor for the chunks that don't have ANALYZE statistics.
 * Fill factor values are between 0 and 1. It's an indication of how much data is
 * in the chunk, expressed as a fraction of its estimated final size.
 *
 * Fill factor estimation assumes that data written is 'recent' in regards to
 * time dimension (eg. almost real-time). For the case when writing historical
 * data, given estimates might be more off as we assume that historical chunks
 * have fill factor 1. Even for writing historical data we might not be totally
 * wrong since most probably data has monotonically increasing time.
 *
 * Estimation handles two possible hypertable configurations:
 * 1. time dimension is of timestamp type
 * 2. time dimension is of integer type.
 *
 * If hypertable uses timestamp type to partition data then there are three
 * possible scenarios here: we are beyond chunk end time (historical chunk), we
 * are somewhere in between chunk time boundaries (current chunk) or chunk start
 * time is in the future (highly unlikely, also treated as current chunk).
 *
 * For integer type we assume that all chunks w/o ANALYZE stats are current.
 * We could use the user-specified integer time function here
 * (set_integer_now_func()), but this logic is a fallback so we're keeping it
 * simple for now.
 *
 * Earlier, this function used chunk ids to guess which chunks are created later,
 * and treated such chunks as current. Unfortunately, the chunk ids are global
 * for all hypertables, so this approach didn't really work if there was more
 * than one hypertable.
 */
static double
estimate_chunk_fillfactor(Chunk *chunk, Hyperspace *space)
{
	const Dimension *time_dim = hyperspace_get_open_dimension(space, 0);
	const DimensionSlice *time_slice = get_chunk_time_slice(chunk, space);
	Oid time_dim_type = ts_dimension_get_partition_type(time_dim);

	if (IS_TIMESTAMP_TYPE(time_dim_type))
	{
		TimestampTz now = GetSQLCurrentTimestamp(-1);
#ifdef TS_DEBUG
		if (ts_current_timestamp_override_value >= 0)
			now = ts_current_timestamp_override_value;
#endif
		int64 now_internal_time =
			ts_time_value_to_internal(TimestampTzGetDatum(now), TIMESTAMPTZOID);

		/* if we are beyond end range then chunk can possibly be totally filled */
		if (time_slice->fd.range_end <= now_internal_time)
		{
			/*
			 * Current time is later than the end of the chunk time range, which
			 * means it is a historical chunk.
			 */
			return FILL_FACTOR_HISTORICAL_CHUNK;
		}

		/*
		 * The chunk time range starts later than current time, so we treat it
		 * as a current chunk.
		 */
		if (time_slice->fd.range_start >= now_internal_time)
			return FILL_FACTOR_CURRENT_CHUNK;

		/*
		 * Current time falls within chunk time constraints. The fill factor is
		 * interpolated linearly based on where the current time is inside the
		 * range, from 'current chunk fill factor' at the start of the range, to
		 * 'historical chunk fill factor' at the end of the range.
		 */
		double elapsed = (now_internal_time - time_slice->fd.range_start);
		double interval = (time_slice->fd.range_end - time_slice->fd.range_start);
		Assert(interval > 0);
		Assert(elapsed <= interval);

		Assert(FILL_FACTOR_HISTORICAL_CHUNK >= FILL_FACTOR_CURRENT_CHUNK);
		double fill_factor =
			FILL_FACTOR_CURRENT_CHUNK +
			(FILL_FACTOR_HISTORICAL_CHUNK - FILL_FACTOR_CURRENT_CHUNK) * (elapsed / interval);

		Assert(fill_factor >= 0.);
		Assert(fill_factor <= 1.);
		return fill_factor;
	}

	/*
	 * This chunk doesn't have the ANALYZE data, so it's more likely to be a
	 * recently created, current chunk, not an old historical chunk.
	 */
	return FILL_FACTOR_CURRENT_CHUNK;
}

static void
estimate_tuples_and_pages_using_shared_buffers(PlannerInfo *root, Hypertable *ht, RelOptInfo *rel)
{
	int64 chunk_size_estimate = ts_chunk_calculate_initial_chunk_target_size();
	const int result_width = rel->reltarget->width;

	if (ht != NULL)
	{
		int total_slices = get_total_number_of_slices(ht->space);
		if (total_slices > 0)
			chunk_size_estimate /= total_slices;
	}
	else
		/* half-size seems to be the safest bet */
		chunk_size_estimate /= 2;

	rel->tuples = chunk_size_estimate / (result_width + MAXALIGN(SizeofHeapTupleHeader));
	rel->pages = chunk_size_estimate / BLCKSZ;
}

/*
 * Estimate the chunk size if we don't have ANALYZE statistics, and update the
 * moving average of chunk sizes used for estimation.
 */
static void
estimate_chunk_size(PlannerInfo *root, RelOptInfo *chunk_rel)
{
	const int parent_relid = bms_next_member(chunk_rel->top_parent_relids, -1);
	if (parent_relid < 0)
	{
		/*
		 * In some cases (e.g., UPDATE stmt) top_parent_relids is not set so the
		 * best we can do is using shared buffers size without partitioning
		 * information. Since updates are not something we generaly optimize
		 * for, this should be fine.
		 */
		if (chunk_rel->pages == 0)
		{
			/* Can't have nonzero tuples in zero pages */
			Assert(chunk_rel->tuples <= 0);
			estimate_tuples_and_pages_using_shared_buffers(root, NULL, chunk_rel);
		}
		return;
	}

	/*
	 * Check if we have the chunk info cached for this chunk relation. For
	 * SELECTs, we should have cached it when we performed chunk exclusion.
	 * The UPDATEs use a completely different code path that doesn't do chunk
	 * exclusion, so we'll have to look up this info now.
	 */
	TimescaleDBPrivate *chunk_private = ts_get_private_reloptinfo(chunk_rel);
	if (chunk_private->chunk == NULL)
	{
		RangeTblEntry *chunk_rte = planner_rt_fetch(chunk_rel->relid, root);
		chunk_private->chunk =
			ts_chunk_get_by_relid(chunk_rte->relid, true /* fail_if_not_found */);
	}

	RelOptInfo *parent_info = root->simple_rel_array[parent_relid];
	/*
	 * The parent FdwRelInfo might not be allocated and initialized here, because
	 * it happens later in tsl_set_pathlist callback. We don't care about this
	 * because we only need it for chunk size estimates, so allocate it ourselves.
	 */
	TsFdwRelInfo *parent_private = fdw_relinfo_alloc_or_get(parent_info);
	RangeTblEntry *parent_rte = planner_rt_fetch(parent_relid, root);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, parent_rte->relid, CACHE_FLAG_NONE);
	Hyperspace *hyperspace = ht->space;

	const double fillfactor = estimate_chunk_fillfactor(chunk_private->chunk, hyperspace);

	/* Can't have nonzero tuples in zero pages */
	Assert(parent_private->average_chunk_pages != 0 || parent_private->average_chunk_tuples <= 0);
	Assert(chunk_rel->pages != 0 || chunk_rel->tuples <= 0);

	const bool have_chunk_statistics = chunk_rel->pages != 0;
	const bool have_moving_average =
		parent_private->average_chunk_pages != 0 || parent_private->average_chunk_tuples > 0;
	if (!have_chunk_statistics)
	{
		/*
		 * If we don't have the statistics from ANALYZE for this chunk,
		 * use the moving average of chunk sizes. If we don't have even
		 * that, use an estimate based on the default shared buffers
		 * size for a chunk.
		 */
		if (have_moving_average)
		{
			chunk_rel->pages = parent_private->average_chunk_pages * fillfactor;
			chunk_rel->tuples = parent_private->average_chunk_tuples * fillfactor;
		}
		else
		{
			estimate_tuples_and_pages_using_shared_buffers(root, ht, chunk_rel);
			chunk_rel->pages *= fillfactor;
			chunk_rel->tuples *= fillfactor;
		}
	}

	if (!have_moving_average)
	{
		/*
		 * Initialize the moving average data if we don't have any yet.
		 * Use even a bad estimate from shared buffers, to save on
		 * recalculating the same bad estimate for the subsequent chunks
		 * that are likely to not have the statistics as well.
		 */
		parent_private->average_chunk_pages = chunk_rel->pages;
		parent_private->average_chunk_tuples = chunk_rel->tuples;
	}
	else if (have_chunk_statistics)
	{
		/*
		 * We have the moving average of chunk sizes and a good estimate
		 * of this chunk size from ANALYZE. Update the moving average.
		 */
		const double f = 0.1;
		parent_private->average_chunk_pages =
			(1 - f) * parent_private->average_chunk_pages + f * chunk_rel->pages / fillfactor;
		parent_private->average_chunk_tuples =
			(1 - f) * parent_private->average_chunk_tuples + f * chunk_rel->tuples / fillfactor;
	}
	else
	{
		/*
		 * Already have some moving average data, but don't have good
		 * statistics for this chunk. Do nothing.
		 */
	}

	ts_cache_release(hcache);
}

TsFdwRelInfo *
fdw_relinfo_create(PlannerInfo *root, RelOptInfo *rel, Oid server_oid, Oid local_table_id,
				   TsFdwRelInfoType type)
{
	TsFdwRelInfo *fpinfo;
	ListCell *lc;
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	const char *refname;

	/*
	 * We use TsFdwRelInfo to pass various information to subsequent
	 * functions. It might be already partially initialized for a data node
	 * hypertable, because we use it to maintain the chunk size estimates when
	 * planning.
	 */
	fpinfo = fdw_relinfo_alloc_or_get(rel);
	Assert(fpinfo->type == TS_FDW_RELINFO_UNINITIALIZED || fpinfo->type == type);
	fpinfo->type = type;

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */

	fpinfo->relation_name = makeStringInfo();
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name,
					 "%s.%s",
					 quote_identifier(get_namespace_name(get_rel_namespace(rte->relid))),
					 quote_identifier(get_rel_name(rte->relid)));
	if (*refname && strcmp(refname, get_rel_name(rte->relid)) != 0)
		appendStringInfo(fpinfo->relation_name, " %s", quote_identifier(rte->eref->aliasname));

	if (type == TS_FDW_RELINFO_HYPERTABLE)
	{
		/* nothing more to do for hypertables */
		Assert(!OidIsValid(server_oid));

		return fpinfo;
	}
	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->server = GetForeignServer(server_oid);

	/*
	 * Extract user-settable option values.  Note that per-table setting
	 * overrides per-server setting.
	 */
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	Assert(ts_extension_oid != InvalidOid);
	fpinfo->shippable_extensions = list_make1_oid(ts_extension_oid);
	fpinfo->fetch_size = DEFAULT_FDW_FETCH_SIZE;

	apply_fdw_and_server_options(fpinfo);

	/*
	 * Identify which baserestrictinfo clauses can be sent to the data
	 * node and which can't.
	 */
	classify_conditions(root,
						rel,
						rel->baserestrictinfo,
						&fpinfo->remote_conds,
						&fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the data
	 * node.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &fpinfo->attrs_used);
	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel =
		clauselist_selectivity(root, fpinfo->local_conds, rel->relid, JOIN_INNER, NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to fdw_estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;
	fpinfo->rel_retrieved_rows = -1;

	if (type == TS_FDW_RELINFO_FOREIGN_TABLE)
	{
		/*
		 * For a chunk, estimate its size if we don't know it, and update the
		 * moving average of chunk sizes used for this estimation.
		 */
		estimate_chunk_size(root, rel);
	}

	/* Estimate rel size as best we can with local statistics. There are
	 * no local statistics for data node rels since they aren't real base
	 * rels (there's no corresponding table in the system to associate
	 * stats with). Instead, data node rels already have basic stats set
	 * at creation time based on data-node-chunk assignment. */
	if (fpinfo->type != TS_FDW_RELINFO_HYPERTABLE_DATA_NODE)
		set_baserel_size_estimates(root, rel);

	/* Fill in basically-bogus cost estimates for use later. */
	fdw_estimate_path_cost_size(root,
								rel,
								NIL,
								&fpinfo->rows,
								&fpinfo->width,
								&fpinfo->startup_cost,
								&fpinfo->total_cost);

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = rel->relid;

	return fpinfo;
}
