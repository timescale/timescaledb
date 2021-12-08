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

#include "remote/connection.h"
#include "option.h"
#include "deparse.h"
#include "relinfo.h"
#include "estimate.h"
#include "chunk_adaptive.h"
#include "cache.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "chunk.h"
#include "hypercube.h"
#include "errors.h"
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
	TimescaleDBPrivate *rel_private;

	if (!rel->fdw_private)
		ts_create_private_reloptinfo(rel);

	rel_private = rel->fdw_private;

	if (!rel_private->fdw_relation_info)
		rel_private->fdw_relation_info = palloc0(sizeof(TsFdwRelInfo));

	return (TsFdwRelInfo *) rel_private->fdw_relation_info;
}

TsFdwRelInfo *
fdw_relinfo_alloc(RelOptInfo *rel, TsFdwRelInfoType reltype)
{
	TimescaleDBPrivate *rel_private;
	TsFdwRelInfo *fpinfo;

	if (NULL == rel->fdw_private)
		ts_create_private_reloptinfo(rel);

	rel_private = rel->fdw_private;

	fpinfo = (TsFdwRelInfo *) palloc0(sizeof(*fpinfo));
	rel_private->fdw_relation_info = (void *) fpinfo;
	fpinfo->type = reltype;

	return fpinfo;
}

static char *
get_relation_qualified_name(Oid relid)
{
	StringInfo name = makeStringInfo();
	const char *relname = get_rel_name(relid);
	const char *namespace = get_namespace_name(get_rel_namespace(relid));
	appendStringInfo(name, "%s.%s", quote_identifier(namespace), quote_identifier(relname));

	return name->data;
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
 * Fillfactor values are between 0 and 1. It's an indication of how much data is in the chunk.
 *
 * Two major drivers for estimation is current time and number of chunks created after.
 *
 * Fill factor estimation assumes that data written is 'recent' in regards to time dimension (eg.
 * almost real-time). For the case when writing historical data, given estimates might be more off
 * as we assume that historical chunks have fill factor 1 unless the number of chunks created after
 * is smaller then total number of slices. Even for writing historical data we might not be totally
 * wrong since most probably data has monotonically increasing time.
 *
 * Estimation handles two possible hypertable configurations: 1. time dimension is of timestamp
 * type 2. time dimension is of integer type. If hypertable uses timestamp type to partition data
 * then there are three possible scenarios here: we are beyond chunk end time (historical chunk), we
 * are somewhere in between chunk time boundaries (current chunk) or chunk start time is in the
 * future (highly unlikely). For integer type we assume that all chunks execpt for current have
 * factor 1.
 *
 * To explain how number of chunks created after the chunk affects estimation
 * let's imagine that table is space partitioned with one dimension and having 3 partitions. If data
 * is equaliy distributed amount partitions then there will be 3 current chunks. If there are two
 * new chunks created after chunk X then chunk X is the current chunk.
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
		int64 now_internal_time;
		double elapsed;
		double interval;

#ifdef TS_DEBUG
		if (ts_current_timestamp_override_value >= 0)
			now = ts_current_timestamp_override_value;
#endif
		now_internal_time = ts_time_value_to_internal(TimestampTzGetDatum(now), TIMESTAMPTZOID);

		/* if we are beyond end range then chunk can possibly be totally filled */
		if (time_slice->fd.range_end <= now_internal_time)
		{
			int total_slices = get_total_number_of_slices(space);
			int num_created_after = ts_chunk_num_of_chunks_created_after(chunk, total_slices);

			/* If there are less newly created chunks then the number of slices then this is current
			 * chunk. This also works better when writing historical data */
			return num_created_after < total_slices ? FILL_FACTOR_CURRENT_CHUNK :
													  FILL_FACTOR_HISTORICAL_CHUNK;
		}

		/* for chunks in future (highly unlikely) we assume same as for `current` chunk */
		if (time_slice->fd.range_start >= now_internal_time)
			return FILL_FACTOR_CURRENT_CHUNK;

		/* current time falls within chunk time constraints */
		elapsed = (now_internal_time - time_slice->fd.range_start);
		interval = (time_slice->fd.range_end - time_slice->fd.range_start);

		Assert(interval != 0);

		return elapsed / interval;
	}
	else
	{
		int total_slices = get_total_number_of_slices(space);
		int num_created_after = ts_chunk_num_of_chunks_created_after(chunk, total_slices);

		/* if current chunk is the last created we assume it has 0.5 fill factor */
		return num_created_after < total_slices ? FILL_FACTOR_CURRENT_CHUNK :
												  FILL_FACTOR_HISTORICAL_CHUNK;
	}
}

typedef struct RelEstimates
{
	double tuples;
	BlockNumber pages;
} RelEstimates;

/*
 * The idea is to look into number of tuples and pages for N previous chunks
 * and calculate an average. Ideally we could add weights to this calculation
 * and give more importance to newer chunks but a ballpark estimate should be
 * just fine.
 */
static RelEstimates *
estimate_tuples_and_pages_using_prev_chunks(PlannerInfo *root, Hyperspace *space,
											Chunk *current_chunk)
{
	RelEstimates *estimates = palloc0(sizeof(RelEstimates));
	ListCell *lc;
	float4 total_tuples = 0;
	int32 total_pages = 0;
	int non_zero_reltuples_cnt = 0;
	int non_zero_relpages_cnt = 0;
	const DimensionSlice *time_slice = get_chunk_time_slice(current_chunk, space);
	List *prev_chunks = ts_chunk_get_window(time_slice->fd.dimension_id,
											time_slice->fd.range_start,
											DEFAULT_CHUNK_LOOKBACK_WINDOW,
											CurrentMemoryContext);

	foreach (lc, prev_chunks)
	{
		Chunk *pc = lfirst(lc);
		HeapTuple rel_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(pc->table_id));
		Form_pg_class rel_form;

		if (!HeapTupleIsValid(rel_tuple))
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("cache lookup failed for chunk relation %u", pc->fd.id),
					 errdetail("Failed to estimate number of tuples and pages for chunk %d.",
							   pc->table_id)));

		rel_form = (Form_pg_class) GETSTRUCT(rel_tuple);
		if (rel_form->reltuples > 0)
		{
			total_tuples += rel_form->reltuples;
			non_zero_reltuples_cnt++;
		}
		if (rel_form->relpages > 0)
		{
			total_pages += rel_form->relpages;
			non_zero_relpages_cnt++;
		}
		ReleaseSysCache(rel_tuple);
	}

	if (non_zero_reltuples_cnt > 0)
		estimates->tuples = total_tuples / non_zero_reltuples_cnt;
	if (non_zero_relpages_cnt > 0)
		estimates->pages = total_pages / non_zero_relpages_cnt;

	return estimates;
}

static RelEstimates *
estimate_tuples_and_pages_using_shared_buffers(PlannerInfo *root, Hypertable *ht, int result_width)
{
	RelEstimates *estimates = palloc(sizeof(RelEstimates));
	int64 chunk_size_estimate = ts_chunk_calculate_initial_chunk_target_size();

	if (ht != NULL)
	{
		int total_slices = get_total_number_of_slices(ht->space);
		if (total_slices > 0)
			chunk_size_estimate /= total_slices;
	}
	else
		/* half-size seems to be the safest bet */
		chunk_size_estimate /= 2;

	estimates->tuples = chunk_size_estimate / (result_width + MAXALIGN(SizeofHeapTupleHeader));
	estimates->pages = chunk_size_estimate / BLCKSZ;
	return estimates;
}

static void
set_rel_estimates(RelOptInfo *rel, RelEstimates *estimates)
{
	rel->tuples = estimates->tuples;
	rel->pages = estimates->pages;
}

static void
rel_estimates_apply_fillfactor(RelEstimates *estimates, double fillfactor)
{
	estimates->pages *= fillfactor;
	estimates->tuples *= fillfactor;
}

/*
 * When there are no local stats we try estimating by either using stats from previous chunks (if
 * they exist) or shared buffers size.
 */
static void
estimate_tuples_and_pages(PlannerInfo *root, RelOptInfo *rel)
{
	int parent_relid;
	RangeTblEntry *hyper_rte;
	Cache *hcache;
	Hypertable *ht;
	double chunk_fillfactor;
	RangeTblEntry *chunk_rte;
	Chunk *chunk;
	Hyperspace *hyperspace;
	RelEstimates *estimates;

	Assert(rel->tuples <= 0);
	Assert(rel->pages == 0);

	/* In some cases (e.g., UPDATE stmt) top_parent_relids is not set so the best
		we can do is using shared buffers size without partitioning information.
	   Since updates are not something we generaly optimize for this should be fine. */
	if (rel->top_parent_relids == NULL)
	{
		estimates =
			estimate_tuples_and_pages_using_shared_buffers(root, NULL, rel->reltarget->width);
		set_rel_estimates(rel, estimates);
		return;
	}

	parent_relid = bms_next_member(rel->top_parent_relids, -1);
	hyper_rte = planner_rt_fetch(parent_relid, root);
	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, hyper_rte->relid, CACHE_FLAG_NONE);
	hyperspace = ht->space;
	chunk_rte = planner_rt_fetch(rel->relid, root);
	chunk = ts_chunk_get_by_relid(chunk_rte->relid, true);

	/* Let's first try figuring out number of tuples/pages using stats from previous chunks,
	otherwise make an estimation based on shared buffers size */
	estimates = estimate_tuples_and_pages_using_prev_chunks(root, hyperspace, chunk);
	if (estimates->tuples <= 0 || estimates->pages == 0)
		estimates = estimate_tuples_and_pages_using_shared_buffers(root, ht, rel->reltarget->width);

	chunk_fillfactor = estimate_chunk_fillfactor(chunk, hyperspace);
	/* adjust tuples/pages using chunk_fillfactor */
	rel_estimates_apply_fillfactor(estimates, chunk_fillfactor);
	set_rel_estimates(rel, estimates);

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
	 * functions.
	 */
	fpinfo = fdw_relinfo_alloc(rel, type);

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */

	fpinfo->relation_name = makeStringInfo();
	refname = rte->eref->aliasname;
	appendStringInfoString(fpinfo->relation_name, get_relation_qualified_name(rte->relid));
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
	fpinfo->shippable_extensions = list_make1_oid(get_extension_oid(EXTENSION_NAME, true));
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

	/*
	 * If the foreign table has never been ANALYZEd, it will have relpages
	 * and reltuples equal to zero, which most likely has nothing to do
	 * with reality. Starting with PG14 it will have reltuples < 0, meaning
	 * "unknown". The best we can do is estimate number of tuples/pages.
	 */
	if (type == TS_FDW_RELINFO_FOREIGN_TABLE)
	{
		const int parent_relid = bms_next_member(rel->top_parent_relids, -1);
		if (parent_relid < 0)
		{
			if (rel->pages == 0 && rel->tuples <= 0)
			{
				estimate_tuples_and_pages(root, rel);
			}
		}
		else
		{
			RelOptInfo *parent_info = root->simple_rel_array[parent_relid];
			TsFdwRelInfo *p = fdw_relinfo_get(parent_info);
			RangeTblEntry *parent_rte = planner_rt_fetch(parent_relid, root);
			Cache *hcache = ts_hypertable_cache_pin();
			Hypertable *ht =
				ts_hypertable_cache_get_entry(hcache, parent_rte->relid, CACHE_FLAG_NONE);
			Hyperspace *hyperspace = ht->space;
			RangeTblEntry *chunk_rte = planner_rt_fetch(rel->relid, root);
			TsFdwRelInfo *chunk_private = fdw_relinfo_get(rel);
			if (!chunk_private->chunk)
			{
				Chunk *chunk =
					ts_chunk_get_by_relid(chunk_rte->relid, true /* fail_if_not_found */);
				chunk_private->chunk = chunk;
			}
			else
			{
				// FIXME figure out when this happens
			}
			double fillfactor = estimate_chunk_fillfactor(chunk_private->chunk, hyperspace);

			if (rel->pages == 0 && rel->tuples <= 0)
			{
				if (p->average_chunk_pages > 0 && p->average_chunk_tuples > 0)
				{
					rel->pages = p->average_chunk_pages * fillfactor;
					rel->tuples = p->average_chunk_tuples * fillfactor;
				}
				else
				{
					estimate_tuples_and_pages(root, rel);

					p->average_chunk_pages = rel->pages / fillfactor;
					p->average_chunk_tuples = rel->tuples / fillfactor;
				}
			}
			else
			{
				const double f = 0.1;
				p->average_chunk_pages =
					(1 - f) * p->average_chunk_pages + f * rel->pages / fillfactor;
				p->average_chunk_tuples =
					(1 - f) * p->average_chunk_tuples + f * rel->tuples / fillfactor;
			}

			ts_cache_release(hcache);
		}
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
