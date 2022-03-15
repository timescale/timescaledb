/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/tsmapi.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <commands/extension.h>
#include <executor/nodeAgg.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/appendinfo.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>
#include <utils/timestamp.h>

#include "compat/compat-msvc-enter.h"
#include <optimizer/cost.h>
#include <tcop/tcopprot.h>
#include <optimizer/plancat.h>
#include <nodes/nodeFuncs.h>
#include <parser/analyze.h>
#include <catalog/pg_constraint.h>
#include "compat/compat-msvc-exit.h"

#include <math.h>

#include "annotations.h"
#include "cross_module_fn.h"
#include "license_guc.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "utils.h"
#include "guc.h"
#include "dimension.h"
#include "nodes/chunk_dispatch_plan.h"
#include "nodes/hypertable_modify.h"
#include "nodes/constraint_aware_append/constraint_aware_append.h"
#include "nodes/chunk_append/chunk_append.h"
#include "partitioning.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "func_cache.h"
#include "chunk.h"
#include "planner.h"
#include "plan_expand_hypertable.h"
#include "plan_add_hashagg.h"
#include "plan_agg_bookend.h"
#include "plan_partialize.h"
#include "import/allpaths.h"

void _planner_init(void);
void _planner_fini(void);

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;
static void cagg_reorder_groupby_clause(RangeTblEntry *subq_rte, int rtno, List *outer_sortcl,
										List *outer_tlist);

/*
 * We mark range table entries (RTEs) in a query with TS_CTE_EXPAND if we'd like
 * to control table expansion ourselves. We exploit the ctename for this purpose
 * since it is not used for regular (base) relations.
 *
 * Note that we cannot use this mark as a general way to identify hypertable
 * RTEs. Child RTEs, for instance, will inherit this value from the parent RTE
 * during expansion. While we can prevent this happening in our custom table
 * expansion, we also have to account for the case when our custom expansion
 * is turned off with a GUC.
 */
static const char *TS_CTE_EXPAND = "ts_expand";

/*
 * Controls which type of fetcher to use to fetch data from the data nodes.
 * There is no place to store planner-global custom information (such as in
 * PlannerInfo). Because of this, we have to use the global variable that is
 * valid inside the scope of timescaledb_planner().
 * Note that that function can be called recursively, e.g. when evaluating a
 * SQL function at the planning time. We only have to determine the fetcher type
 * in the outermost scope, so we distinguish it by that the fetcher type is set
 * to the invalid value of 'auto'.
 */
DataFetcherType ts_data_node_fetcher_scan_type = AutoFetcherType;

static void
rte_mark_for_expansion(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->ctename == NULL);
	rte->ctename = (char *) TS_CTE_EXPAND;
	rte->inh = false;
}

bool
ts_rte_is_marked_for_expansion(const RangeTblEntry *rte)
{
	if (NULL == rte->ctename)
		return false;

	if (rte->ctename == TS_CTE_EXPAND)
		return true;

	return strcmp(rte->ctename, TS_CTE_EXPAND) == 0;
}

/*
 * Planner-global hypertable cache.
 *
 * Each invocation of the planner (and our hooks) should reference the same
 * cache object. Since we warm the cache when pre-processing the query (prior to
 * invoking the planner), we'd like to ensure that we use the same cache object
 * throughout the planning of that query so that we can trust that the cache
 * holds the objects it was warmed with. Since the planner can be invoked
 * recursively, we also need to stack and pop cache objects.
 */
static List *planner_hcaches = NIL;

static Cache *
planner_hcache_push(void)
{
	Cache *hcache = ts_hypertable_cache_pin();

	planner_hcaches = lcons(hcache, planner_hcaches);

	return hcache;
}

static void
planner_hcache_pop(bool release)
{
	Cache *hcache;

	Assert(list_length(planner_hcaches) > 0);

	hcache = linitial(planner_hcaches);

	if (release)
		ts_cache_release(hcache);

	planner_hcaches = list_delete_first(planner_hcaches);
}

static bool
planner_hcache_exists(void)
{
	return planner_hcaches != NIL;
}

static Cache *
planner_hcache_get(void)
{
	if (planner_hcaches == NIL)
		return NULL;

	return (Cache *) linitial(planner_hcaches);
}

/*
 * Get the Hypertable corresponding to the given relid.
 *
 * This function gets a hypertable from a pre-warmed hypertable cache. If
 * noresolve is specified (true), then it will do a cache-only lookup (i.e., it
 * will not try to scan metadata for a new entry to put in the cache). This
 * allows fast lookups during planning to also determine if something is _not_ a
 * hypertable.
 */
static Hypertable *
get_hypertable(const Oid relid, const unsigned int flags)
{
	Cache *cache = planner_hcache_get();

	if (NULL == cache)
		return NULL;

	return ts_hypertable_cache_get_entry(cache, relid, flags);
}

bool
ts_rte_is_hypertable(const RangeTblEntry *rte, bool *isdistributed)
{
	Hypertable *ht = get_hypertable(rte->relid, CACHE_FLAG_CHECK);

	if (isdistributed && ht != NULL)
		*isdistributed = hypertable_is_distributed(ht);

	return ht != NULL;
}

#define IS_UPDL_CMD(parse)                                                                         \
	((parse)->commandType == CMD_UPDATE || (parse)->commandType == CMD_DELETE)

typedef struct
{
	Query *rootquery;
	/*
	 * The number of distributed hypertables in the query and its subqueries.
	 * Specifically, we count range table entries here, so using the same
	 * distributed table twice counts as two tables. No matter whether it's the
	 * same physical table or not, the range table entries can be scanned
	 * concurrently, and more than one of them being distributed means we have
	 * to use the cursor fetcher so that these scans can be interleaved.
	 */
	int num_distributed_tables;
} PreprocessQueryContext;

/*
 * Preprocess the query tree, including, e.g., subqueries.
 *
 * Preprocessing includes:
 *
 * 1. Identifying all range table entries (RTEs) that reference
 *    hypertables. This will also warm the hypertable cache for faster lookup
 *    of both hypertables (cache hit) and non-hypertables (cache miss),
 *    without having to scan the metadata in either case.
 *
 * 2. Turning off inheritance for hypertable RTEs that we expand ourselves.
 *
 * 3. Reordering of GROUP BY clauses for continuous aggregates.
 */
static bool
preprocess_query(Node *node, PreprocessQueryContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = castNode(Query, node);
		Cache *hcache = planner_hcache_get();
		ListCell *lc;
		Index rti = 1;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			Hypertable *ht;

			switch (rte->rtekind)
			{
				case RTE_SUBQUERY:
					if (ts_guc_enable_optimizations && ts_guc_enable_cagg_reorder_groupby &&
						query->commandType == CMD_SELECT)
					{
						/* applicable to selects on continuous aggregates */
						List *outer_tlist = query->targetList;
						List *outer_sortcl = query->sortClause;
						cagg_reorder_groupby_clause(rte, rti, outer_sortcl, outer_tlist);
					}
					break;
				case RTE_RELATION:
					/* This lookup will warm the cache with all hypertables in the query */
					ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);

					if (ht)
					{
						/* Mark hypertable RTEs we'd like to expand ourselves */
						if (ts_guc_enable_optimizations && ts_guc_enable_constraint_exclusion &&
							!IS_UPDL_CMD(context->rootquery) && query->resultRelation == 0 &&
							query->rowMarks == NIL && rte->inh)
							rte_mark_for_expansion(rte);

						if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
						{
							int compr_htid = ht->fd.compressed_hypertable_id;

							/* Also warm the cache with the compressed
							 * companion hypertable */
							ht = ts_hypertable_cache_get_entry_by_id(hcache, compr_htid);
							Assert(ht != NULL);
						}

						if (hypertable_is_distributed(ht))
						{
							context->num_distributed_tables++;
						}
					}
					else
					{
						/* To properly keep track of SELECT FROM ONLY <chunk> we
						 * have to mark the rte here because postgres will set
						 * rte->inh to false (when it detects the chunk has no
						 * children which is true for all our chunks) before it
						 * reaches set_rel_pathlist hook. But chunks from queries
						 * like SELECT ..  FROM ONLY <chunk> has rte->inh set to
						 * false and other chunks have rte->inh set to true.
						 * We want to distinguish between the two cases here by
						 * marking the chunk when rte->inh is true.
						 */
						Chunk *chunk = ts_chunk_get_by_relid(rte->relid, false);
						if (chunk && rte->inh)
							rte_mark_for_expansion(rte);
					}
					break;
				default:
					break;
			}
			rti++;
		}
		return query_tree_walker(query, preprocess_query, context, 0);
	}

	return expression_tree_walker(node, preprocess_query, context);
}

static PlannedStmt *
#if PG13_GE
timescaledb_planner(Query *parse, const char *query_string, int cursor_opts,
					ParamListInfo bound_params)
#else
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
#endif
{
	PlannedStmt *stmt;
	ListCell *lc;
	bool reset_fetcher_type = false;

	/*
	 * If we are in an aborted transaction, reject all queries.
	 * While this state will not happen during normal operation it
	 * can happen when executing plpgsql procedures.
	 */
	if (IsAbortedTransactionBlockState())
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	planner_hcache_push();

	PG_TRY();
	{
		PreprocessQueryContext context = { 0 };
		context.rootquery = parse;
		if (ts_extension_is_loaded())
		{
			/*
			 * Some debug checks.
			 */
			Assert(ts_extension_oid == get_extension_oid(EXTENSION_NAME, /* missing_ok = */ false));

			/*
			 * Preprocess the hypertables in the query and warm up the caches.
			 */
			preprocess_query((Node *) parse, &context);

			/*
			 * Determine which type of fetcher to use. If set by GUC, use what
			 * is set. If the GUC says 'auto', use the row-by-row fetcher if we
			 * have at most one distributed table in the query. This enables
			 * parallel plans on data nodes, which speeds up the query.
			 * We can't use parallel plans with the cursor fetcher, because the
			 * cursors don't support parallel execution. This is because a
			 * cursor can be suspended at any time, then some arbitrary user
			 * code can be executed, and then the cursor is resumed. The
			 * parallel infrastructure doesn't have enough reentrability to
			 * survive this.
			 * We have to use a cursor fetcher when we have multiple distributed
			 * tables, because we might first have to get some rows from one
			 * table and then from another, without running either of them to
			 * completion first. This happens e.g. when doing a join. If we had
			 * a connection per table, we could avoid this requirement.
			 *
			 * Note that this function can be called recursively, e.g. when
			 * trying to evaluate an SQL function at the planning stage. We must
			 * only set/reset the fetcher type at the topmost level, that's why
			 * we check it's not already set.
			 */
			if (ts_data_node_fetcher_scan_type == AutoFetcherType)
			{
				reset_fetcher_type = true;

				if (ts_guc_remote_data_fetcher == AutoFetcherType)
				{
					if (context.num_distributed_tables >= 2)
					{
						ts_data_node_fetcher_scan_type = CursorFetcherType;
					}
					else
					{
						ts_data_node_fetcher_scan_type = RowByRowFetcherType;
					}
				}
				else
				{
					ts_data_node_fetcher_scan_type = ts_guc_remote_data_fetcher;
				}
			}
		}

		if (prev_planner_hook != NULL)
		/* Call any earlier hooks */
#if PG13_GE
			stmt = (prev_planner_hook)(parse, query_string, cursor_opts, bound_params);
#else
			stmt = (prev_planner_hook)(parse, cursor_opts, bound_params);
#endif
		else
		/* Call the standard planner */
#if PG13_GE
			stmt = standard_planner(parse, query_string, cursor_opts, bound_params);
#else
			stmt = standard_planner(parse, cursor_opts, bound_params);
#endif

		if (ts_extension_is_loaded())
		{
			/*
			 * Our top-level HypertableInsert plan node that wraps ModifyTable needs
			 * to have a final target list that is the same as the ModifyTable plan
			 * node, and we only have access to its final target list after
			 * set_plan_references() (setrefs.c) has run at the end of
			 * standard_planner. Therefore, we fixup the final target list for
			 * HypertableInsert here.
			 */
			ts_hypertable_modify_fixup_tlist(stmt->planTree);

			foreach (lc, stmt->subplans)
			{
				Plan *subplan = (Plan *) lfirst(lc);

				if (subplan)
					ts_hypertable_modify_fixup_tlist(subplan);
			}

			if (reset_fetcher_type)
			{
				ts_data_node_fetcher_scan_type = AutoFetcherType;
			}
		}
	}
	PG_CATCH();
	{
		/* Pop the cache, but do not release since caches are auto-released on
		 * error */
		planner_hcache_pop(false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	planner_hcache_pop(true);

	return stmt;
}

static RangeTblEntry *
get_parent_rte(const PlannerInfo *root, Index rti)
{
	ListCell *lc;

	/* Fast path when arrays are setup */
	if (root->append_rel_array != NULL && root->append_rel_array[rti] != NULL)
	{
		AppendRelInfo *appinfo = root->append_rel_array[rti];
		return planner_rt_fetch(appinfo->parent_relid, root);
	}

	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);

		if (appinfo->child_relid == rti)
			return planner_rt_fetch(appinfo->parent_relid, root);
	}

	return NULL;
}

/*
 * Classify a planned relation.
 *
 * This makes use of cache warming that happened during Query preprocessing in
 * the first planner hook.
 */
static TsRelType
classify_relation(const PlannerInfo *root, const RelOptInfo *rel, Hypertable **p_ht)
{
	RangeTblEntry *rte;
	RangeTblEntry *parent_rte;
	TsRelType reltype = TS_REL_OTHER;
	Hypertable *ht = NULL;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:
			rte = planner_rt_fetch(rel->relid, root);
			/*
			 * To correctly classify relations in subqueries we cannot call get_hypertable
			 * with CACHE_FLAG_CHECK which includes CACHE_FLAG_NOCREATE flag because
			 * the rel might not be in cache yet.
			 */
			ht = get_hypertable(rte->relid, CACHE_FLAG_MISSING_OK);

			if (ht != NULL)
				reltype = TS_REL_HYPERTABLE;
			else
			{
				/* This case is hit also by non-chunk BASERELs and might slow
				 * down planning since it requires a metadata scan every
				 * time. But there's probably no way around it if we are to
				 * reliably identify chunk BASERELs. We should, however, be able
				 * to identify these in the query preprocessing and cache them
				 * there if we need to speed this up. */
				Chunk *chunk = ts_chunk_get_by_relid(rte->relid, false);

				if (chunk != NULL)
				{
					reltype = TS_REL_CHUNK;
					ht = get_hypertable(chunk->hypertable_relid, CACHE_FLAG_NONE);
					Assert(ht != NULL);
					ts_chunk_free(chunk);
				}
			}
			break;
		case RELOPT_OTHER_MEMBER_REL:
			rte = planner_rt_fetch(rel->relid, root);
			parent_rte = get_parent_rte(root, rel->relid);

			/*
			 * An entry of reloptkind RELOPT_OTHER_MEMBER_REL might still
			 * be a hypertable here if it was pulled up from a subquery
			 * as happens with UNION ALL for example. So we have to
			 * check for that to properly detect that pattern.
			 */
			if (parent_rte->rtekind == RTE_SUBQUERY)
			{
				ht =
					get_hypertable(rte->relid, rte->inh ? CACHE_FLAG_MISSING_OK : CACHE_FLAG_CHECK);

				if (ht != NULL)
					reltype = TS_REL_HYPERTABLE;
			}
			else
			{
				ht = get_hypertable(parent_rte->relid, CACHE_FLAG_CHECK);

				if (ht != NULL)
				{
					if (parent_rte->relid == rte->relid)
						reltype = TS_REL_HYPERTABLE_CHILD;
					else
						reltype = TS_REL_CHUNK_CHILD;
				}
			}
			break;
		default:
			Assert(reltype == TS_REL_OTHER);
			break;
	}

	if (p_ht)
		*p_ht = ht;

	return reltype;
}

extern void ts_sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

static inline bool
should_chunk_append(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel, Path *path, bool ordered,
					int order_attno)
{
	if (root->parse->commandType != CMD_SELECT || !ts_guc_enable_chunk_append ||
		hypertable_is_distributed(ht))
		return false;

	switch (nodeTag(path))
	{
		case T_AppendPath:
			/*
			 * If there are clauses that have mutable functions, or clauses that reference
			 * Params this Path might benefit from startup or runtime exclusion
			 */
			{
				AppendPath *append = castNode(AppendPath, path);
				ListCell *lc;

				/* Don't create ChunkAppend with no children */
				if (list_length(append->subpaths) == 0)
					return false;

				foreach (lc, rel->baserestrictinfo)
				{
					RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

					if (contain_mutable_functions((Node *) rinfo->clause) ||
						ts_contain_param((Node *) rinfo->clause))
						return true;
				}
				return false;
				break;
			}
		case T_MergeAppendPath:
			/*
			 * Can we do ordered append
			 */
			{
				MergeAppendPath *merge = castNode(MergeAppendPath, path);
				PathKey *pk;

				if (!ordered || path->pathkeys == NIL || list_length(merge->subpaths) == 0)
					return false;

				pk = linitial_node(PathKey, path->pathkeys);

				/*
				 * Check PathKey is compatible with Ordered Append ordering
				 * we created when expanding hypertable.
				 * Even though ordered is true on the RelOptInfo we have to
				 * double check that current Path fulfills requirements for
				 * Ordered Append transformation because the RelOptInfo may
				 * be used for multiple Pathes.
				 */
				Expr *em_expr = find_em_expr_for_rel(pk->pk_eclass, rel);

				if (IsA(em_expr, Var) && castNode(Var, em_expr)->varattno == order_attno)
					return true;
				else if (IsA(em_expr, FuncExpr) && list_length(path->pathkeys) == 1)
				{
					FuncExpr *func = castNode(FuncExpr, em_expr);
					FuncInfo *info = ts_func_cache_get_bucketing_func(func->funcid);
					Expr *transformed;

					if (info != NULL)
					{
						transformed = info->sort_transform(func);
						if (IsA(transformed, Var) &&
							castNode(Var, transformed)->varattno == order_attno)
							return true;
					}
				}

				return false;
				break;
			}
		default:
			return false;
	}
}

static inline bool
should_constraint_aware_append(Hypertable *ht, Path *path)
{
	/* Constraint-aware append currently expects children that scans a real
	 * "relation" (e.g., not an "upper" relation). So, we do not run it on a
	 * distributed hypertable because the append children are typically
	 * per-server relations without a corresponding "real" table in the
	 * system. Further, per-server appends shouldn't need runtime pruning in any
	 * case. */
	if (hypertable_is_distributed(ht))
		return false;

	return ts_constraint_aware_append_possible(path);
}

static bool
rte_should_expand(const RangeTblEntry *rte)
{
	bool is_hypertable = ts_rte_is_hypertable(rte, NULL);

	return is_hypertable && !rte->inh && ts_rte_is_marked_for_expansion(rte);
}

static void
reenable_inheritance(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Index i;
	bool set_pathlist_for_current_rel = false;
	double total_pages;
	bool reenabled_inheritance = false;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RangeTblEntry *in_rte = root->simple_rte_array[i];

		if (rte_should_expand(in_rte))
		{
			RelOptInfo *in_rel = root->simple_rel_array[i];
			Hypertable *ht = get_hypertable(in_rte->relid, CACHE_FLAG_NOCREATE);

			Assert(ht != NULL && in_rel != NULL);
			ts_plan_expand_hypertable_chunks(ht, root, in_rel);

			in_rte->inh = true;
			reenabled_inheritance = true;
			/* Redo set_rel_consider_parallel, as results of the call may no longer be valid here
			 * (due to adding more tables to the set of tables under consideration here). This is
			 * especially true if dealing with foreign data wrappers. */

			/*
			 * An entry of reloptkind RELOPT_OTHER_MEMBER_REL might still
			 * be a hypertable here if it was pulled up from a subquery
			 * as happens with UNION ALL for example.
			 */
			if (in_rel->reloptkind == RELOPT_BASEREL ||
				in_rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
			{
				Assert(in_rte->relkind == RELKIND_RELATION);
				ts_set_rel_size(root, in_rel, i, in_rte);
			}

			/* if we're activating inheritance during a hypertable's pathlist
			 * creation then we're past the point at which postgres will add
			 * paths for the children, and we have to do it ourselves. We delay
			 * the actual setting of the pathlists until after this loop,
			 * because set_append_rel_pathlist will eventually call this hook again.
			 */
			if (in_rte == rte)
			{
				Assert(rti == i);
				set_pathlist_for_current_rel = true;
			}
		}
	}

	if (!reenabled_inheritance)
		return;

	total_pages = 0;
	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *brel = root->simple_rel_array[i];

		if (brel == NULL)
			continue;

		Assert(brel->relid == i); /* sanity check on array */

		if (IS_DUMMY_REL(brel))
			continue;

		if (IS_SIMPLE_REL(brel))
			total_pages += (double) brel->pages;
	}
	root->total_table_pages = total_pages;

	if (set_pathlist_for_current_rel)
	{
		bool do_distributed;

		Hypertable *ht = get_hypertable(rte->relid, CACHE_FLAG_NOCREATE);
		Assert(ht != NULL);

		/* the hypertable will have been planned as if it was a regular table
		 * with no data. Since such a plan would be cheaper than any real plan,
		 * it would always be used, and we need to remove these plans before
		 * adding ours.
		 *
		 * Also, if it's a distributed hypertable and per data node queries are
		 * enabled then we will be throwing this below append path away. So only
		 * build it otherwise
		 */
		do_distributed = !IS_DUMMY_REL(rel) && hypertable_is_distributed(ht) &&
						 ts_guc_enable_per_data_node_queries;

		rel->pathlist = NIL;
		rel->partial_pathlist = NIL;
		/* allow a session parameter to override the use of this datanode only path */
#ifdef TS_DEBUG
		if (do_distributed)
		{
			const char *allow_dn_path =
				GetConfigOption("timescaledb.debug_allow_datanode_only_path", true, false);
			if (allow_dn_path && pg_strcasecmp(allow_dn_path, "on") != 0)
			{
				do_distributed = false;
				elog(DEBUG2, "creating per chunk append paths");
			}
			else
				elog(DEBUG2, "avoiding per chunk append paths");
		}
#endif

		if (!do_distributed)
			ts_set_append_rel_pathlist(root, rel, rti, rte);
	}
}

static void
apply_optimizations(PlannerInfo *root, TsRelType reltype, RelOptInfo *rel, RangeTblEntry *rte,
					Hypertable *ht)
{
	if (!ts_guc_enable_optimizations)
		return;

	switch (reltype)
	{
		case TS_REL_HYPERTABLE_CHILD:
			/* empty table so nothing to optimize */
			break;
		case TS_REL_CHUNK:
		case TS_REL_CHUNK_CHILD:
			ts_sort_transform_optimization(root, rel);
			break;
		default:
			break;
	}

	/*
	 * Since the sort optimization adds new paths to the rel it has
	 * to happen before any optimizations that replace pathlist.
	 */
	if (ts_cm_functions->set_rel_pathlist_query != NULL)
		ts_cm_functions->set_rel_pathlist_query(root, rel, rel->relid, rte, ht);

	if (
		/*
		 * Right now this optimization applies only to hypertables (ht used
		 * below). Can be relaxed later to apply to reg tables but needs testing
		 */
		reltype == TS_REL_HYPERTABLE &&
		/* Do not optimize result relations (INSERT, UPDATE, DELETE) */
		0 == root->parse->resultRelation)
	{
		TimescaleDBPrivate *private = ts_get_private_reloptinfo(rel);
		bool ordered = private->appends_ordered;
		int order_attno = private->order_attno;
		List *nested_oids = private->nested_oids;
		ListCell *lc;

		Assert(ht != NULL);

		foreach (lc, rel->pathlist)
		{
			Path **pathptr = (Path **) &lfirst(lc);

			switch (nodeTag(*pathptr))
			{
				case T_AppendPath:
				case T_MergeAppendPath:
					if (should_chunk_append(ht, root, rel, *pathptr, ordered, order_attno))
						*pathptr = ts_chunk_append_path_create(root,
															   rel,
															   ht,
															   *pathptr,
															   false,
															   ordered,
															   nested_oids);
					else if (should_constraint_aware_append(ht, *pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, *pathptr);
					break;
				default:
					break;
			}
		}

		foreach (lc, rel->partial_pathlist)
		{
			Path **pathptr = (Path **) &lfirst(lc);

			switch (nodeTag(*pathptr))
			{
				case T_AppendPath:
				case T_MergeAppendPath:
					if (should_chunk_append(ht, root, rel, *pathptr, false, 0))
						*pathptr =
							ts_chunk_append_path_create(root, rel, ht, *pathptr, true, false, NIL);
					else if (should_constraint_aware_append(ht, *pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, *pathptr);
					break;
				default:
					break;
			}
		}
	}
}

static bool
valid_hook_call(void)
{
	return ts_extension_is_loaded() && planner_hcache_exists();
}

static bool
dml_involves_hypertable(PlannerInfo *root, Hypertable *ht, Index rti)
{
	Index result_rti = root->parse->resultRelation;
	RangeTblEntry *result_rte = planner_rt_fetch(result_rti, root);

	return result_rti == rti || ht->main_table_relid == result_rte->relid;
}

static void
timescaledb_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	TsRelType reltype;
	Hypertable *ht;

	/* Quick exit if this is a relation we're not interested in */
	if (!valid_hook_call() || !OidIsValid(rte->relid) || IS_DUMMY_REL(rel))
	{
		if (prev_set_rel_pathlist_hook != NULL)
			(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);
		return;
	}

	reltype = classify_relation(root, rel, &ht);

	/* Check for unexpanded hypertable */
	if (!rte->inh && ts_rte_is_marked_for_expansion(rte))
		reenable_inheritance(root, rel, rti, rte);

	/* Call other extensions. Do it after table expansion. */
	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);

	if (ts_cm_functions->set_rel_pathlist != NULL)
		ts_cm_functions->set_rel_pathlist(root, rel, rti, rte);

	switch (reltype)
	{
		case TS_REL_HYPERTABLE_CHILD:
			/* empty child is not of interest */
			break;
		case TS_REL_CHUNK:
		case TS_REL_CHUNK_CHILD:
			/* Check for UPDATE/DELETE (DLM) on compressed chunks */
			if (IS_UPDL_CMD(root->parse) && dml_involves_hypertable(root, ht, rti))
			{
				if (ts_cm_functions->set_rel_pathlist_dml != NULL)
					ts_cm_functions->set_rel_pathlist_dml(root, rel, rti, rte, ht);
				break;
			}
			TS_FALLTHROUGH;
		default:
			apply_optimizations(root, reltype, rel, rte, ht);
			break;
	}
}

/* This hook is meant to editorialize about the information the planner gets
 * about a relation. We use it to attach our own metadata to hypertable and
 * chunk relations that we need during planning. We also expand hypertables
 * here. */
static void
timescaledb_get_relation_info_hook(PlannerInfo *root, Oid relation_objectid, bool inhparent,
								   RelOptInfo *rel)
{
	Hypertable *ht;

	if (prev_get_relation_info_hook != NULL)
		prev_get_relation_info_hook(root, relation_objectid, inhparent, rel);

	if (!valid_hook_call())
		return;

	switch (classify_relation(root, rel, &ht))
	{
		case TS_REL_HYPERTABLE:
		{
			/* This only works for PG12 because for earlier versions the inheritance
			 * expansion happens too early during the planning phase
			 */
			RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
			Query *query = root->parse;
			/* Mark hypertable RTEs we'd like to expand ourselves.
			 * Hypertables inside inlineable functions don't get marked during the query
			 * preprocessing step. Therefore we do an extra try here. However, we need to
			 * be careful for UPDATE/DELETE as Postgres (in at least version 12) plans them
			 * in a complicated way (see planner.c:inheritance_planner). First, it runs the
			 * UPDATE/DELETE through the planner as a simulated SELECT. It uses the results
			 * of this fake planning to adapt its own UPDATE/DELETE plan. Then it's planned
			 * a second time as a real UPDATE/DELETE, but with requiredPerms set to 0, as it
			 * assumes permission checking has been done already during the first planner call.
			 * We don't want to touch the UPDATE/DELETEs, so we need to check all the regular
			 * conditions here that are checked during preprocess_query, as well as the
			 * condition that rte->requiredPerms is not requiring UPDATE/DELETE on this rel.
			 */
			if (ts_guc_enable_optimizations && ts_guc_enable_constraint_exclusion && inhparent &&
				rte->ctename == NULL && !IS_UPDL_CMD(query) && query->resultRelation == 0 &&
				query->rowMarks == NIL && (rte->requiredPerms & (ACL_UPDATE | ACL_DELETE)) == 0)
			{
				rte_mark_for_expansion(rte);
			}
			ts_create_private_reloptinfo(rel);
			ts_plan_expand_timebucket_annotate(root, rel);
			break;
		}
		case TS_REL_CHUNK:
		case TS_REL_CHUNK_CHILD:
		{
			ts_create_private_reloptinfo(rel);

			if (ts_guc_enable_transparent_decompression && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
			{
				RangeTblEntry *chunk_rte = planner_rt_fetch(rel->relid, root);
				Chunk *chunk = ts_chunk_get_by_relid(chunk_rte->relid, true);

				if (chunk->fd.compressed_chunk_id > 0)
				{
					Relation uncompressed_chunk = table_open(relation_objectid, NoLock);

					ts_get_private_reloptinfo(rel)->compressed = true;

					/* Planning indexes are expensive, and if this is a compressed chunk, we
					 * know we'll never need to use indexes on the uncompressed version, since
					 * all the data is in the compressed chunk anyway. Therefore, it is much
					 * faster if we simply trash the indexlist here and never plan any useless
					 * IndexPaths at all
					 */
					rel->indexlist = NIL;

					/* Relation size estimates are messed up on compressed chunks due to there
					 * being no actual pages for the table in the storage manager.
					 */
					rel->pages = (BlockNumber) uncompressed_chunk->rd_rel->relpages;
					rel->tuples = (double) uncompressed_chunk->rd_rel->reltuples;
					if (rel->pages == 0)
						rel->allvisfrac = 0.0;
					else if (uncompressed_chunk->rd_rel->relallvisible >= rel->pages)
						rel->allvisfrac = 1.0;
					else
						rel->allvisfrac =
							(double) uncompressed_chunk->rd_rel->relallvisible / rel->pages;

					table_close(uncompressed_chunk, NoLock);
				}
			}
			break;
		}
		case TS_REL_HYPERTABLE_CHILD:
		case TS_REL_OTHER:
			break;
	}
}

static bool
join_involves_hypertable(const PlannerInfo *root, const RelOptInfo *rel)
{
	int relid = -1;

	while ((relid = bms_next_member(rel->relids, relid)) >= 0)
	{
		const RangeTblEntry *rte = planner_rt_fetch(relid, root);

		if (rte != NULL)
			/* This might give a false positive for chunks in case of PostgreSQL
			 * expansion since the ctename is copied from the parent hypertable
			 * to the chunk */
			return ts_rte_is_marked_for_expansion(rte);
	}
	return false;
}

static bool
involves_hypertable(PlannerInfo *root, RelOptInfo *rel)
{
	if (rel->reloptkind == RELOPT_JOINREL)
		return join_involves_hypertable(root, rel);

	return classify_relation(root, rel, NULL) == TS_REL_HYPERTABLE;
}

/*
 * Replace INSERT (ModifyTablePath) paths on hypertables.
 *
 * From the ModifyTable description: "Each ModifyTable node contains
 * a list of one or more subplans, much like an Append node.  There
 * is one subplan per result relation."
 *
 * The subplans produce the tuples for INSERT, while the result relation is the
 * table we'd like to insert into.
 *
 * The way we redirect tuples to chunks is to insert an intermediate "chunk
 * dispatch" plan node, between the ModifyTable and its subplan that produces
 * the tuples. When the ModifyTable plan is executed, it tries to read a tuple
 * from the intermediate chunk dispatch plan instead of the original
 * subplan. The chunk plan reads the tuple from the original subplan, looks up
 * the chunk, sets the executor's resultRelation to the chunk table and finally
 * returns the tuple to the ModifyTable node.
 *
 * We also need to wrap the ModifyTable plan node with a HypertableInsert node
 * to give the ChunkDispatchState node access to the ModifyTableState node in
 * the execution phase.
 *
 * Conceptually, the plan modification looks like this:
 *
 * Original plan:
 *
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 *
 * Modified plan:
 *
 *	[ HypertableInsert ]
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^			   ^
 *		  | Tuple	  / <Set resultRelation to the matching chunk table>
 *		  |			 /
 * [ ChunkDispatch ]
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 * PG11 adds a value to the create_upper_paths_hook for FDW support. (See:
 * https://github.com/postgres/postgres/commit/7e0d64c7a57e28fbcf093b6da9310a38367c1d75).
 * Additionally, it calls the hook in a different place, once for each
 * RelOptInfo (see:
 * https://github.com/postgres/postgres/commit/c596fadbfe20ff50a8e5f4bc4b4ff5b7c302ecc0),
 * we do not change our behavior yet, but might choose to in the future.
 */
static List *
replace_hypertable_modify_paths(PlannerInfo *root, List *pathlist)
{
	List *new_pathlist = NIL;
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, ModifyTablePath))
		{
			ModifyTablePath *mt = castNode(ModifyTablePath, path);

#if PG14_GE
			/* We only route DELETEs through our CustomNode for PG 14+ because
			 * the codepath for earlier versions is different. */
			if (mt->operation == CMD_INSERT || mt->operation == CMD_DELETE)
#else
			if (mt->operation == CMD_INSERT)
#endif
			{
				RangeTblEntry *rte = planner_rt_fetch(linitial_int(mt->resultRelations), root);
				Hypertable *ht = get_hypertable(rte->relid, CACHE_FLAG_CHECK);

				if (ht && (mt->operation == CMD_INSERT || !hypertable_is_distributed(ht)))
				{
					path = ts_hypertable_modify_path_create(root, mt, ht);
				}
			}
		}

		new_pathlist = lappend(new_pathlist, path);
	}

	return new_pathlist;
}

static void
timescale_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;
	bool partials_found = false;
	TsRelType reltype = TS_REL_OTHER;
	Hypertable *ht = NULL;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (!ts_extension_is_loaded())
		return;

	if (input_rel != NULL)
		reltype = classify_relation(root, input_rel, &ht);

	if (ts_cm_functions->create_upper_paths_hook != NULL)
		ts_cm_functions
			->create_upper_paths_hook(root, stage, input_rel, output_rel, reltype, ht, extra);

	if (output_rel != NULL)
	{
		/* Modify for INSERTs on a hypertable */
		if (output_rel->pathlist != NIL)
			output_rel->pathlist = replace_hypertable_modify_paths(root, output_rel->pathlist);
		if (parse->hasAggs && stage == UPPERREL_GROUP_AGG)
		{
			/* Existing AggPaths are modified here.
			 * No new AggPaths should be added after this if there
			 * are partials. */
			partials_found = ts_plan_process_partialize_agg(root, output_rel);
		}
	}

	if (!ts_guc_enable_optimizations || input_rel == NULL || IS_DUMMY_REL(input_rel))
		return;

	if (!involves_hypertable(root, input_rel))
		return;

	if (stage == UPPERREL_GROUP_AGG && output_rel != NULL)
	{
		if (!partials_found)
			ts_plan_add_hashagg(root, input_rel, output_rel);

		if (parse->hasAggs)
			ts_preprocess_first_last_aggregates(root, root->processed_tlist);
	}
}

static bool
contain_param_exec_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
		return true;

	return expression_tree_walker(node, contain_param_exec_walker, context);
}

bool
ts_contain_param(Node *node)
{
	return contain_param_exec_walker(node, NULL);
}

static List *
fill_missing_groupclause(List *new_groupclause, List *orig_groupclause)
{
	if (new_groupclause != NIL)
	{
		ListCell *gl;
		foreach (gl, orig_groupclause)
		{
			SortGroupClause *gc = lfirst_node(SortGroupClause, gl);

			if (list_member_ptr(new_groupclause, gc))
				continue; /* already in list */
			new_groupclause = lappend(new_groupclause, gc);
		}
	}
	return new_groupclause;
}

static bool
check_cagg_view_rte(RangeTblEntry *rte)
{
	ContinuousAgg *cagg = NULL;
	ListCell *rtlc;
	bool found = false;
	Query *viewq = rte->subquery;
	Assert(rte->rtekind == RTE_SUBQUERY);

	if (list_length(viewq->rtable) != 3) /* a view has 3 entries */
	{
		return false;
	}

	/* should cache this information for cont. aggregates */
	foreach (rtlc, viewq->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, rtlc);

		if (!OidIsValid(rte->relid))
			break;

		if ((cagg = ts_continuous_agg_find_by_relid(rte->relid)) != NULL)
			found = true;
	}
	return found;
}

/* Note that it modifies the passed in Query
* select * from (select a, b, max(c), min(d) from ...
				 group by a, b)
  order by b;
* is transformed as
* SELECT * from (select a, b, max(c), min(d) from ..
*                 group by B desc, A  <------ note the change in order here
*              )
*  order by b desc;
*  we transform only if order by is a subset of group-by
* transformation is applicable only to continuous aggregates
* Parameters:
* subq_rte - rte for subquery (inner query that will be modified)
* outer_sortcl -- outer query's sort clause
* outer_tlist - outer query's target list
*/
static void
cagg_reorder_groupby_clause(RangeTblEntry *subq_rte, int rtno, List *outer_sortcl,
							List *outer_tlist)
{
	bool not_found = true;
	Query *subq;
	ListCell *lc;
	Assert(subq_rte->rtekind == RTE_SUBQUERY);
	subq = subq_rte->subquery;
	if (outer_sortcl && subq->groupClause && subq->sortClause == NIL &&
		check_cagg_view_rte(subq_rte))
	{
		List *new_groupclause = NIL;
		/* we are going to modify this. so make a copy and use it
		 if we replace */
		List *subq_groupclause_copy = copyObject(subq->groupClause);
		foreach (lc, outer_sortcl)
		{
			SortGroupClause *outer_sc = (SortGroupClause *) lfirst(lc);
			TargetEntry *outer_tle = get_sortgroupclause_tle(outer_sc, outer_tlist);
			not_found = true;
			if (IsA(outer_tle->expr, Var) && (((Var *) outer_tle->expr)->varno == rtno))
			{
				int outer_attno = ((Var *) outer_tle->expr)->varattno;
				TargetEntry *subq_tle = list_nth(subq->targetList, outer_attno - 1);
				if (subq_tle->ressortgroupref > 0)
				{
					/* get group clause corresponding to this */
					SortGroupClause *subq_gclause =
						get_sortgroupref_clause(subq_tle->ressortgroupref, subq_groupclause_copy);
					subq_gclause->sortop = outer_sc->sortop;
					subq_gclause->nulls_first = outer_sc->nulls_first;
					Assert(subq_gclause->eqop == outer_sc->eqop);
					new_groupclause = lappend(new_groupclause, subq_gclause);
					not_found = false;
				}
			}
			if (not_found)
				break;
		}
		/* all order by found in group by clause */
		if (new_groupclause != NIL && not_found == false)
		{
			/* use new groupby clause for this subquery/view */
			subq->groupClause = fill_missing_groupclause(new_groupclause, subq_groupclause_copy);
		}
	}
}

void
_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = timescaledb_planner;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = timescaledb_set_rel_pathlist;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = timescaledb_get_relation_info_hook;

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = timescale_create_upper_paths_hook;
}

void
_planner_fini(void)
{
	planner_hook = prev_planner_hook;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	get_relation_info_hook = prev_get_relation_info_hook;
	create_upper_paths_hook = prev_create_upper_paths_hook;
}
