/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/tsmapi.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <catalog/namespace.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/restrictinfo.h>
#include <utils/lsyscache.h>
#include <executor/nodeAgg.h>
#include <utils/timestamp.h>
#include <utils/selfuncs.h>
#include <access/xact.h>

#include "compat-msvc-enter.h"
#include <optimizer/cost.h>
#include <tcop/tcopprot.h>
#include <optimizer/plancat.h>
#include <nodes/nodeFuncs.h>

#include <parser/analyze.h>

#include <catalog/pg_constraint.h>
#include "compat.h"
#if PG11_LT /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#endif
#include "compat-msvc-exit.h"

#if PG12_LT
#include <optimizer/var.h>
#else
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#endif

#include <math.h>

#include "cross_module_fn.h"
#include "license_guc.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "utils.h"
#include "guc.h"
#include "dimension.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_insert.h"
#include "constraint_aware_append.h"
#include "chunk_append/chunk_append.h"
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
static bool contain_param(Node *node);
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

static void
rte_mark_for_expansion(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->ctename == NULL);
	rte->ctename = (char *) TS_CTE_EXPAND;
	rte->inh = false;
}

static bool
rte_is_marked_for_expansion(const RangeTblEntry *rte)
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
get_hypertable(Oid relid, bool noresolve)
{
	Cache *cache = planner_hcache_get();

	if (NULL == cache)
		return NULL;

	if (noresolve)
		return ts_hypertable_cache_get_entry_no_resolve(cache, relid);
	return ts_hypertable_cache_get_entry(cache, relid, false);
}

bool
ts_rte_is_hypertable(const RangeTblEntry *rte)
{
	return get_hypertable(rte->relid, true) != NULL;
}

#define IS_UPDL_CMD(parse)                                                                         \
	((parse)->commandType == CMD_UPDATE || (parse)->commandType == CMD_DELETE)

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
preprocess_query(Node *node, Query *rootquery)
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
					if (!ts_guc_disable_optimizations && ts_guc_enable_cagg_reorder_groupby &&
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
					ht = ts_hypertable_cache_get_entry(hcache, rte->relid, true);

					if (NULL != ht)
					{
						/* Mark hypertable RTEs we'd like to expand ourselves */
						if (!ts_guc_disable_optimizations && ts_guc_enable_constraint_exclusion &&
							!IS_UPDL_CMD(rootquery) && query->resultRelation == 0 &&
							query->rowMarks == NIL && rte->inh)
							rte_mark_for_expansion(rte);

						if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
						{
							int compr_htid = ht->fd.compressed_hypertable_id;

							/* Also warm the cache with the compressed
							 * companion hypertable */
							ht = ts_hypertable_cache_get_entry_by_id(hcache, compr_htid);
							Assert(ht != NULL);
						}
					}
					break;
				default:
					break;
			}
			rti++;
		}
		return query_tree_walker(query, preprocess_query, rootquery, 0);
	}

	return expression_tree_walker(node, preprocess_query, rootquery);
}

static PlannedStmt *
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
{
	PlannedStmt *stmt;
	ListCell *lc;

	planner_hcache_push();

	PG_TRY();
	{
		if (ts_extension_is_loaded())
			preprocess_query((Node *) parse, parse);

		if (prev_planner_hook != NULL)
			/* Call any earlier hooks */
			stmt = (prev_planner_hook)(parse, cursor_opts, bound_params);
		else
			/* Call the standard planner */
			stmt = standard_planner(parse, cursor_opts, bound_params);

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
			ts_hypertable_insert_fixup_tlist(stmt->planTree);

			foreach (lc, stmt->subplans)
			{
				Plan *subplan = (Plan *) lfirst(lc);

				ts_hypertable_insert_fixup_tlist(subplan);
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

#if PG11_GE
	/* Fast path when arrays are setup */
	if (root->append_rel_array != NULL && root->append_rel_array[rti] != NULL)
	{
		AppendRelInfo *appinfo = root->append_rel_array[rti];
		return planner_rt_fetch(appinfo->parent_relid, root);
	}
#endif

	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);

		if (appinfo->child_relid == rti)
			return planner_rt_fetch(appinfo->parent_relid, root);
	}

	return NULL;
}

/*
 * TsRelType provides consistent classification of planned relations across
 * planner hooks.
 */
typedef enum TsRelType
{
	TS_REL_HYPERTABLE,		 /* A hypertable with no parent */
	TS_REL_CHUNK,			 /* Chunk with no parent (i.e., it's part of the
							  * plan as a standalone table. For example,
							  * querying the chunk directly and not via the
							  * parent hypertable). */
	TS_REL_HYPERTABLE_CHILD, /* Self child. With PostgreSQL's table expansion,
							  * the root table is expanded as a child of
							  * itself. This happens when our expansion code
							  * is turned off. */
	TS_REL_CHUNK_CHILD,		 /* Chunk with parent and the result of table
							  * expansion. */
	TS_REL_OTHER,			 /* Anything which is none of the above */
} TsRelType;

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
			ht = get_hypertable(rte->relid, true);

			if (NULL != ht)
				reltype = TS_REL_HYPERTABLE;
			else
			{
				/* This case is hit also by non-chunk BASERELs and might slow
				 * down planning since it requires a metadata scan every
				 * time. But there's probably no way around it if we are to
				 * reliably identify chunk BASERELs. We should, however, be able
				 * to identify these in the query preprocessing and cache them
				 * there if we need to speed this up. */
				Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, false);

				if (NULL != chunk)
				{
					reltype = TS_REL_CHUNK;
					ht = get_hypertable(chunk->hypertable_relid, false);
					Assert(ht != NULL);
				}
			}
			break;
		case RELOPT_OTHER_MEMBER_REL:
			rte = planner_rt_fetch(rel->relid, root);
			parent_rte = get_parent_rte(root, rel->relid);
			ht = get_hypertable(parent_rte->relid, true);

			if (NULL != ht)
			{
				if (parent_rte->relid == rte->relid)
					reltype = TS_REL_HYPERTABLE_CHILD;
				else
					reltype = TS_REL_CHUNK_CHILD;
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
should_optimize_append(const Path *path)
{
	RelOptInfo *rel = path->parent;
	ListCell *lc;

	if (!ts_guc_constraint_aware_append || constraint_exclusion == CONSTRAINT_EXCLUSION_OFF)
		return false;

	/*
	 * If there are clauses that have mutable functions, this path is ripe for
	 * execution-time optimization.
	 */
	foreach (lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (contain_mutable_functions((Node *) rinfo->clause))
			return true;
	}
	return false;
}

static inline bool
should_chunk_append(PlannerInfo *root, RelOptInfo *rel, Path *path, bool ordered, int order_attno)
{
	if (root->parse->commandType != CMD_SELECT || !ts_guc_enable_chunk_append)
		return false;

	switch (nodeTag(path))
	{
		case T_AppendPath:
			/*
			 * If there are clauses that have mutable functions, or clauses that reference
			 * Params this Path might benefit from startup or runtime exclusion
			 */
			{
				ListCell *lc;

				foreach (lc, rel->baserestrictinfo)
				{
					RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

					if (contain_mutable_functions((Node *) rinfo->clause) ||
						contain_param((Node *) rinfo->clause))
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
				PathKey *pk;
				ListCell *lc;

				if (!ordered || path->pathkeys == NIL)
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
				foreach (lc, pk->pk_eclass->ec_members)
				{
					EquivalenceMember *em = lfirst(lc);
					if (!em->em_is_child)
					{
						if (IsA(em->em_expr, Var) &&
							castNode(Var, em->em_expr)->varattno == order_attno)
							return true;
						else if (IsA(em->em_expr, FuncExpr) && list_length(path->pathkeys) == 1)
						{
							FuncExpr *func = castNode(FuncExpr, em->em_expr);
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
					}
				}
				return false;
				break;
			}
		default:
			return false;
	}
}

#if PG12_GE

static bool
rte_should_expand(const RangeTblEntry *rte)
{
	bool is_hypertable = ts_rte_is_hypertable(rte);

	return is_hypertable && !rte->inh && rte_is_marked_for_expansion(rte);
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
			Hypertable *ht = get_hypertable(in_rte->relid, true);

			Assert(ht != NULL);
			ts_plan_expand_hypertable_chunks(ht, root, in_rel);

			/* TODO move this back into ts_plan_expand_hypertable_chunks */
			in_rte->inh = true;
			reenabled_inheritance = true;
			/* FIXME redo set_rel_consider_parallel */
			if (in_rel != NULL && in_rel->reloptkind == RELOPT_BASEREL)
				ts_set_rel_size(root, in_rel, i, in_rte);

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
		/* the hypertable will have been planned as if it was a regular table
		 * with no data. Since such a plan would be cheaper than any real plan,
		 * it would always be used, and we need to remove these plans before
		 * adding ours.
		 */
		rel->pathlist = NIL;
		rel->partial_pathlist = NIL;
		ts_set_append_rel_pathlist(root, rel, rti, rte);
	}
}
#endif /* PG12_GE */

static void
apply_optimizations(PlannerInfo *root, TsRelType reltype, RelOptInfo *rel, RangeTblEntry *rte,
					Hypertable *ht)
{
	if (ts_guc_disable_optimizations)
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
			if (ts_guc_optimize_non_hypertables)
				ts_sort_transform_optimization(root, rel);
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
					if (should_chunk_append(root, rel, *pathptr, ordered, order_attno))
						*pathptr = ts_chunk_append_path_create(root,
															   rel,
															   ht,
															   *pathptr,
															   false,
															   ordered,
															   nested_oids);
					else if (should_optimize_append(*pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, ht, *pathptr);
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
					if (should_chunk_append(root, rel, *pathptr, false, 0))
						*pathptr =
							ts_chunk_append_path_create(root, rel, ht, *pathptr, true, false, NIL);
					else if (should_optimize_append(*pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, ht, *pathptr);
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

#if PG12_GE
	/* Check for unexpanded hypertable */
	if (!rte->inh && rte_is_marked_for_expansion(rte))
		reenable_inheritance(root, rel, rti, rte);
#endif

	/* Call other extensions. Do it after table expansion. */
	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);

	switch (reltype)
	{
		case TS_REL_HYPERTABLE_CHILD:
			/* empty child is not of interest */
			break;
		case TS_REL_CHUNK:
		case TS_REL_CHUNK_CHILD:
			/* Check for UPDATE/DELETE (DLM) on compressed chunks */
			if (IS_UPDL_CMD(root->parse))
			{
				if (ts_cm_functions->set_rel_pathlist_dml != NULL)
					ts_cm_functions->set_rel_pathlist_dml(root, rel, rti, rte, ht);
				break;
			}
			/* Fall through */
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
			ts_create_private_reloptinfo(rel);
#if PG12_GE
			/* in earlier versions this is done during expand_hypertable_inheritance() below */
			ts_plan_expand_timebucket_annotate(root, rel);
#else
			if (ts_guc_enable_constraint_exclusion && rel->relid != root->parse->resultRelation)
			{
				RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

				if (rte_is_marked_for_expansion(rte) && !inhparent)
					ts_plan_expand_hypertable_chunks(ht, root, rel);
			}
#endif
			break;
		case TS_REL_CHUNK:
		case TS_REL_CHUNK_CHILD:
		{
			ts_create_private_reloptinfo(rel);

			if (ts_guc_enable_transparent_decompression && TS_HYPERTABLE_HAS_COMPRESSION(ht))
			{
				RangeTblEntry *chunk_rte = planner_rt_fetch(rel->relid, root);
				Chunk *chunk = ts_chunk_get_by_relid(chunk_rte->relid, 0, true);

				if (chunk->fd.compressed_chunk_id > 0)
				{
					ts_get_private_reloptinfo(rel)->compressed = true;

					/* Planning indexes are expensive, and if this is a compressed chunk, we
					 * know we'll never need to us indexes on the uncompressed version, since
					 * all the data is in the compressed chunk anyway. Therefore, it is much
					 * faster if we simply trash the indexlist here and never plan any useless
					 * IndexPaths at all
					 */
					rel->indexlist = NIL;
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
			return rte_is_marked_for_expansion(rte);
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
replace_hypertable_insert_paths(PlannerInfo *root, List *pathlist)
{
	List *new_pathlist = NIL;
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, ModifyTablePath) && ((ModifyTablePath *) path)->operation == CMD_INSERT)
		{
			ModifyTablePath *mt = (ModifyTablePath *) path;
			RangeTblEntry *rte = planner_rt_fetch(linitial_int(mt->resultRelations), root);
			Hypertable *ht = get_hypertable(rte->relid, true);

			if (NULL != ht)
				path = ts_hypertable_insert_path_create(root, mt);
		}

		new_pathlist = lappend(new_pathlist, path);
	}

	return new_pathlist;
}

static void
#if PG11_LT
timescale_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel)
{
	Query *parse = root->parse;
	bool partials_found = false;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel);
#else
timescale_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;
	bool partials_found = false;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);
#endif

	if (!ts_extension_is_loaded())
		return;

	if (ts_cm_functions->create_upper_paths_hook != NULL)
		ts_cm_functions->create_upper_paths_hook(root, stage, input_rel, output_rel);

	if (output_rel != NULL)
	{
		/* Modify for INSERTs on a hypertable */
		if (output_rel->pathlist != NIL)
			output_rel->pathlist = replace_hypertable_insert_paths(root, output_rel->pathlist);
		if (parse->hasAggs && stage == UPPERREL_GROUP_AGG)
		{
			/* existing AggPaths are modified here.
			 * No new AggPaths should be added after this if there
			 * are partials*/
			partials_found = ts_plan_process_partialize_agg(root, input_rel, output_rel);
		}
	}

	if (ts_guc_disable_optimizations || input_rel == NULL || IS_DUMMY_REL(input_rel))
		return;

	if (!ts_guc_optimize_non_hypertables && !involves_hypertable(root, input_rel))
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

static bool
contain_param(Node *node)
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
		char *schema;
		char *table;

		if (!OidIsValid(rte->relid))
			break;

		schema = get_namespace_name(get_rel_namespace(rte->relid));
		table = get_rel_name(rte->relid);
		if ((cagg = ts_continuous_agg_find_by_view_name(schema, table)) != NULL)
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
