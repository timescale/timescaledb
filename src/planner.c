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
#include <utils/guc.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/restrictinfo.h>
#include <utils/lsyscache.h>
#include <executor/nodeAgg.h>
#include <utils/timestamp.h>
#include <utils/selfuncs.h>

#include "compat-msvc-enter.h"
#include <optimizer/cost.h>
#include <tcop/tcopprot.h>
#include <optimizer/plancat.h>
#include <nodes/nodeFuncs.h>

#include <catalog/pg_constraint.h>
#include "compat.h"
#if PG11_LT /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#endif
#include "compat-msvc-exit.h"

#if PG12_LT
#include <optimizer/var.h> /* f09346a */
#elif PG12_GE
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#endif

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

void _planner_init(void);
void _planner_fini(void);

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;
static bool contain_param(Node *node);
static void expand_hypertable_inheritance(PlannerInfo *root, Oid relation_objectid, bool inhparent, RelOptInfo *rel);

#define CTE_NAME_HYPERTABLES "hypertable_parent"

static uint64 inheritance_disabled_counter = 0;
static uint64 inheritance_reenabled_counter = 0;

static void
mark_rte_hypertable_parent(RangeTblEntry *rte)
{
	/*
	 * CTE name is never used for regular tables so use that as a signal that
	 * the rte is a hypertable.
	 */
	Assert(rte->ctename == NULL);
	rte->ctename = CTE_NAME_HYPERTABLES;
}

bool
is_rte_hypertable(RangeTblEntry *rte)
{
	return rte->ctename != NULL && strcmp(rte->ctename, CTE_NAME_HYPERTABLES) == 0;
}

/* This turns off inheritance on hypertables where we will do chunk
 * expansion ourselves. This prevents postgres from expanding the inheritance
 * tree itself. We will expand the chunks in expand_hypertable_inheritance. */
static bool
turn_off_inheritance_walker(Node *node, Cache *hc)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *lc;
		int rti = 1;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst(lc);

			if (rte->inh)
			{
				Hypertable *ht = ts_hypertable_cache_get_entry(hc, rte->relid);

				if (NULL != ht && ts_plan_expand_hypertable_valid_hypertable(ht, query, rti, rte))
				{
					if (inheritance_disabled_counter <= inheritance_reenabled_counter)
						inheritance_disabled_counter = inheritance_reenabled_counter + 1;
					rte->inh = false;
					mark_rte_hypertable_parent(rte);
				}
			}
			rti++;
		}

		return query_tree_walker(query, turn_off_inheritance_walker, hc, 0);
	}

	return expression_tree_walker(node, turn_off_inheritance_walker, hc);
}

static PlannedStmt *
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
{
	PlannedStmt *stmt;
	ListCell *lc;

	if (ts_extension_is_loaded() && !ts_guc_disable_optimizations &&
		ts_guc_enable_constraint_exclusion &&
		(parse->commandType == CMD_INSERT || parse->commandType == CMD_SELECT))
	{
		Cache *hc = ts_hypertable_cache_pin();

		/*
		 * turn of inheritance on hypertables we will expand ourselves in
		 * expand_hypertable_inheritance
		 */
		turn_off_inheritance_walker((Node *) parse, hc);

		ts_cache_release(hc);
	}

	if (prev_planner_hook != NULL)
		/* Call any earlier hooks */
		stmt = (prev_planner_hook)(parse, cursor_opts, bound_params);
	else
		/* Call the standard planner */
		stmt = standard_planner(parse, cursor_opts, bound_params);

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
	return stmt;
}

static inline bool
should_optimize_query(Hypertable *ht)
{
	return !ts_guc_disable_optimizations && (ts_guc_optimize_non_hypertables || ht != NULL);
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

static inline bool
is_append_child(RelOptInfo *rel, RangeTblEntry *rte)
{
	return rel->reloptkind == RELOPT_OTHER_MEMBER_REL && rte->inh == false &&
		   rel->rtekind == RTE_RELATION && rte->relkind == RELKIND_RELATION;
}

static inline bool
is_append_parent(RelOptInfo *rel, RangeTblEntry *rte)
{
	return rel->reloptkind == RELOPT_BASEREL && rte->inh == true && rel->rtekind == RTE_RELATION &&
		   rte->relkind == RELKIND_RELATION;
}

static Oid
get_parentoid(PlannerInfo *root, Index rti)
{
#if PG11_LT
	ListCell *lc;
	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		if (appinfo->child_relid == rti)
			return appinfo->parent_reloid;
	}
#else
	if (root->append_rel_array[rti])
		return root->append_rel_array[rti]->parent_reloid;
#endif
	return 0;
}

/* is this a hypertable's chunk involved in DML
 * returns the hypertable's Oid if so, otherwise returns 0
 * used only for updates and deletes for compression now */
static Oid
is_hypertable_chunk_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	if (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)
	{
#if PG12_LT
		Oid parent_oid;
		AppendRelInfo *appinfo = ts_get_appendrelinfo(root, rti, true);
		if (!appinfo)
			return false;
		parent_oid = appinfo->parent_reloid;
		if (parent_oid != InvalidOid && rte->relid != parent_oid)
		{
			Cache *hcache = ts_hypertable_cache_pin();
			Hypertable *parent_ht = ts_hypertable_cache_get_entry(hcache, parent_oid);
			ts_cache_release(hcache);
			if (parent_ht)
				return parent_oid;
		}
#else
		/* In PG12 UPDATE/DELETE on inheritance relations are planned in two
		 * stages. In stage 1, the statement is planned as if it was a SELECT
		 * and all leaf tables are discovered. In stage 2, the original query
		 * is planned against each leaf table, discovered in stage 1, directly,
		 * not part of an Append. Unfortunately, this means we cannot look in
		 * the appendrelinfo to determine if a table is a chunk as the
		 * appendrelinfo is not initialized.
		 * (see https://github.com/postgres/postgres/blob/REL_12_1/src/backend/optimizer/plan/planner.c#L1281-L1291
		 *  for more details)
		 * instead, we'll look in the chunk catalog for now.
		 */
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, false);
		if (chunk != NULL)
			return chunk->hypertable_relid;
#endif
	}
	return 0;
}

static void
timescaledb_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti,
								   RangeTblEntry *rte, Hypertable *ht)
{
	if (!should_optimize_query(ht))
		return;

	/*
	 * Since the sort optimization adds new paths to the rel it has
	 * to happen before any optimizations that replace pathlist.
	 */
	if (ts_guc_optimize_non_hypertables || (ht != NULL && is_append_child(rel, rte)))
		ts_sort_transform_optimization(root, rel);

	if (ts_cm_functions->set_rel_pathlist_query != NULL)
		ts_cm_functions->set_rel_pathlist_query(root, rel, rti, rte, ht);

	if (
		/*
		 * Right now this optimization applies only to hypertables (ht used
		 * below). Can be relaxed later to apply to reg tables but needs testing
		 */
		ht != NULL && is_append_parent(rel, rte) &&
		/* Do not optimize result relations (INSERT, UPDATE, DELETE) */
		0 == root->parse->resultRelation)
	{
		ListCell *lc;
		bool ordered = false;
		int order_attno = 0;
		List *nested_oids = NIL;

		if (rel->fdw_private != NULL)
		{
			TimescaleDBPrivate *private = (TimescaleDBPrivate *) rel->fdw_private;

			ordered = private->appends_ordered;
			order_attno = private->order_attno;
			nested_oids = private->nested_oids;
		}

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
	return;
}

#if PG12

static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);


/* copied from allpaths.c */
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Call the FDW's GetForeignPaths function to generate path(s) */
	rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
}


/* copied from allpaths.c */
static void
set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	Path	   *path;

	/*
	 * We don't support pushing join clauses into the quals of a samplescan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in its tlist or TABLESAMPLE arguments.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sampled scan */
	path = create_samplescan_path(root, rel, required_outer);

	/*
	 * If the sampling method does not support repeatable scans, we must avoid
	 * plans that would scan the rel multiple times.  Ideally, we'd simply
	 * avoid putting the rel on the inside of a nestloop join; but adding such
	 * a consideration to the planner seems like a great deal of complication
	 * to support an uncommon usage of second-rate sampling methods.  Instead,
	 * if there is a risk that the query might perform an unsafe join, just
	 * wrap the SampleScan in a Materialize node.  We can check for joins by
	 * counting the membership of all_baserels (note that this correctly
	 * counts inheritance trees as single rels).  If we're inside a subquery,
	 * we can't easily check whether a join might occur in the outer query, so
	 * just assume one is possible.
	 *
	 * GetTsmRoutine is relatively expensive compared to the other tests here,
	 * so check repeatable_across_scans last, even though that's a bit odd.
	 */
	if ((root->query_level > 1 ||
		 bms_membership(root->all_baserels) != BMS_SINGLETON) &&
		!(GetTsmRoutine(rte->tablesample->tsmhandler)->repeatable_across_scans))
	{
		path = (Path *) create_material_path(rel, path);
	}

	add_path(rel, path);

	/* For the moment, at least, there are no other paths to consider */
}

/* copied from allpaths.c */
static void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int			parallel_workers;

	parallel_workers = compute_parallel_worker(rel, rel->pages, -1,
											   max_parallel_workers_per_gather);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return;

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));
}

/* copied from allpaths.c */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	add_path(rel, create_seqscan_path(root, rel, required_outer, 0));

	/* If appropriate, consider parallel sequential scan */
	if (rel->consider_parallel && required_outer == NULL)
		create_plain_partial_paths(root, rel);

	/* Consider index scans */
	create_index_paths(root, rel);

	/* Consider TID scans */
	create_tidscan_paths(root, rel);
}

/* copied from allpaths.c */
static void
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	List	   *live_childrels = NIL;
	ListCell   *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * non-dummy children.
	 */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/*
		 * If set_append_rel_size() decided the parent appendrel was
		 * parallel-unsafe at some point after visiting this child rel, we
		 * need to propagate the unsafety marking down to the child, so that
		 * we don't generate useless partial paths for it.
		 */
		if (!rel->consider_parallel)
			childrel->consider_parallel = false;

		/*
		 * Compute the child's access paths.
		 */
		set_rel_pathlist(root, childrel, childRTindex, childRTE);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* Bubble up childrel's partitioned children. */
		if (rel->part_scheme)
			rel->partitioned_child_rels =
				list_concat(rel->partitioned_child_rels,
							list_copy(childrel->partitioned_child_rels));

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);
	}

	/* Add paths to the append relation. */
	add_paths_to_append_rel(root, rel, live_childrels);
}

/* based on the function in allpaths.c, with the irrelevant branches removed */
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 Index rti, RangeTblEntry *rte)
{
	if (IS_DUMMY_REL(rel))
	{
		/* We already proved the relation empty, so nothing more to do */
	}
	else
	{
		Assert(!rte->inh);
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_pathlist(root, rel, rte);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_pathlist(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_pathlist(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
			case RTE_RESULT:
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * Allow a plugin to editorialize on the set of Paths for this base
	 * relation.  It could add new paths (such as CustomPaths) by calling
	 * add_path(), or add_partial_path() if parallel aware.  It could also
	 * delete or modify paths added by the core code.
	 */
	if (set_rel_pathlist_hook)
		(*set_rel_pathlist_hook) (root, rel, rti, rte);

	/*
	 * If this is a baserel, we should normally consider gathering any partial
	 * paths we may have created for it.  We have to do this after calling the
	 * set_rel_pathlist_hook, else it cannot add partial paths to be included
	 * here.
	 *
	 * However, if this is an inheritance child, skip it.  Otherwise, we could
	 * end up with a very large number of gather nodes, each trying to grab
	 * its own pool of workers.  Instead, we'll consider gathering partial
	 * paths for the parent appendrel.
	 *
	 * Also, if this is the topmost scan/join rel (that is, the only baserel),
	 * we postpone gathering until the final scan/join targetlist is available
	 * (see grouping_planner).
	 */
	if (rel->reloptkind == RELOPT_BASEREL &&
		bms_membership(root->all_baserels) != BMS_SINGLETON)
		generate_gather_paths(root, rel, false);

	/* Now find the cheapest of the paths for this rel */
	set_cheapest(rel);

#ifdef OPTIMIZER_DEBUG
	debug_print_rel(root, rel);
#endif
}

/* copied from allpaths.c */
static void
set_dummy_rel_pathlist(RelOptInfo *rel)
{
	/* Set dummy size estimates --- we leave attr_widths[] as zeroes */
	rel->rows = 0;
	rel->reltarget->width = 0;

	/* Discard any pre-existing paths; no further need for them */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;

	/* Set up the dummy path */
	add_path(rel, (Path *) create_append_path(NULL, rel, NIL, NIL,
											  NIL, rel->lateral_relids,
											  0, false, NIL, -1));

	/*
	 * We set the cheapest-path fields immediately, just in case they were
	 * pointing at some discarded path.  This is redundant when we're called
	 * from set_rel_size(), but not when called from elsewhere, and doing it
	 * twice is harmless anyway.
	 */
	set_cheapest(rel);
}

static void set_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);

/* copied from allpaths.c */
static void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte)
{
	/*
	 * The flag has previously been initialized to false, so we can just
	 * return if it becomes clear that we can't safely set it.
	 */
	Assert(!rel->consider_parallel);

	/* Don't call this if parallelism is disallowed for the entire query. */
	Assert(root->glob->parallelModeOK);

	/* This should only be called for baserels and appendrel children. */
	Assert(IS_SIMPLE_REL(rel));

	/* Assorted checks based on rtekind. */
	switch (rte->rtekind)
	{
		case RTE_RELATION:

			/*
			 * Currently, parallel workers can't access the leader's temporary
			 * tables.  We could possibly relax this if the wrote all of its
			 * local buffers at the start of the query and made no changes
			 * thereafter (maybe we could allow hint bit changes), and if we
			 * taught the workers to read them.  Writing a large number of
			 * temporary buffers could be expensive, though, and we don't have
			 * the rest of the necessary infrastructure right now anyway.  So
			 * for now, bail out if we see a temporary table.
			 */
			if (get_rel_persistence(rte->relid) == RELPERSISTENCE_TEMP)
				return;

			/*
			 * Table sampling can be pushed down to workers if the sample
			 * function and its arguments are safe.
			 */
			if (rte->tablesample != NULL)
			{
				char		proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (!is_parallel_safe(root, (Node *) rte->tablesample->args))
					return;
			}

			/*
			 * Ask FDWs whether they can support performing a ForeignScan
			 * within a worker.  Most often, the answer will be no.  For
			 * example, if the nature of the FDW is such that it opens a TCP
			 * connection with a remote server, each parallel worker would end
			 * up with a separate connection, and these connections might not
			 * be appropriately coordinated between workers and the leader.
			 */
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				Assert(rel->fdwroutine);
				if (!rel->fdwroutine->IsForeignScanParallelSafe)
					return;
				if (!rel->fdwroutine->IsForeignScanParallelSafe(root, rel, rte))
					return;
			}

			/*
			 * There are additional considerations for appendrels, which we'll
			 * deal with in set_append_rel_size and set_append_rel_pathlist.
			 * For now, just set consider_parallel based on the rel's own
			 * quals and targetlist.
			 */
			break;

		case RTE_SUBQUERY:

			/*
			 * There's no intrinsic problem with scanning a subquery-in-FROM
			 * (as distinct from a SubPlan or InitPlan) in a parallel worker.
			 * If the subquery doesn't happen to have any parallel-safe paths,
			 * then flagging it as consider_parallel won't change anything,
			 * but that's true for plain tables, too.  We must set
			 * consider_parallel based on the rel's own quals and targetlist,
			 * so that if a subquery path is parallel-safe but the quals and
			 * projection we're sticking onto it are not, we correctly mark
			 * the SubqueryScanPath as not parallel-safe.  (Note that
			 * set_subquery_pathlist() might push some of these quals down
			 * into the subquery itself, but that doesn't change anything.)
			 *
			 * We can't push sub-select containing LIMIT/OFFSET to workers as
			 * there is no guarantee that the row order will be fully
			 * deterministic, and applying LIMIT/OFFSET will lead to
			 * inconsistent results at the top-level.  (In some cases, where
			 * the result is ordered, we could relax this restriction.  But it
			 * doesn't currently seem worth expending extra effort to do so.)
			 */
			{
				Query	   *subquery = castNode(Query, rte->subquery);

				if (limit_needed(subquery))
					return;
			}
			break;

		case RTE_JOIN:
			/* Shouldn't happen; we're only considering baserels here. */
			Assert(false);
			return;

		case RTE_FUNCTION:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->functions))
				return;
			break;

		case RTE_TABLEFUNC:
			/* not parallel safe */
			return;

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->values_lists))
				return;
			break;

		case RTE_CTE:

			/*
			 * CTE tuplestores aren't shared among parallel workers, so we
			 * force all CTE scans to happen in the leader.  Also, populating
			 * the CTE would require executing a subplan that's not available
			 * in the worker, might be parallel-restricted, and must get
			 * executed only once.
			 */
			return;

		case RTE_NAMEDTUPLESTORE:

			/*
			 * tuplestore cannot be shared, at least without more
			 * infrastructure to support that.
			 */
			return;

		case RTE_RESULT:
			/* RESULT RTEs, in themselves, are no problem. */
			break;
	}

	/*
	 * If there's anything in baserestrictinfo that's parallel-restricted, we
	 * give up on parallelizing access to this relation.  We could consider
	 * instead postponing application of the restricted quals until we're
	 * above all the parallelism in the plan tree, but it's not clear that
	 * that would be a win in very many cases, and it might be tricky to make
	 * outer join clauses work correctly.  It would likely break equivalence
	 * classes, too.
	 */
	if (!is_parallel_safe(root, (Node *) rel->baserestrictinfo))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (!is_parallel_safe(root, (Node *) rel->reltarget->exprs))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}

/* copied from allpaths.c */
static void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
					Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	bool		has_live_children;
	double		parent_rows;
	double		parent_size;
	double	   *parent_attrsizes;
	int			nattrs;
	ListCell   *l;

	/* Guard against stack overflow due to overly deep inheritance tree. */
	check_stack_depth();

	Assert(IS_SIMPLE_REL(rel));

	/*
	 * Initialize partitioned_child_rels to contain this RT index.
	 *
	 * Note that during the set_append_rel_pathlist() phase, we will bubble up
	 * the indexes of partitioned relations that appear down in the tree, so
	 * that when we've created Paths for all the children, the root
	 * partitioned table's list will contain all such indexes.
	 */
	if (rte->relkind == RELKIND_PARTITIONED_TABLE)
		rel->partitioned_child_rels = list_make1_int(rti);

	/*
	 * If this is a partitioned baserel, set the consider_partitionwise_join
	 * flag; currently, we only consider partitionwise joins with the baserel
	 * if its targetlist doesn't contain a whole-row Var.
	 */
	if (enable_partitionwise_join &&
		rel->reloptkind == RELOPT_BASEREL &&
		rte->relkind == RELKIND_PARTITIONED_TABLE &&
		rel->attr_needed[InvalidAttrNumber - rel->min_attr] == NULL)
		rel->consider_partitionwise_join = true;

	/*
	 * Initialize to compute size estimates for whole append relation.
	 *
	 * We handle width estimates by weighting the widths of different child
	 * rels proportionally to their number of rows.  This is sensible because
	 * the use of width estimates is mainly to compute the total relation
	 * "footprint" if we have to sort or hash it.  To do this, we sum the
	 * total equivalent size (in "double" arithmetic) and then divide by the
	 * total rowcount estimate.  This is done separately for the total rel
	 * width and each attribute.
	 *
	 * Note: if you consider changing this logic, beware that child rels could
	 * have zero rows and/or width, if they were excluded by constraints.
	 */
	has_live_children = false;
	parent_rows = 0;
	parent_size = 0;
	nattrs = rel->max_attr - rel->min_attr + 1;
	parent_attrsizes = (double *) palloc0(nattrs * sizeof(double));

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;
		ListCell   *parentvars;
		ListCell   *childvars;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];

		/*
		 * The child rel's RelOptInfo was already created during
		 * add_other_rels_to_query.
		 */
		childrel = find_base_rel(root, childRTindex);
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/* We may have already proven the child to be dummy. */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * We have to copy the parent's targetlist and quals to the child,
		 * with appropriate substitution of variables.  However, the
		 * baserestrictinfo quals were already copied/substituted when the
		 * child RelOptInfo was built.  So we don't need any additional setup
		 * before applying constraint exclusion.
		 */
		if (relation_excluded_by_constraints(root, childrel, childRTE))
		{
			/*
			 * This child need not be scanned, so we can omit it from the
			 * appendrel.
			 */
			set_dummy_rel_pathlist(childrel);
			continue;
		}

		/*
		 * Constraint exclusion failed, so copy the parent's join quals and
		 * targetlist to the child, with appropriate variable substitutions.
		 *
		 * NB: the resulting childrel->reltarget->exprs may contain arbitrary
		 * expressions, which otherwise would not occur in a rel's targetlist.
		 * Code that might be looking at an appendrel child must cope with
		 * such.  (Normally, a rel's targetlist would only include Vars and
		 * PlaceHolderVars.)  XXX we do not bother to update the cost or width
		 * fields of childrel->reltarget; not clear if that would be useful.
		 */
		childrel->joininfo = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->joininfo,
								   1, &appinfo);
		childrel->reltarget->exprs = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->reltarget->exprs,
								   1, &appinfo);

		/*
		 * We have to make child entries in the EquivalenceClass data
		 * structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings).
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
		childrel->has_eclass_joins = rel->has_eclass_joins;

		/*
		 * Note: we could compute appropriate attr_needed data for the child's
		 * variables, by transforming the parent's attr_needed through the
		 * translated_vars mapping.  However, currently there's no need
		 * because attr_needed is only examined for base relations not
		 * otherrels.  So we just leave the child's attr_needed empty.
		 */

		/*
		 * If we consider partitionwise joins with the parent rel, do the same
		 * for partitioned child rels.
		 *
		 * Note: here we abuse the consider_partitionwise_join flag by setting
		 * it for child rels that are not themselves partitioned.  We do so to
		 * tell try_partitionwise_join() that the child rel is sufficiently
		 * valid to be used as a per-partition input, even if it later gets
		 * proven to be dummy.  (It's not usable until we've set up the
		 * reltarget and EC entries, which we just did.)
		 */
		if (rel->consider_partitionwise_join)
			childrel->consider_partitionwise_join = true;

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this childrel in particular.  But if we've
		 * already decided the appendrel is not parallel-safe as a whole,
		 * there's no point in considering parallelism for this child.  For
		 * consistency, do this before calling set_rel_size() for the child.
		 */
		if (root->glob->parallelModeOK && rel->consider_parallel)
			set_rel_consider_parallel(root, childrel, childRTE);

		/*
		 * Compute the child's size.
		 */
		set_rel_size(root, childrel, childRTindex, childRTE);

		/*
		 * It is possible that constraint exclusion detected a contradiction
		 * within a child subquery, even though we didn't prove one above. If
		 * so, we can skip this child.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* We have at least one live child. */
		has_live_children = true;

		/*
		 * If any live child is not parallel-safe, treat the whole appendrel
		 * as not parallel-safe.  In future we might be able to generate plans
		 * in which some children are farmed out to workers while others are
		 * not; but we don't have that today, so it's a waste to consider
		 * partial paths anywhere in the appendrel unless it's all safe.
		 * (Child rels visited before this one will be unmarked in
		 * set_append_rel_pathlist().)
		 */
		if (!childrel->consider_parallel)
			rel->consider_parallel = false;

		/*
		 * Accumulate size information from each live child.
		 */
		Assert(childrel->rows > 0);

		parent_rows += childrel->rows;
		parent_size += childrel->reltarget->width * childrel->rows;

		/*
		 * Accumulate per-column estimates too.  We need not do anything for
		 * PlaceHolderVars in the parent list.  If child expression isn't a
		 * Var, or we didn't record a width estimate for it, we have to fall
		 * back on a datatype-based estimate.
		 *
		 * By construction, child's targetlist is 1-to-1 with parent's.
		 */
		forboth(parentvars, rel->reltarget->exprs,
				childvars, childrel->reltarget->exprs)
		{
			Var		   *parentvar = (Var *) lfirst(parentvars);
			Node	   *childvar = (Node *) lfirst(childvars);

			if (IsA(parentvar, Var))
			{
				int			pndx = parentvar->varattno - rel->min_attr;
				int32		child_width = 0;

				if (IsA(childvar, Var) &&
					((Var *) childvar)->varno == childrel->relid)
				{
					int			cndx = ((Var *) childvar)->varattno - childrel->min_attr;

					child_width = childrel->attr_widths[cndx];
				}
				if (child_width <= 0)
					child_width = get_typavgwidth(exprType(childvar),
												  exprTypmod(childvar));
				Assert(child_width > 0);
				parent_attrsizes[pndx] += child_width * childrel->rows;
			}
		}
	}

	if (has_live_children)
	{
		/*
		 * Save the finished size estimates.
		 */
		int			i;

		Assert(parent_rows > 0);
		rel->rows = parent_rows;
		rel->reltarget->width = rint(parent_size / parent_rows);
		for (i = 0; i < nattrs; i++)
			rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);

		/*
		 * Set "raw tuples" count equal to "rows" for the appendrel; needed
		 * because some places assume rel->tuples is valid for any baserel.
		 */
		rel->tuples = parent_rows;

		/*
		 * Note that we leave rel->pages as zero; this is important to avoid
		 * double-counting the appendrel tree in total_table_pages.
		 */
	}
	else
	{
		/*
		 * All children were excluded by constraints, so mark the whole
		 * appendrel dummy.  We must do this in this phase so that the rel's
		 * dummy-ness is visible when we generate paths for other rels.
		 */
		set_dummy_rel_pathlist(rel);
	}

	pfree(parent_attrsizes);
}

/* copied from allpaths.c */
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Mark rel with estimated output rows, width, etc */
	set_foreign_size_estimates(root, rel);

	/* Let FDW adjust the size estimates, if it can */
	rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

	/* ... but do not let it set the rows estimate to zero */
	rel->rows = clamp_row_est(rel->rows);
}

/* copied from allpaths.c */
static void
set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	TableSampleClause *tsc = rte->tablesample;
	TsmRoutine *tsm;
	BlockNumber pages;
	double		tuples;

	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/*
	 * Call the sampling method's estimation function to estimate the number
	 * of pages it will read and the number of tuples it will return.  (Note:
	 * we assume the function returns sane values.)
	 */
	tsm = GetTsmRoutine(tsc->tsmhandler);
	tsm->SampleScanGetSampleSize(root, rel, tsc->args,
								 &pages, &tuples);

	/*
	 * For the moment, because we will only consider a SampleScan path for the
	 * rel, it's okay to just overwrite the pages and tuples estimates for the
	 * whole relation.  If we ever consider multiple path types for sampled
	 * rels, we'll need more complication.
	 */
	rel->pages = pages;
	rel->tuples = tuples;

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/* copied from allpaths.c */
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/* extracted from the same function in allpaths.c
 * assumes that the root table is either excluded by constraints, or is an
 * inheritance base table, and that chunks are regular tables
 */
static void
set_rel_size(PlannerInfo *root, RelOptInfo *rel,
			 Index rti, RangeTblEntry *rte)
{
	if (rel->reloptkind == RELOPT_BASEREL &&
		relation_excluded_by_constraints(root, rel, rte))
	{
		/*
		 * We proved we don't need to scan the rel via constraint exclusion,
		 * so set up a single dummy path for it.  Here we only check this for
		 * regular baserels; if it's an otherrel, CE was already checked in
		 * set_append_rel_size().
		 *
		 * In this case, we go ahead and set up the relation's path right away
		 * instead of leaving it for set_rel_pathlist to do.  This is because
		 * we don't have a convention for marking a rel as dummy except by
		 * assigning a dummy path to it.
		 */
		set_dummy_rel_pathlist(rel);
	}
	else if (rte->inh)
	{
		/* It's an "append relation", process accordingly */
		set_append_rel_size(root, rel, rti, rte);
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_size(root, rel, rte);
				}
				else if (rte->relkind == RELKIND_PARTITIONED_TABLE)
				{
					/*
					 * We could get here if asked to scan a partitioned table
					 * with ONLY.  In that case we shouldn't scan any of the
					 * partitions, so mark it as a dummy rel.
					 */
					set_dummy_rel_pathlist(rel);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_size(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_size(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
			case RTE_RESULT:
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * We insist that all non-dummy rels have a nonzero rowcount estimate.
	 */
	Assert(rel->rows > 0 || IS_DUMMY_REL(rel));
}

static void
reenable_inheritance(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Index i;
	bool set_pathlist_for_current_rel = false;
	double total_pages;
	bool reenabled_inheritance = false;

	// Assert(inheritance_disabled_counter > inheritance_reenabled_counter);
	inheritance_reenabled_counter = inheritance_disabled_counter;

	for(i = 1; i < root->simple_rel_array_size; i++)
	{
		RangeTblEntry *in_rte = root->simple_rte_array[i];
		if(is_rte_hypertable(in_rte) && !in_rte->inh)
		{
			RelOptInfo *in_rel = root->simple_rel_array[i];
			expand_hypertable_inheritance(root, in_rte->relid, in_rte->inh, in_rel);
			//TODO move this back into ts_plan_expand_hypertable_chunks
			in_rte->inh = true;
			reenabled_inheritance = true;
			//FIXME redo set_rel_consider_parallel
			if (in_rel != NULL && in_rel->reloptkind == RELOPT_BASEREL)
				set_rel_size(root, in_rel, i, in_rte);
			/* if we're activating inheritance during a hypertable's pathlist
			 * creation then we're past the point at which postgres will add
			 * paths for the children, and we have to do it ourselves. We delay
			 * the actual setting of the pathlists until after this loop,
			 * because set_append_rel_pathlist will eventually call this hook again.
			 */
			if(in_rte == rte)
			{
				Assert(rti == i);
				set_pathlist_for_current_rel = true;
			}

		}
	}

	if(!reenabled_inheritance)
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

	if(set_pathlist_for_current_rel)
	{
		/* the hypertable will have been planned as if it was a regular table
		 * with no data. Since such a plan would be cheaper than any real plan,
		 * it would always be used, and we need to remove these plans before
		 * adding ours.
		 */
		rel->pathlist = NIL;
		rel->partial_pathlist = NIL;
		set_append_rel_pathlist(root, rel, rti, rte);
	}
}

#endif

static void
timescaledb_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Hypertable *ht;
	Cache *hcache;
	Oid ht_reloid = rte->relid;
	Oid is_htdml;

#if PG12
	// if(inheritance_disabled_counter > inheritance_reenabled_counter)
		reenable_inheritance(root, rel, rti, rte);
#endif

	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);

	if (!ts_extension_is_loaded() || IS_DUMMY_REL(rel) || !OidIsValid(rte->relid))
		return;

	/* do we have a DML transformation here */
	is_htdml = is_hypertable_chunk_dml(root, rel, rti, rte);

	/* quick abort if only optimizing hypertables */
	if (!ts_guc_optimize_non_hypertables &&
		!(is_append_parent(rel, rte) || is_append_child(rel, rte) || is_htdml))
		return;

	hcache = ts_hypertable_cache_pin();

	/*
	 * if this is an append child or DML we use the parent relid to
	 * check if its a hypertable
	 */
	if (is_htdml)
		ht_reloid = is_htdml;
	else if (is_append_child(rel, rte))
		ht_reloid = get_parentoid(root, rti);

	ht = ts_hypertable_cache_get_entry(hcache, ht_reloid);

	if (!is_htdml)
	{
		timescaledb_set_rel_pathlist_query(root, rel, rti, rte, ht);
	}
	else
	{
		if (ts_cm_functions->set_rel_pathlist_dml != NULL)
			ts_cm_functions->set_rel_pathlist_dml(root, rel, rti, rte, ht);
	}

	ts_cache_release(hcache);
}

/* This hook is meant to editorialize about the information
 * the planner gets about a relation. We hijack it here
 * to also expand the append relation for hypertables. */
static void
timescaledb_get_relation_info_hook(PlannerInfo *root, Oid relation_objectid, bool inhparent,
								   RelOptInfo *rel)
{
	RangeTblEntry *rte;
	if (prev_get_relation_info_hook != NULL)
		prev_get_relation_info_hook(root, relation_objectid, inhparent, rel);

	if (!ts_extension_is_loaded())
		return;

#if PG12
	/* in earlier versions this is done during expand_hypertable_inheritance() below */
	ts_plan_expand_timebucket_annotate(root, rel);
#endif

#if PG12_LT
	if (ts_guc_enable_constraint_exclusion)
		expand_hypertable_inheritance(root, relation_objectid, inhparent, rel);
#endif


	rte = rt_fetch(rel->relid, root->parse->rtable);

	if (ts_guc_enable_transparent_decompression && is_append_child(rel, rte))
	{
		Cache *hcache;
		Hypertable *ht;
		Oid ht_oid = get_parentoid(root, rel->relid);
		/* in PG12 UPDATE/DELETE the root table gets treated as a child of itself */
		if (rte->relid == ht_oid)
			return;
		hcache = ts_hypertable_cache_pin();
		ht = ts_hypertable_cache_get_entry(hcache, ht_oid);

		if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION(ht))
		{
			Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);

			if (chunk->fd.compressed_chunk_id > 0)
			{
				Assert(rel->fdw_private == NULL);
				rel->fdw_private = palloc0(sizeof(TimescaleDBPrivate));
				((TimescaleDBPrivate *) rel->fdw_private)->compressed = true;

				/* Planning indexes are expensive, and if this is a compressed chunk, we
				 * know we'll never need to us indexes on the uncompressed version, since all
				 * the data is in the compressed chunk anyway. Therefore, it is much faster if
				 * we simply trash the indexlist here and never plan any useless IndexPaths
				 *  at all
				 */
				rel->indexlist = NIL;
			}
		}
		ts_cache_release(hcache);
	}
}

static void
expand_hypertable_inheritance(PlannerInfo *root, Oid relation_objectid, bool inhparent,
								   RelOptInfo *rel)
{

	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);

	/*
	 * We expand the hypertable chunks into an append relation. Previously, in
	 * `turn_off_inheritance_walker` we suppressed this expansion. This hook
	 * is really the first one that's called after the initial planner setup
	 * and so it's convenient to do the expansion here. Note that this is
	 * after the usual expansion happens in `expand_inherited_tables` (called
	 * in `subquery_planner`). Note also that `get_relation_info` (the
	 * function that calls this hook at the end) is the expensive function to
	 * run on many chunks so the expansion really cannot be called before this
	 * hook.
	 */
	if (!rte->inh && is_rte_hypertable(rte))
	{
		Cache *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

		Assert(ht != NULL);

		Assert(rel->fdw_private == NULL);
		rel->fdw_private = palloc0(sizeof(TimescaleDBPrivate));

		ts_plan_expand_hypertable_chunks(ht, root, relation_objectid, inhparent, rel);
#if PG11_GE
		setup_append_rel_array(root);
#endif

		ts_cache_release(hcache);
	}
}

static bool
involves_ts_hypertable_relid(PlannerInfo *root, Index relid)
{
	if (relid == 0)
		return false;

	return is_rte_hypertable(planner_rt_fetch(relid, root));
}

static bool
involves_hypertable_relid_set(PlannerInfo *root, Relids relid_set)
{
	int relid = -1;

	while ((relid = bms_next_member(relid_set, relid)) >= 0)
	{
		if (involves_ts_hypertable_relid(root, relid))
			return true;
	}
	return false;
}

static bool
involves_hypertable(PlannerInfo *root, RelOptInfo *rel)
{
	RangeTblEntry *rte;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
			/* Optimization for a quick exit */
			rte = planner_rt_fetch(rel->relid, root);
			if (!(is_append_parent(rel, rte) || is_append_child(rel, rte)))
				return false;

			return involves_ts_hypertable_relid(root, rel->relid);
		case RELOPT_JOINREL:
			return involves_hypertable_relid_set(root, rel->relids);
		default:
			return false;
	}
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
	Cache *htcache = ts_hypertable_cache_pin();
	List *new_pathlist = NIL;
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, ModifyTablePath) && ((ModifyTablePath *) path)->operation == CMD_INSERT)
		{
			ModifyTablePath *mt = (ModifyTablePath *) path;
			RangeTblEntry *rte = planner_rt_fetch(linitial_int(mt->resultRelations), root);
			Hypertable *ht = ts_hypertable_cache_get_entry(htcache, rte->relid);

			if (NULL != ht)
				path = ts_hypertable_insert_path_create(root, mt);
		}

		new_pathlist = lappend(new_pathlist, path);
	}

	ts_cache_release(htcache);

	return new_pathlist;
}

static void
#if PG11_LT
timescale_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel)
{
	Query *parse = root->parse;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel);
#else
timescale_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;

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

		/* modify aggregates that need to be partialized */
		ts_plan_process_partialize_agg(root, input_rel, output_rel);
	}

	if (ts_guc_disable_optimizations || input_rel == NULL || IS_DUMMY_REL(input_rel))
		return;

	if (!ts_guc_optimize_non_hypertables && !involves_hypertable(root, input_rel))
		return;

	if (UPPERREL_GROUP_AGG == stage && output_rel != NULL)
	{
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
