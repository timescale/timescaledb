/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <math.h>
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

typedef struct TimescaledbWalkerState
{
	CmdType cmdtype;
	Cache *hc;
} TimescaledbWalkerState;

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;
static bool contain_param(Node *node);
static void cagg_reorder_groupby_clause(RangeTblEntry *subq_rte, int rtno, List *outer_sortcl,
										List *outer_tlist);
static void expand_hypertable_inheritance(PlannerInfo *root, Oid relation_objectid, bool inhparent,
										  RelOptInfo *rel);

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
ts_is_rte_hypertable(RangeTblEntry *rte)
{
	return rte->ctename != NULL && strcmp(rte->ctename, CTE_NAME_HYPERTABLES) == 0;
}

/* This turns off inheritance on hypertables where we will do chunk
 * expansion ourselves. This prevents postgres from expanding the inheritance
 * tree itself. We will expand the chunks in expand_hypertable_inheritance. */
static bool
timescaledb_query_walker(Node *node, TimescaledbWalkerState *cxt)
{
	if (node == NULL)
		return false;
	Assert(cxt->cmdtype == CMD_INSERT || (cxt->cmdtype == CMD_SELECT));
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *lc;
		int rti = 1;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst(lc);
			if (rte->rtekind == RTE_SUBQUERY && cxt->cmdtype == CMD_SELECT &&
				ts_guc_enable_cagg_reorder_groupby)
			{
				/* applicable to selects on continuous aggregates */
				List *outer_tlist = query->targetList;
				List *outer_sortcl = query->sortClause;
				cagg_reorder_groupby_clause(rte, rti, outer_sortcl, outer_tlist);
			}
			else if (rte->inh && ts_guc_enable_constraint_exclusion)
			{
				/* turn off inheritance on hypertables for inserts and selects*/
				Cache *hc = cxt->hc;
				Hypertable *ht = ts_hypertable_cache_get_entry(hc, rte->relid, true);

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

		return query_tree_walker(query, timescaledb_query_walker, cxt, 0);
	}

	return expression_tree_walker(node, timescaledb_query_walker, cxt);
}

static PlannedStmt *
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
{
	PlannedStmt *stmt;
	ListCell *lc;
	if (ts_extension_is_loaded() && !ts_guc_disable_optimizations)
	{
		TimescaledbWalkerState cxt;
		Query *node_arg = NULL;
		if (ts_guc_enable_cagg_reorder_groupby && (parse->commandType == CMD_UTILITY))
		{
			Query *modq = NULL;
			if ((nodeTag(parse->utilityStmt) == T_ExplainStmt))
			{
				modq = (Query *) ((ExplainStmt *) parse->utilityStmt)->query;
			}

			if (modq && modq->commandType == CMD_SELECT)
			{
				cxt.cmdtype = CMD_SELECT;
				node_arg = modq;
			}
		}
		else if (parse->commandType == CMD_INSERT && ts_guc_enable_constraint_exclusion)
		{
			cxt.cmdtype = CMD_INSERT;
			node_arg = parse;
		}
		else if (parse->commandType == CMD_SELECT &&
				 (ts_guc_enable_constraint_exclusion || ts_guc_enable_cagg_reorder_groupby))
		{
			cxt.cmdtype = CMD_SELECT;
			node_arg = parse;
		}
		if (node_arg)
		{
			Cache *hc = ts_hypertable_cache_pin();
			cxt.hc = hc;
			/*
			 * turn of inheritance on hypertables we will expand ourselves in
			 * expand_hypertable_inheritance
			 */
			timescaledb_query_walker((Node *) node_arg, &cxt);

			ts_cache_release(hc);
		}
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
			if (ts_is_hypertable(parent_oid))
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
		 * (see
		 * https://github.com/postgres/postgres/blob/REL_12_1/src/backend/optimizer/plan/planner.c#L1281-L1291
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

#if PG12_GE

static void
reenable_inheritance(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Index i;
	bool set_pathlist_for_current_rel = false;
	double total_pages;
	bool reenabled_inheritance = false;

	// Assert(inheritance_disabled_counter > inheritance_reenabled_counter);
	inheritance_reenabled_counter = inheritance_disabled_counter;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RangeTblEntry *in_rte = root->simple_rte_array[i];
		if (ts_is_rte_hypertable(in_rte) && !in_rte->inh)
		{
			RelOptInfo *in_rel = root->simple_rel_array[i];
			expand_hypertable_inheritance(root, in_rte->relid, in_rte->inh, in_rel);
			// TODO move this back into ts_plan_expand_hypertable_chunks
			in_rte->inh = true;
			reenabled_inheritance = true;
			// FIXME redo set_rel_consider_parallel
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

#endif

static void
timescaledb_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Hypertable *ht;
	Cache *hcache;
	Oid ht_reloid = rte->relid;
	Oid is_htdml;

#if PG12_GE
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

	/*
	 * if this is an append child or DML we use the parent relid to
	 * check if its a hypertable
	 */
	if (is_htdml)
		ht_reloid = is_htdml;
	else if (is_append_child(rel, rte))
		ht_reloid = get_parentoid(root, rti);

	ht = ts_hypertable_cache_get_cache_and_entry(ht_reloid, true, &hcache);

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

#if PG12_GE
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

		ht = ts_hypertable_cache_get_cache_and_entry(ht_oid, true, &hcache);

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
	 * `timescaledb_query_walker` we suppressed this expansion. This hook
	 * is really the first one that's called after the initial planner setup
	 * and so it's convenient to do the expansion here. Note that this is
	 * after the usual expansion happens in `expand_inherited_tables` (called
	 * in `subquery_planner`). Note also that `get_relation_info` (the
	 * function that calls this hook at the end) is the expensive function to
	 * run on many chunks so the expansion really cannot be called before this
	 * hook.
	 */
	if (!rte->inh && ts_is_rte_hypertable(rte))
	{
		Cache *hcache;
		Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(rte->relid, false, &hcache);

		Assert(rel->fdw_private == NULL);
		rel->fdw_private = palloc0(sizeof(TimescaleDBPrivate));

		ts_plan_expand_hypertable_chunks(ht, root, relation_objectid, inhparent, rel);

		ts_cache_release(hcache);
	}
}

static bool
involves_ts_hypertable_relid(PlannerInfo *root, Index relid)
{
	if (relid == 0)
		return false;

	return ts_is_rte_hypertable(planner_rt_fetch(relid, root));
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
			Hypertable *ht = ts_hypertable_cache_get_entry(htcache, rte->relid, true);

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

	// should cache this information for cont. aggregates
	foreach (rtlc, viewq->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rtlc);
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
