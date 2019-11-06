/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

/*
 *	  Optimization for FIRST/LAST aggregate functions.
 *
 * This module tries to replace FIRST/LAST aggregate functions by subqueries
 * of the form
 *		(SELECT value FROM tab
 *		 WHERE sort IS NOT NULL AND existing-quals
 *		 ORDER BY sort ASC/DESC
 *		 LIMIT 1)
 * Given a suitable index on sort column, this can be much faster than the
 * generic scan-all-the-rows aggregation plan.  We can handle multiple
 * FIRST/LAST aggregates by generating multiple subqueries, and their
 * orderings can be different.  However, if the query also contains some
 * other aggregates (eg. MIN/MAX), we will skip optimization since we can't
 * optimize across different aggregate functions.
 *
 *	  Most of the code is borrowed from:
 *	  src/backend/optimizer/plan/planagg.c
 *
 *
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include <catalog/namespace.h>
#include "utils/typcache.h"
#include "access/stratnum.h"
#include "plan_agg_bookend.h"
#include "utils.h"
#include "extension.h"

#include "compat.h"

#if PG12
#include "optimizer/optimizer.h"
#endif

typedef struct FirstLastAggInfo
{
	MinMaxAggInfo *m_agg_info; /* reusing MinMaxAggInfo to avoid code
								* duplication */
	Expr *sort;				   /* Expression to use for ORDER BY */
} FirstLastAggInfo;

typedef struct MutatorContext
{
	MinMaxAggPath *mm_path;
} MutatorContext;

static bool find_first_last_aggs_walker(Node *node, List **context);
static bool build_first_last_path(PlannerInfo *root, FirstLastAggInfo *flinfo, Oid eqop, Oid sortop,
								  bool nulls_first);
static void first_last_qp_callback(PlannerInfo *root, void *extra);
static Node *mutate_aggref_node(Node *node, MutatorContext *context);
static void replace_aggref_in_tlist(MinMaxAggPath *minmaxagg_path);

/*
 * mutate_aggref_node
 *
 * Mutator function used by recursive `expression_tree_mutator`
 * to replace Aggref node with Param node
 */
Node *
mutate_aggref_node(Node *node, MutatorContext *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		/* See if the Aggref should be replaced by a Param */
		if (context->mm_path != NULL && list_length(aggref->args) == 2)
		{
			TargetEntry *curTarget = (TargetEntry *) linitial(aggref->args);
			ListCell *cell;

			foreach (cell, context->mm_path->mmaggregates)
			{
				MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(cell);

				if (mminfo->aggfnoid == aggref->aggfnoid && equal(mminfo->target, curTarget->expr))
					return (Node *) copyObject(mminfo->param);
			}
		}
	}
	return expression_tree_mutator(node, mutate_aggref_node, (void *) context);
}

/*
 * replace_aggref_in_tlist
 *
 * If MinMaxAggPath is chosen, instead of running aggregate
 * function we will execute subquery that we've generated. Since we
 * use subquery we need to replace target list Aggref node with Param
 * node. Param node passes output value from the subquery.
 *
 */
void
replace_aggref_in_tlist(MinMaxAggPath *minmaxagg_path)
{
	MutatorContext context;

	context.mm_path = minmaxagg_path;

	((Path *) minmaxagg_path)->pathtarget->exprs =
		(List *) mutate_aggref_node((Node *) ((Path *) minmaxagg_path)->pathtarget->exprs,
									(void *) &context);
}

/* Stores function id (FIRST/LAST) with proper comparison strategy */
typedef struct FuncStrategy
{
	Oid func_oid;
	StrategyNumber strategy;
} FuncStrategy;

static Oid first_last_arg_types[] = { ANYELEMENTOID, ANYOID };

static struct FuncStrategy first_func_strategy = { .func_oid = InvalidOid,
												   .strategy = BTLessStrategyNumber };
static struct FuncStrategy last_func_strategy = { .func_oid = InvalidOid,
												  .strategy = BTGreaterStrategyNumber };

static FuncStrategy *
initialize_func_strategy(FuncStrategy *func_strategy, char *name, int nargs, Oid arg_types[])
{
	List *l = list_make2(makeString(ts_extension_schema_name()), makeString(name));
	func_strategy->func_oid = LookupFuncName(l, nargs, arg_types, false);
	return func_strategy;
}

static FuncStrategy *
get_func_strategy(Oid func_oid)
{
	if (first_func_strategy.func_oid == InvalidOid)
		initialize_func_strategy(&first_func_strategy, "first", 2, first_last_arg_types);
	if (last_func_strategy.func_oid == InvalidOid)
		initialize_func_strategy(&last_func_strategy, "last", 2, first_last_arg_types);
	if (first_func_strategy.func_oid == func_oid)
		return &first_func_strategy;
	if (last_func_strategy.func_oid == func_oid)
		return &last_func_strategy;
	return NULL;
}

static bool
is_first_last_node(Node *node, List **context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		FuncStrategy *func_strategy = get_func_strategy(aggref->aggfnoid);

		if (func_strategy != NULL)
			return true;
	}
	return expression_tree_walker(node, is_first_last_node, context);
}

static bool
contains_first_last_node(List *sortClause, List *targetList)
{
	List *exprs = get_sortgrouplist_exprs(sortClause, targetList);
	ListCell *cell;
	List *context = NIL;

	foreach (cell, exprs)
	{
		Node *expr = lfirst(cell);
		bool found = is_first_last_node(expr, &context);

		if (found)
			return true;
	}
	return false;
}

/*
 * preprocess_first_last_aggregates - preprocess FIRST/LAST aggregates
 *
 * Check to see whether the query contains FIRST/LAST aggregate functions that
 * might be optimizable via index scans.  If it does, and all the aggregates
 * are potentially optimizable, then create a MinMaxAggPath(reusing MinMax path implementation)\
 * and add it to the (UPPERREL_GROUP_AGG, NULL) upperrel.
 *
 * This method is called from create_upper_paths_hook in the UPPERREL_GROUP_AGG stage.
 *
 * Note: we are passed the preprocessed targetlist separately, because it's
 * not necessarily equal to root->parse->targetList.
 *
 * Most of the code is borrowed from: preprocess_minmax_aggregates (planagg.c). Few
 * major differences:
 *  - generate FirstLastAggInfo that wraps MinMaxAggInfo
 *  - generate subquery (path) for FIRST/LAST (we reuse MinMaxAggPath)
 *  - replace Aggref node with Param node
 * 	- reject ORDER BY on FIRST/LAST
 */
void
ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist)
{
	Query *parse = root->parse;
	FromExpr *jtnode;
	RangeTblRef *rtr;
	RangeTblEntry *rte;
	List *first_last_aggs;
	RelOptInfo *grouped_rel;
	ListCell *lc;
	List *mm_agg_list;
	MinMaxAggPath *minmaxagg_path;

	/* minmax_aggs list should be empty at this point */
	Assert(root->minmax_aggs == NIL);

	/* Nothing to do if query has no aggregates */
	if (!parse->hasAggs)
		return;

	Assert(!parse->setOperations);  /* shouldn't get here if a setop */
	Assert(parse->rowMarks == NIL); /* nor if FOR UPDATE */

	/*
	 * Reject unoptimizable cases.
	 *
	 * We don't handle the case when agg function is in ORDER BY. The reason
	 * being is that we replace Aggref node before sort keys are being
	 * generated.
	 *
	 * We don't handle GROUP BY or windowing, because our current
	 * implementations of grouping require looking at all the rows anyway, and
	 * so there's not much point in optimizing FIRST/LAST.
	 */
	if (parse->groupClause || list_length(parse->groupingSets) > 1 || parse->hasWindowFuncs ||
		contains_first_last_node(parse->sortClause, tlist))
		return;

	/*
	 * Reject if query contains any CTEs; there's no way to build an indexscan
	 * on one so we couldn't succeed here.  (If the CTEs are unreferenced,
	 * that's not true, but it doesn't seem worth expending cycles to check.)
	 */
	if (parse->cteList)
		return;

	/*
	 * We also restrict the query to reference exactly one table, since join
	 * conditions can't be handled reasonably.  (We could perhaps handle a
	 * query containing cartesian-product joins, but it hardly seems worth the
	 * trouble.)  However, the single table could be buried in several levels
	 * of FromExpr due to subqueries.  Note the "single" table could be an
	 * inheritance parent, too, including the case of a UNION ALL subquery
	 * that's been flattened to an appendrel.
	 */
	jtnode = parse->jointree;
	while (IsA(jtnode, FromExpr))
	{
		if (list_length(jtnode->fromlist) != 1)
			return;
		jtnode = linitial(jtnode->fromlist);
	}
	if (!IsA(jtnode, RangeTblRef))
		return;
	rtr = (RangeTblRef *) jtnode;
	rte = planner_rt_fetch(rtr->rtindex, root);
	if (rte->rtekind == RTE_RELATION)
		/* ordinary relation, ok */;
	else if (rte->rtekind == RTE_SUBQUERY && rte->inh)
		/* flattened UNION ALL subquery, ok */;
	else
		return;

	/*
	 * Scan the tlist and HAVING qual to find all the aggregates and verify
	 * all are FIRST/LAST aggregates.  Stop as soon as we find one that isn't.
	 */
	first_last_aggs = NIL;
	if (find_first_last_aggs_walker((Node *) tlist, &first_last_aggs))
		return;
	if (find_first_last_aggs_walker(parse->havingQual, &first_last_aggs))
		return;

	/*
	 * OK, there is at least the possibility of performing the optimization.
	 * Build an access path for each aggregate.  If any of the aggregates
	 * prove to be non-indexable, give up; there is no point in optimizing
	 * just some of them.
	 */
	foreach (lc, first_last_aggs)
	{
		FirstLastAggInfo *fl_info = (FirstLastAggInfo *) lfirst(lc);
		MinMaxAggInfo *mminfo = fl_info->m_agg_info;
		Oid eqop;
		bool reverse;

		/*
		 * We'll need the equality operator that goes with the aggregate's
		 * ordering operator.
		 */
		eqop = get_equality_op_for_ordering_op(mminfo->aggsortop, &reverse);
		if (!OidIsValid(eqop)) /* shouldn't happen */
			elog(ERROR,
				 "could not find equality operator for ordering operator %u",
				 mminfo->aggsortop);

		/*
		 * We can use either an ordering that gives NULLS FIRST or one that
		 * gives NULLS LAST; furthermore there's unlikely to be much
		 * performance difference between them, so it doesn't seem worth
		 * costing out both ways if we get a hit on the first one.  NULLS
		 * FIRST is more likely to be available if the operator is a
		 * reverse-sort operator, so try that first if reverse.
		 */
		if (build_first_last_path(root, fl_info, eqop, mminfo->aggsortop, reverse))
			continue;
		if (build_first_last_path(root, fl_info, eqop, mminfo->aggsortop, !reverse))
			continue;

		/* No indexable path for this aggregate, so fail */
		return;
	}

	/*
	 * OK, we can do the query this way. We are using MinMaxAggPath to store
	 * First/Last Agg path since the logic is almost the same. MinMaxAggPath
	 * is used later on by planner so by reusing it we don't need to re-invent
	 * planner.
	 *
	 * Prepare to create a MinMaxAggPath node.
	 *
	 * First, create an output Param node for each agg.  (If we end up not
	 * using the MinMaxAggPath, we'll waste a PARAM_EXEC slot for each agg,
	 * which is not worth worrying about.  We can't wait till create_plan time
	 * to decide whether to make the Param, unfortunately.)
	 */
	mm_agg_list = NIL;
	foreach (lc, first_last_aggs)
	{
		FirstLastAggInfo *fl_info = (FirstLastAggInfo *) lfirst(lc);
		MinMaxAggInfo *mminfo = fl_info->m_agg_info;

		mminfo->param = SS_make_initplan_output_param(root,
													  exprType((Node *) mminfo->target),
													  -1,
													  exprCollation((Node *) mminfo->target));
		mm_agg_list = lcons(mminfo, mm_agg_list);
	}

	/*
	 * Create a MinMaxAggPath node with the appropriate estimated costs and
	 * other needed data, and add it to the UPPERREL_GROUP_AGG upperrel, where
	 * it will compete against the standard aggregate implementation.  (It
	 * will likely always win, but we need not assume that here.)
	 *
	 * Note: grouping_planner won't have created this upperrel yet, but it's
	 * fine for us to create it first.  We will not have inserted the correct
	 * consider_parallel value in it, but MinMaxAggPath paths are currently
	 * never parallel-safe anyway, so that doesn't matter.  Likewise, it
	 * doesn't matter that we haven't filled FDW-related fields in the rel.
	 */
	grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, NULL);
	minmaxagg_path = create_minmaxagg_path(root,
										   grouped_rel,
										   create_pathtarget(root, tlist),
										   mm_agg_list,
										   (List *) parse->havingQual);
	/* Let's replace Aggref node since we will use subquery we've generated  */
	replace_aggref_in_tlist(minmaxagg_path);
	add_path(grouped_rel, (Path *) minmaxagg_path);
}

/*
 * find_first_last_aggs_walker
 *		Recursively scan the Aggref nodes in an expression tree, and check
 *		that each one is a FIRST/LAST aggregate.  If so, build a list of the
 *		distinct aggregate calls in the tree.
 *
 * Returns TRUE if a non-FIRST/LAST aggregate is found, FALSE otherwise.
 * (This seemingly-backward definition is used because expression_tree_walker
 * aborts the scan on TRUE return, which is what we want.)
 *
 * Found aggregates are added to the list at *context; it's up to the caller
 * to initialize the list to NIL.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans.  There mustn't be outer-aggregate
 * references either.
 *
 * Major differences from find_minmax_aggs_walker (planagg.c):
 * - only allow Aggref with two arguments
 * - wrap agg info in FirstLastAggInfo
 */
static bool
find_first_last_aggs_walker(Node *node, List **context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;
		Oid aggsortop;
		TargetEntry *value;
		TargetEntry *sort;
		MinMaxAggInfo *mminfo;
		ListCell *l;
		FirstLastAggInfo *fl_info;
		Oid sort_oid;
		TypeCacheEntry *sort_tce;
		FuncStrategy *func_strategy;

		Assert(aggref->agglevelsup == 0);
		if (list_length(aggref->args) != 2)
			return true; /* it couldn't be first/last */

		/*
		 * ORDER BY is usually irrelevant for FIRST/LAST, but it can change
		 * the outcome if the aggsortop's operator class recognizes
		 * non-identical values as equal.  For example, 4.0 and 4.00 are equal
		 * according to numeric_ops, yet distinguishable.  If FIRST() receives
		 * more than one value equal to 4.0 and no value less than 4.0, it is
		 * unspecified which of those equal values FIRST() returns.  An ORDER
		 * BY expression that differs for each of those equal values of the
		 * argument expression makes the result predictable once again.  This
		 * is a niche requirement, and we do not implement it with subquery
		 * paths. In any case, this test lets us reject ordered-set aggregates
		 * quickly.
		 */
		if (aggref->aggorder != NIL)
			return true;
		/* note: we do not care if DISTINCT is mentioned ... */

		/*
		 * We might implement the optimization when a FILTER clause is present
		 * by adding the filter to the quals of the generated subquery.  For
		 * now, just punt.
		 */
		if (aggref->aggfilter != NULL)
			return true;

		/* We sort by second argument (eg. time) */
		sort_oid = lsecond_oid(aggref->aggargtypes);

		func_strategy = get_func_strategy(aggref->aggfnoid);
		if (func_strategy == NULL)
			return true; /* not first/last aggregate */

		sort_tce = lookup_type_cache(sort_oid, TYPECACHE_BTREE_OPFAMILY);
		aggsortop =
			get_opfamily_member(sort_tce->btree_opf, sort_oid, sort_oid, func_strategy->strategy);
		if (aggsortop == InvalidOid)
			elog(ERROR,
				 "Can't resolve sort operator oid for function oid: %d and type: %d",
				 aggref->aggfnoid,
				 sort_oid);

		/* Used in projection */
		value = (TargetEntry *) linitial(aggref->args);
		/* Used in ORDER BY */
		sort = (TargetEntry *) lsecond(aggref->args);

		if (contain_mutable_functions((Node *) sort->expr))
			return true; /* not potentially indexable */

		if (type_is_rowtype(exprType((Node *) sort->expr)))
			return true; /* IS NOT NULL would have weird semantics */

		/*
		 * Check whether it's already in the list, and add it if not.
		 */
		foreach (l, *context)
		{
			mminfo = (MinMaxAggInfo *) lfirst(l);
			if (mminfo->aggfnoid == aggref->aggfnoid && equal(mminfo->target, value->expr))
				return false;
		}

		mminfo = makeNode(MinMaxAggInfo);
		mminfo->aggfnoid = aggref->aggfnoid;
		mminfo->aggsortop = aggsortop;
		mminfo->target = value->expr;
		mminfo->subroot = NULL;
		mminfo->path = NULL;
		mminfo->pathcost = 0;
		mminfo->param = NULL;

		fl_info = palloc(sizeof(FirstLastAggInfo));

		fl_info->m_agg_info = mminfo;
		fl_info->sort = sort->expr;
		*context = lappend(*context, fl_info);

		/*
		 * We need not recurse into the argument, since it can't contain any
		 * aggregates.
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, find_first_last_aggs_walker, (void *) context);
}

/*
 * build_first_last_path
 *		Given a FIRST/LAST aggregate, try to build an indexscan Path it can be
 *		optimized with.
 *		We will generate subquery with value and sort target, where we
 * 	    SELECT value and we ORDER BY sort.
 *
 * If successful, stash the best path in *mminfo and return TRUE.
 * Otherwise, return FALSE.
 *
 * Major differences when compared to build_minmax_path(planagg.c):
 * - generates different subquery
 * - works with two target entries (value and sortby)
 * - resets EquivalenceClass(es)
 *
 */
static bool
build_first_last_path(PlannerInfo *root, FirstLastAggInfo *fl_info, Oid eqop, Oid sortop,
					  bool nulls_first)
{
	PlannerInfo *subroot;
	Query *parse;
	TargetEntry *value_target;
	TargetEntry *sort_target;
	List *tlist;
	NullTest *ntest;
	SortGroupClause *sortcl;
	RelOptInfo *final_rel;
	Path *sorted_path;
	Cost path_cost;
	double path_fraction;
	MinMaxAggInfo *mminfo;

	/*
	 * We are going to construct what is effectively a sub-SELECT query, so
	 * clone the current query level's state and adjust it to make it look
	 * like a subquery.  Any outer references will now be one level higher
	 * than before.  (This means that when we are done, there will be no Vars
	 * of level 1, which is why the subquery can become an initplan.)
	 */
	subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
	memcpy(subroot, root, sizeof(PlannerInfo));
	subroot->query_level++;
	subroot->parent_root = root;
	/* reset subplan-related stuff */
	subroot->plan_params = NIL;
	subroot->outer_params = NULL;
	subroot->init_plans = NIL;
	/* reset EquivalenceClass since we will create it later on */
	subroot->eq_classes = NIL;

	subroot->parse = parse = copyObject(root->parse);
	IncrementVarSublevelsUp((Node *) parse, 1, 1);

	/* append_rel_list might contain outer Vars? */
	subroot->append_rel_list = copyObject(root->append_rel_list);
	IncrementVarSublevelsUp((Node *) subroot->append_rel_list, 1, 1);
	/* There shouldn't be any OJ info to translate, as yet */
	Assert(subroot->join_info_list == NIL);
	/* and we haven't made equivalence classes, either */
	Assert(subroot->eq_classes == NIL);
	/* and we haven't created PlaceHolderInfos, either */
	Assert(subroot->placeholder_list == NIL);

	mminfo = fl_info->m_agg_info;

	/*----------
	 * Generate modified query of the form
	 *		(SELECT value FROM tab
	 *		 WHERE sort IS NOT NULL AND existing-quals
	 *		 ORDER BY sort ASC/DESC
	 *		 LIMIT 1)
	 *----------
	 */

	/*
	 * Value and sort target entries but sort target is eliminated later on
	 * from target list
	 */
	value_target =
		makeTargetEntry(copyObject(mminfo->target), (AttrNumber) 1, pstrdup("value"), false);
	sort_target = makeTargetEntry(copyObject(fl_info->sort), (AttrNumber) 2, pstrdup("sort"), true);
	tlist = list_make2(value_target, sort_target);
	subroot->processed_tlist = parse->targetList = tlist;

	/* No HAVING, no DISTINCT, no aggregates anymore */
	parse->havingQual = NULL;
	subroot->hasHavingQual = false;
	parse->distinctClause = NIL;
	parse->hasDistinctOn = false;
	parse->hasAggs = false;

	/* Build "sort IS NOT NULL" expression. Not that target can still be NULL */
	ntest = makeNode(NullTest);
	ntest->nulltesttype = IS_NOT_NULL;
	ntest->arg = copyObject(fl_info->sort);
	/* we checked it wasn't a rowtype in find_minmax_aggs_walker */
	ntest->argisrow = false;
	ntest->location = -1;

	/* User might have had that in WHERE already */
	if (!list_member((List *) parse->jointree->quals, ntest))
		parse->jointree->quals = (Node *) lcons(ntest, (List *) parse->jointree->quals);

	/* Build suitable ORDER BY clause */
	sortcl = makeNode(SortGroupClause);
	sortcl->tleSortGroupRef = assignSortGroupRef(sort_target, tlist);
	sortcl->eqop = eqop;
	sortcl->sortop = sortop;
	sortcl->nulls_first = nulls_first;
	sortcl->hashable = false; /* no need to make this accurate */
	parse->sortClause = list_make1(sortcl);

	/* set up expressions for LIMIT 1 */
	parse->limitOffset = NULL;
	parse->limitCount = (Node *)
		makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(1), false, FLOAT8PASSBYVAL);

	/*
	 * Generate the best paths for this query, telling query_planner that we
	 * have LIMIT 1.
	 */
	subroot->tuple_fraction = 1.0;
	subroot->limit_tuples = 1.0;

#if PG12
	{
		ListCell *lc;
		/* min/max optimizations ususally happen before
		 * inheritance-relations are expanded, and thus query_planner will
		 * try to expand our hypertables if they are marked as
		 * inheritance-relations. Since we do not want this, we must mark
		 * hypertabls as non-inheritance now.
		 */
		foreach (lc, subroot->parse->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
			if (is_rte_hypertable(rte))
			{
				ListCell *prev = NULL;
				ListCell *next = list_head(subroot->append_rel_list);
				Assert(rte->inh);
				rte->inh = false;
				/* query planner gets confused when entries in the
				 * append_rel_list refer to entreis in the relarray that
				 * don't exist. Since we need to rexpand hypertables in the
				 * subquery, all of the chunk entries will be invalid in
				 * this manner, so we remove them from the list
				 */
				/* TODO this can be made non-quadratic by storing all the
				 *      relids in a bitset, then iterating over the
				 *      append_rel_list once
				 */
				while (next != NULL)
				{
					AppendRelInfo *app = lfirst(next);
					if (app->parent_reloid == rte->relid)
					{
						subroot->append_rel_list =
							list_delete_cell(subroot->append_rel_list, next, prev);
						next = prev != NULL ? prev->next : list_head(subroot->append_rel_list);
					}
					else
					{
						prev = next;
						next = next->next;
					}
				}
			}
		}
	}
#endif

	final_rel = query_planner(subroot,
#if PG12_LT
							  /* as of 333ed24 uses subroot->processed_tlist instead  */
							  tlist,
#endif
							  first_last_qp_callback,
							  NULL);

#if PG12
	{
		ListCell *lc;
		/* we need to disable inheritance so the chunks are re-expanded correctly in the subroot */
		foreach (lc, root->parse->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
			if (is_rte_hypertable(rte))
			{
				rte->inh = true;
			}
		}
	}
#endif
	/*
	 * Since we didn't go through subquery_planner() to handle the subquery,
	 * we have to do some of the same cleanup it would do, in particular cope
	 * with params and initplans used within this subquery.  (This won't
	 * matter if we end up not using the subplan.)
	 */
	SS_identify_outer_params(subroot);
	SS_charge_for_initplans(subroot, final_rel);

	/*
	 * Get the best presorted path, that being the one that's cheapest for
	 * fetching just one row.  If there's no such path, fail.
	 */
	if (final_rel->rows > 1.0)
		path_fraction = 1.0 / final_rel->rows;
	else
		path_fraction = 1.0;

	sorted_path = get_cheapest_fractional_path_for_pathkeys(final_rel->pathlist,
															subroot->query_pathkeys,
															NULL,
															path_fraction);
	if (!sorted_path)
		return false;

	/*
	 * The path might not return exactly what we want, so fix that.  (We
	 * assume that this won't change any conclusions about which was the
	 * cheapest path.)
	 */
	sorted_path = apply_projection_to_path(subroot,
										   final_rel,
										   sorted_path,
										   create_pathtarget(subroot, tlist));

	/*
	 * Determine cost to get just the first row of the presorted path.
	 *
	 * Note: cost calculation here should match
	 * compare_fractional_path_costs().
	 */
	path_cost = sorted_path->startup_cost +
				path_fraction * (sorted_path->total_cost - sorted_path->startup_cost);

	/* Save state for further processing */
	mminfo->subroot = subroot;
	mminfo->path = sorted_path;
	mminfo->pathcost = path_cost;

	return true;
}

/*
 * Compute query_pathkeys and other pathkeys during query_planner()
 */
static void
first_last_qp_callback(PlannerInfo *root, void *extra)
{
	root->group_pathkeys = NIL;
	root->window_pathkeys = NIL;
	root->distinct_pathkeys = NIL;

	root->sort_pathkeys =
		make_pathkeys_for_sortclauses(root, root->parse->sortClause, root->parse->targetList);

	root->query_pathkeys = root->sort_pathkeys;
}
