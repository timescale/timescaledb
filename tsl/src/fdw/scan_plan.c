/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <optimizer/paths.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>
#include <miscadmin.h>
#include <fmgr.h>

#include <export.h>
#include <planner.h>

#include "estimate.h"
#include "relinfo.h"
#include "utils.h"
#include "deparse.h"
#include "scan_plan.h"
#include "debug.h"
#include "fdw_utils.h"
#include "scan_exec.h"
#include "chunk.h"

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List *useful_pathkeys_list = NIL;
	ListCell *lc;

	/*
	 * Pushing the query_pathkeys to the data node is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys)
	{
		bool query_pathkeys_ok = true;

		foreach (lc, root->query_pathkeys)
		{
			PathKey *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile || !(em_expr = find_em_expr_for_rel(pathkey_ec, rel)) ||
				!ts_is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
	}

	return useful_pathkeys_list;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
								CreatePathFunc create_scan_path,
								CreateUpperPathFunc create_upper_path)
{
	List *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell *lc;

	Assert((create_scan_path || create_upper_path) && !(create_scan_path && create_upper_path));

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach (lc, useful_pathkeys_list)
	{
		double rows;
		int width;
		Cost startup_cost;
		Cost total_cost;
		List *useful_pathkeys = lfirst(lc);
		Path *sorted_epq_path;
		Path *scan_path;

		fdw_estimate_path_cost_size(root,
									rel,
									useful_pathkeys,
									&rows,
									&width,
									&startup_cost,
									&total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys, sorted_epq_path->pathkeys))
			sorted_epq_path =
				(Path *) create_sort_path(root, rel, sorted_epq_path, useful_pathkeys, -1.0);

		if (create_scan_path)
		{
			Assert(IS_SIMPLE_REL(rel) || IS_JOIN_REL(rel));
			scan_path = create_scan_path(root,
										 rel,
										 NULL,
										 rows,
										 startup_cost,
										 total_cost,
										 useful_pathkeys,
										 NULL,
										 sorted_epq_path,
										 NIL);
		}
		else
		{
			Assert(IS_UPPER_REL(rel));
			scan_path = create_upper_path(root,
										  rel,
										  NULL,
										  rows,
										  startup_cost,
										  total_cost,
										  useful_pathkeys,
										  sorted_epq_path,
										  NIL);
		}

		fdw_utils_add_path(rel, scan_path);
	}
}

void
fdw_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
									CreatePathFunc create_scan_path)
{
	add_paths_with_pathkeys_for_rel(root, rel, epq_path, create_scan_path, NULL);
}

void
fdw_add_upper_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
										  CreateUpperPathFunc create_upper_path)
{
	add_paths_with_pathkeys_for_rel(root, rel, epq_path, NULL, create_upper_path);
}

typedef struct
{
	ParamListInfo boundParams;
	PlannerInfo *root;
	List *active_fns;
	Node *case_val;
	bool estimate;
} eval_stable_functions_context;

static Node *eval_stable_functions_mutator(Node *node, void *context);

static Expr *
evaluate_stable_function(Oid funcid, Oid result_type, int32 result_typmod, Oid result_collid,
						 Oid input_collid, List *args, bool funcvariadic, Form_pg_proc funcform)
{
	bool has_nonconst_input = false;
	PG_USED_FOR_ASSERTS_ONLY bool has_null_input = false;
	ListCell *arg;
	FuncExpr *newexpr;

#ifdef TS_DEBUG
	/* Allow tests to specify the time to push down in place of now() */
	if (funcid == F_NOW && ts_current_timestamp_override_value != -1)
	{
		return (Expr *) makeConst(TIMESTAMPTZOID,
								  -1,
								  InvalidOid,
								  sizeof(TimestampTz),
								  TimestampTzGetDatum(ts_current_timestamp_override_value),
								  false,
								  FLOAT8PASSBYVAL);
	}
#endif

	/*
	 * Can't simplify if it returns a set or a RECORD. See the comments for
	 * eval_const_expressions(). We should only see the whitelisted functions
	 * here, no sets or RECORDS among them.
	 */
	Assert(!funcform->proretset);
	Assert(funcform->prorettype != RECORDOID);

	/*
	 * Check for constant inputs and especially constant-NULL inputs.
	 */
	foreach (arg, args)
	{
		if (IsA(lfirst(arg), Const))
			has_null_input |= ((Const *) lfirst(arg))->constisnull;
		else
			has_nonconst_input = true;
	}

	/*
	 * The simplification of strict functions with constant NULL inputs must
	 * have been already performed by eval_const_expressions().
	 */
	Assert(!(funcform->proisstrict && has_null_input));

	/*
	 * Otherwise, can simplify only if all inputs are constants. (For a
	 * non-strict function, constant NULL inputs are treated the same as
	 * constant non-NULL inputs.)
	 */
	if (has_nonconst_input)
		return NULL;

	/*
	 * This is called on the access node for the expressions that will be pushed
	 * down to data nodes. These expressions can contain only whitelisted stable
	 * functions, so we shouldn't see volatile functions here. Immutable
	 * functions can also occur here for expressions like
	 * `immutable(stable(....))`, after we evaluate the stable function.
	 */
	Assert(funcform->provolatile != PROVOLATILE_VOLATILE);

	/*
	 * OK, looks like we can simplify this operator/function.
	 *
	 * Build a new FuncExpr node containing the already-simplified arguments.
	 */
	newexpr = makeNode(FuncExpr);
	newexpr->funcid = funcid;
	newexpr->funcresulttype = result_type;
	newexpr->funcretset = false;
	newexpr->funcvariadic = funcvariadic;
	newexpr->funcformat = COERCE_EXPLICIT_CALL; /* doesn't matter */
	newexpr->funccollid = result_collid;		/* doesn't matter */
	newexpr->inputcollid = input_collid;
	newexpr->args = args;
	newexpr->location = -1;

	return evaluate_expr((Expr *) newexpr, result_type, result_typmod, result_collid);
}

/*
 * Execute the function to deliver a constant result.
 */
static Expr *
simplify_stable_function(Oid funcid, Oid result_type, int32 result_typmod, Oid result_collid,
						 Oid input_collid, List **args_p, bool funcvariadic)
{
	List *args = *args_p;
	HeapTuple func_tuple;
	Form_pg_proc funcform;
	Expr *newexpr;

	func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

	/*
	 * Process the function arguments. Here we must deal with named or defaulted
	 * arguments, and then recursively apply eval_stable_functions to the whole
	 * argument list.
	 */
	args = expand_function_arguments_compat(args, result_type, func_tuple);
	args = (List *) expression_tree_mutator((Node *) args, eval_stable_functions_mutator, NULL);
	/* Argument processing done, give it back to the caller */
	*args_p = args;

	/* Now attempt simplification of the function call proper. */
	newexpr = evaluate_stable_function(funcid,
									   result_type,
									   result_typmod,
									   result_collid,
									   input_collid,
									   args,
									   funcvariadic,
									   funcform);

	ReleaseSysCache(func_tuple);

	return newexpr;
}

/*
 * Recursive guts of eval_stable_functions.
 * We don't use 'context' here but it is required by the signature of
 * expression_tree_mutator.
 */
static Node *
eval_stable_functions_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;
	switch (nodeTag(node))
	{
		case T_FuncExpr:
		{
			FuncExpr *expr = (FuncExpr *) node;
			List *args = expr->args;
			Expr *simple;
			FuncExpr *newexpr;

			/*
			 * Code for op/func reduction is pretty bulky, so split it out
			 * as a separate function.  Note: exprTypmod normally returns
			 * -1 for a FuncExpr, but not when the node is recognizably a
			 * length coercion; we want to preserve the typmod in the
			 * eventual Const if so.
			 */
			simple = simplify_stable_function(expr->funcid,
											  expr->funcresulttype,
											  exprTypmod(node),
											  expr->funccollid,
											  expr->inputcollid,
											  &args,
											  expr->funcvariadic);
			if (simple) /* successfully simplified it */
				return (Node *) simple;

			/*
			 * The expression cannot be simplified any further, so build
			 * and return a replacement FuncExpr node using the
			 * possibly-simplified arguments.  Note that we have also
			 * converted the argument list to positional notation.
			 */
			newexpr = makeNode(FuncExpr);
			newexpr->funcid = expr->funcid;
			newexpr->funcresulttype = expr->funcresulttype;
			newexpr->funcretset = expr->funcretset;
			newexpr->funcvariadic = expr->funcvariadic;
			newexpr->funcformat = expr->funcformat;
			newexpr->funccollid = expr->funccollid;
			newexpr->inputcollid = expr->inputcollid;
			newexpr->args = args;
			newexpr->location = expr->location;
			return (Node *) newexpr;
		}
		case T_OpExpr:
		{
			OpExpr *expr = (OpExpr *) node;
			List *args = expr->args;
			Expr *simple;
			OpExpr *newexpr;

			/*
			 * Need to get OID of underlying function.  Okay to scribble
			 * on input to this extent.
			 */
			set_opfuncid(expr);

			/*
			 * Code for op/func reduction is pretty bulky, so split it out
			 * as a separate function.
			 */
			simple = simplify_stable_function(expr->opfuncid,
											  expr->opresulttype,
											  -1,
											  expr->opcollid,
											  expr->inputcollid,
											  &args,
											  false);
			if (simple) /* successfully simplified it */
				return (Node *) simple;

			/*
			 * The expression cannot be simplified any further, so build
			 * and return a replacement OpExpr node using the
			 * possibly-simplified arguments.
			 */
			newexpr = makeNode(OpExpr);
			newexpr->opno = expr->opno;
			newexpr->opfuncid = expr->opfuncid;
			newexpr->opresulttype = expr->opresulttype;
			newexpr->opretset = expr->opretset;
			newexpr->opcollid = expr->opcollid;
			newexpr->inputcollid = expr->inputcollid;
			newexpr->args = args;
			newexpr->location = expr->location;
			return (Node *) newexpr;
		}
		default:
			break;
	}
	/*
	 * For any node type not handled above, copy the node unchanged but
	 * const-simplify its subexpressions.  This is the correct thing for node
	 * types whose behavior might change between planning and execution, such
	 * as CurrentOfExpr.  It's also a safe default for new node types not
	 * known to this routine.
	 */
	return expression_tree_mutator((Node *) node, eval_stable_functions_mutator, NULL);
}

/*
 * Try to evaluate stable functions and operators on the access node. This
 * function is similar to eval_const_expressions, but much simpler, because it
 * only evaluates the functions and doesn't have to perform any additional
 * canonicalizations.
 */
static Node *
eval_stable_functions(PlannerInfo *root, Node *node)
{
	return eval_stable_functions_mutator(node, NULL);
}

void
fdw_scan_info_init(ScanInfo *scaninfo, PlannerInfo *root, RelOptInfo *rel, Path *best_path,
				   List *scan_clauses, Plan *outer_plan)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	List *remote_having = NIL;
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *params_list = NIL;
	List *fdw_scan_tlist = NIL;
	List *fdw_recheck_quals = NIL;
	List *retrieved_attrs;
	List *fdw_private;
	Index scan_relid;
	StringInfoData sql;
	ListCell *lc;

	if (IS_SIMPLE_REL(rel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = rel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by GetForeignPaths.  Possibly it'd be worth
		 * passing forward the classification work done then, rather than
		 * repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach (lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (ts_is_foreign_expr(root, rel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else if (IS_JOIN_REL(rel))
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(rel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan)
		{
			ListCell *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(rel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach (lc, local_exprs)
			{
				Node *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) || IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual, qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan =
				change_plan_targetlist(outer_plan, fdw_scan_tlist, best_path->parallel_safe);
		}
	}
	else
	{
		/*
		 * Upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 * For upper relations, the WHERE clause is built from the remote
		 * conditions of the underlying scan relation.
		 */
		TsFdwRelInfo *ofpinfo;
		ofpinfo = fdw_relinfo_get(fpinfo->outerrel);
		remote_exprs = extract_actual_clauses(ofpinfo->remote_conds, false);
		remote_having = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see GetForeignJoinPaths().  If
		 * we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the data node. */
		fdw_scan_tlist = build_tlist_to_deparse(rel);
	}

	/*
	 * Try to locally evaluate the stable functions such as now() before pushing
	 * them to the remote node.
	 * We have to do this at the execution stage as oppossed to the planning stage, because stable
	 * functions must be recalculated with each execution of a prepared
	 * statement.
	 * Note that the query planner currently only pushes down to remote side
	 * the whitelisted stable functions, see `function_is_whitelisted()`. So
	 * this code only has to deal with such functions.
	 */
	remote_exprs = (List *) eval_stable_functions(root, (Node *) remote_exprs);
	remote_having = (List *) eval_stable_functions(root, (Node *) remote_having);

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql,
							root,
							rel,
							fdw_scan_tlist,
							remote_exprs,
							remote_having,
							best_path->pathkeys,
							false,
							&retrieved_attrs,
							&params_list,
							fpinfo->sca);

	/* Remember remote_exprs for possible use by PlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/* Build the chunk oid list for use by EXPLAIN. */
	List *chunk_oids = NIL;
	if (fpinfo->sca)
	{
		foreach (lc, fpinfo->sca->chunks)
		{
			Chunk *chunk = (Chunk *) lfirst(lc);
			chunk_oids = lappend_oid(chunk_oids, chunk->table_id);
		}
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(fpinfo->fetch_size),
							 makeInteger(fpinfo->server->serverid),
							 chunk_oids);

	if (IS_UPPER_REL(rel))
		fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name->data));

	scaninfo->fdw_private = fdw_private;
	scaninfo->fdw_scan_tlist = fdw_scan_tlist;
	scaninfo->fdw_recheck_quals = fdw_recheck_quals;
	scaninfo->local_exprs = local_exprs;
	scaninfo->params_list = params_list;
	scaninfo->scan_relid = scan_relid;
	scaninfo->data_node_serverid = rel->serverid;
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to be NULL.
 */
static void
merge_fdw_options(TsFdwRelInfo *fpinfo, const TsFdwRelInfo *fpinfo_o, const TsFdwRelInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i || fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/* Currently, we don't support JOINs, so Asserting fpinfo_i is NULL here
	 * in the meantime. */
	Assert(fpinfo_i == NULL);

	/*
	 * Copy the server specific FDW options. (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->fetch_size = fpinfo_o->fetch_size;
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the data node.  As a side effect, save information we obtain in
 * this function to TsFdwRelInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel, GroupPathExtraData *extra)
{
	Query *query = root->parse;
	Node *having_qual = extra->havingQual;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(grouped_rel);
	PathTarget *grouping_target = grouped_rel->reltarget;
	bool ispartial = extra->patype == PARTITIONWISE_AGGREGATE_PARTIAL;
	TsFdwRelInfo *ofpinfo;
	List *aggvars;
	ListCell *lc;
	int i;
	List *tlist = NIL;

	/* Cannot have grouping sets since that wouldn't be a distinct coverage of
	 * all partition keys */
	Assert(query->groupingSets == NIL);

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (TsFdwRelInfo *) fdw_relinfo_get(fpinfo->outerrel);

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the data
	 * node.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to data node.
	 */
	i = 0;
	foreach (lc, grouping_target->exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);
		Index sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the data node.
			 */
			if (!ts_is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Is it shippable?
			 */
			if (ts_is_foreign_expr(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				aggvars = pull_var_clause((Node *) expr, PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the data node.
				 */
				if (!ts_is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the data node to complain
				 * that the shipped query is invalid.
				 */
				foreach (l, aggvars)
				{
					Expr *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * For non-partial aggregations, classify the pushable and non-pushable
	 * HAVING clauses and save them in remote_conds and local_conds of the
	 * grouped rel's fpinfo.
	 *
	 * For partial agggregations, we never push-down the HAVING clause since
	 * it either has (1) been reduced by the planner to a simple filter on the
	 * base rel, or, in case of aggregates, the aggregates must be partials
	 * and have therefore been pulled up into the target list (unless they're
	 * already there). Any partial aggregates in the HAVING clause must be
	 * finalized on the access node and applied there.
	 */
	if (having_qual && !ispartial)
	{
		ListCell *lc;

		foreach (lc, (List *) having_qual)
		{
			Expr *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo_compat(root,
											 expr,
											 true,
											 false,
											 false,
											 false,
											 false,
											 root->qual_security_level,
											 grouped_rel->relids,
											 NULL,
											 NULL,
											 NULL);
			if (ts_is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List *aggvars = NIL;
		ListCell *lc;

		foreach (lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause, PVC_INCLUDE_AGGREGATES));
		}

		foreach (lc, aggvars)
		{
			Expr *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.
			 */
			if (IsA(expr, Aggref))
			{
				if (!ts_is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs, during one (usually the
	 * first) of the calls to fdw_estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)", ofpinfo->relation_name->data);

	return true;
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra, CreateUpperPathFunc create_path)
{
	Query *parse = root->parse;
	TsFdwRelInfo *ifpinfo = fdw_relinfo_get(input_rel);
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(grouped_rel);
	Path *grouppath;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs && !root->hasHavingQual)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, data node, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->sca = ifpinfo->sca;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra))
		return;

	/* Estimate the cost of push down */
	fdw_estimate_path_cost_size(root, grouped_rel, NIL, &rows, &width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add path to the grouping relation. */
	grouppath = (Path *) create_path(root,
									 grouped_rel,
									 grouped_rel->reltarget,
									 rows,
									 startup_cost,
									 total_cost,
									 NIL, /* no pathkeys */
									 NULL,
									 NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	fdw_utils_add_path(grouped_rel, grouppath);

	/* Add paths with pathkeys if there's an order by clause */
	if (root->sort_pathkeys != NIL)
		fdw_add_upper_paths_with_pathkeys_for_rel(root, grouped_rel, NULL, create_path);
}

void
fdw_create_upper_paths(TsFdwRelInfo *input_fpinfo, PlannerInfo *root, UpperRelationKind stage,
					   RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra,
					   CreateUpperPathFunc create_path)
{
	Assert(input_fpinfo != NULL);

	TsFdwRelInfo *output_fpinfo = NULL;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the data node.
	 */
	if (!input_fpinfo->pushdown_safe)
		return;

	/* Skip any duplicate calls (i.e., output_rel->fdw_private has already
	 * been set by a previous call to this function). */
	if (output_rel->fdw_private)
		return;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
		case UPPERREL_PARTIAL_GROUP_AGG:
			output_fpinfo = fdw_relinfo_alloc_or_get(output_rel);
			output_fpinfo->type = input_fpinfo->type;
			output_fpinfo->pushdown_safe = false;
			add_foreign_grouping_paths(root,
									   input_rel,
									   output_rel,
									   (GroupPathExtraData *) extra,
									   create_path);
			break;
			/* Currently not handled (or received) */
		case UPPERREL_DISTINCT:
		case UPPERREL_ORDERED:
		case UPPERREL_SETOP:
		case UPPERREL_WINDOW:
		case UPPERREL_FINAL:
#if PG15_GE
		case UPPERREL_PARTIAL_DISTINCT:
#endif
			break;
	}
}
