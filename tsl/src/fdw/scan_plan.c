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
				!is_foreign_expr(root, rel, em_expr))
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
			Assert(IS_SIMPLE_REL(rel));
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
	bool has_null_input = false;
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
				   List *scan_clauses)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	List *remote_where = NIL;
	List *remote_having = NIL;
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
				remote_where = lappend(remote_where, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, rel, rinfo->clause))
				remote_where = lappend(remote_where, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_where;
	}
	else if (IS_JOIN_REL(rel))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));
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
		remote_where = extract_actual_clauses(ofpinfo->remote_conds, false);
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
	remote_where = (List *) eval_stable_functions(root, (Node *) remote_where);
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
							remote_where,
							remote_having,
							best_path->pathkeys,
							false,
							&retrieved_attrs,
							&params_list,
							fpinfo->sca);

	/* Remember remote_exprs for possible use by PlanDirectModify */
	fpinfo->final_remote_exprs = remote_where;

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
	Assert(!IS_JOIN_REL(rel));

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
			if (!is_foreign_expr(root, grouped_rel, expr))
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
			if (is_foreign_expr(root, grouped_rel, expr))
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
				if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
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
											 root->qual_security_level,
											 grouped_rel->relids,
											 NULL,
											 NULL);
			if (is_foreign_expr(root, grouped_rel, expr))
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
				if (!is_foreign_expr(root, grouped_rel, expr))
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
 * Find an equivalence class member expression to be computed as a sort column
 * in the given target.
 */
static Expr *
find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target)
{
	ListCell *lc1;
	int i;

	i = 0;
	foreach (lc1, target->exprs)
	{
		Expr *expr = (Expr *) lfirst(lc1);
		Index sgref = get_pathtarget_sortgroupref(target, i);
		ListCell *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 || get_sortgroupref_clause_noerr(sgref, root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach (lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (equal(em_expr, expr))
				return em->em_expr;
		}

		i++;
	}

	elog(ERROR, "could not find pathkey item to sort");
	return NULL; /* keep compiler quiet */
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

/*
 * add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *ordered_rel,
						  CreateUpperPathFunc create_path)
{
	Query *parse = root->parse;
	TsFdwRelInfo *ifpinfo = fdw_relinfo_get(input_rel);
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(ordered_rel);
	TsFdwPathExtraData *fpextra;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	List *fdw_private;
	Path *ordered_path;
	ListCell *lc;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->sca = ifpinfo->sca;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert((input_rel->reloptkind == RELOPT_UPPER_REL ||
			input_rel->reloptkind == RELOPT_OTHER_UPPER_REL) &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach (lc, root->sort_pathkeys)
	{
		PathKey *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		Expr *sort_expr;

		/*
		 * is_foreign_expr would detect volatile expressions as well, but
		 * checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/* Get the sort expression for the pathkey_ec */
		sort_expr = find_em_expr_for_input_target(root, pathkey_ec, input_rel->reltarget);

		/* If it's unsafe to remote, we cannot push down the final sort */
		if (!is_foreign_expr(root, input_rel, sort_expr))
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct TsFdwPathExtraData */
	fpextra = (TsFdwPathExtraData *) palloc0(sizeof(TsFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_ORDERED];
	fpextra->has_final_sort = true;

	/* Estimate the costs of performing the final sort remotely */
	fdw_estimate_path_cost_size(root,
								input_rel,
								root->sort_pathkeys,
								&rows,
								&width,
								&startup_cost,
								&total_cost);

	/*
	 * Build the fdw_private list that will be used by GetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(true), makeInteger(false));

	/* Create foreign ordering path */
	ordered_path = (Path *) create_path(root,
										input_rel,
										root->upper_targets[UPPERREL_ORDERED],
										rows,
										startup_cost,
										total_cost,
										root->sort_pathkeys,
										NULL, /* no extra plan */
										fdw_private);

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *final_rel,
						CreateUpperPathFunc create_path, FinalPathExtraData *extra)
{
	Query *parse = root->parse;
	TsFdwRelInfo *ifpinfo = fdw_relinfo_get(input_rel);
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(final_rel);
	bool has_final_sort = false;
	List *pathkeys = NIL;
	TsFdwPathExtraData *fpextra;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	List *fdw_private;
	Path *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->sca = ifpinfo->sca;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach (lc, input_rel->pathlist)
		{
			Path *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) && IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
				final_path = create_path(root,
										 path->parent,
										 path->pathtarget,
										 path->rows,
										 path->startup_cost,
										 path->total_cost,
										 path->pathkeys,
										 NULL,  /* no extra plan */
										 NULL); /* no fdw_private */

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = fdw_relinfo_get(input_rel);
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) ||
		!is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct TsFdwPathExtraData */
	fpextra = (TsFdwPathExtraData *) palloc0(sizeof(TsFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_FINAL];
	fpextra->has_final_sort = has_final_sort;
	fpextra->has_limit = extra->limit_needed;
	fpextra->limit_tuples = extra->limit_tuples;
	fpextra->count_est = extra->count_est;
	fpextra->offset_est = extra->offset_est;

	/*
	 * Estimate the costs of performing the final sort and the LIMIT
	 * restriction remotely.
	 */
	fdw_estimate_path_cost_size(root,
								input_rel,
								pathkeys,
								&rows,
								&width,
								&startup_cost,
								&total_cost);

	/*
	 * Build the fdw_private list that will be used by GetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort), makeInteger(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_path(root,
							 input_rel,
							 root->upper_targets[UPPERREL_FINAL],
							 rows,
							 startup_cost,
							 total_cost,
							 pathkeys,
							 NULL, /* no extra plan */
							 fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

#define MAX_UPPER_KIND UPPERREL_FINAL + 1
static UpperRelationKind
fetch_upper_relstage(PlannerInfo *root, RelOptInfo *input_rel)
{
	RelOptInfo *upperrel;
	ListCell *lc;
	UpperRelationKind i = UPPERREL_SETOP, kind; /* setop is first enum */

	while (i < UPPERREL_FINAL + 1)
	{
		kind = i++;
		foreach (lc, root->upper_rels[kind])
		{
			upperrel = (RelOptInfo *) lfirst(lc);

			if (upperrel == input_rel)
				return kind;
		}
	}

	/*
	 * This should never happen, we always expect an entry above. But
	 * we don't want to crash/error if we don't find an entry.
	 */
	return MAX_UPPER_KIND;
}

void
fdw_create_upper_paths(TsFdwRelInfo *input_fpinfo, PlannerInfo *root, UpperRelationKind stage,
					   RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra,
					   CreateUpperPathFunc create_path)
{
	UpperRelationKind input_stage;
	TsFdwRelInfo *output_fpinfo = NULL;

	/*
	 * Skip any duplicate calls (i.e., output_rel->fdw_private has already
	 * been set by a previous call to this function).
	 */
	if (output_rel->fdw_private)
		return;

	/*
	 * if input_fpinfo is NULL, then the input_rel might be the parent "UPPERREL" and there
	 * might be additional entries for this UpperRelationKind in the root. We will examine
	 * those and if they have fdw_private set then invoke the additional remote pushdown
	 * logic below.
	 *
	 * To identify the stage for the passed in input_rel, we need to go through the
	 * root->upper_rels entries one-by-one. That's not a problem typically because there
	 * won't be a lot of such entries
	 */

	if (input_fpinfo == NULL)
	{
		RelOptInfo *upperrel = NULL;
		TsFdwRelInfo *ufpinfo;
		ListCell *lc;

		/* We don't support sub queries yet */
		if (root->query_level > 1)
			return;

		input_stage = fetch_upper_relstage(root, input_rel);

		/* if we cannot deduce the stage nothing much can be done */
		if (input_stage >= MAX_UPPER_KIND)
			return;

		/*
		 * We only support one datanode plans as of now.
		 *
		 * The original passed in parent "input_rel" to this function and the actual child if it
		 * contains a filled up fdw_private entry.
		 */
		if (list_length(root->upper_rels[input_stage]) > 2)
			return;

		/*
		 * Get a list of all upper rels with filled in fdw_private entries. We are interested
		 * in pushdown with only those.
		 *
		 * Also, as stated above, we only support a list with 2 entries for this input_stage.
		 */
		foreach (lc, root->upper_rels[input_stage])
		{
			upperrel = (RelOptInfo *) lfirst(lc);
			;
			if (input_rel == upperrel)
				continue;
			else
				break;
		}

		Assert(upperrel != NULL);
		if (upperrel == NULL) /* keep warnings at bay */
			return;

		ufpinfo = upperrel->fdw_private ? fdw_relinfo_get(upperrel) : NULL;

		if (ufpinfo == NULL)
			return;

		/*
		 * Use this fpinfo for further processing.
		 * Also, set the fdw_private of this childrel in the parent
		 */
		input_fpinfo = ufpinfo;
		input_rel->fdw_private = upperrel->fdw_private;
	}

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the data node.
	 */
	if (!input_fpinfo->pushdown_safe)
		return;

	output_fpinfo = fdw_relinfo_alloc_or_get(output_rel);
	output_fpinfo->type = input_fpinfo->type;
	output_fpinfo->pushdown_safe = false;
	output_fpinfo->stage = stage;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
		case UPPERREL_PARTIAL_GROUP_AGG:
			add_foreign_grouping_paths(root,
									   input_rel,
									   output_rel,
									   (GroupPathExtraData *) extra,
									   create_path);
			break;

		case UPPERREL_ORDERED:
			add_foreign_ordered_paths(root, input_rel, output_rel, create_path);
			break;

		case UPPERREL_FINAL:
			add_foreign_final_paths(root,
									input_rel,
									output_rel,
									create_path,
									(FinalPathExtraData *) extra);
			break;

			/* Currently not handled (or received) */
		case UPPERREL_DISTINCT:
		case UPPERREL_SETOP:
		case UPPERREL_WINDOW:
#if PG15_GE
		case UPPERREL_PARTIAL_DISTINCT:
#endif
			break;
	}
}
