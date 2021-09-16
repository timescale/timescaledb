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
 *
 * These function were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_statistic.h>
#include <executor/nodeAgg.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/paramassign.h>
#include <optimizer/paths.h>
#include <optimizer/placeholder.h>
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "compat/compat.h"
#include "planner.h"

#if PG_VERSION_NUM < 130003
static EquivalenceMember *find_ec_member_matching_expr(EquivalenceClass *ec, Expr *expr,
													   Relids relids);
static EquivalenceMember *find_computable_ec_member(PlannerInfo *root, EquivalenceClass *ec,
													List *exprs, Relids relids,
													bool require_parallel_safe);
#endif
static Node *replace_nestloop_params(PlannerInfo *root, Node *expr);
static Node *replace_nestloop_params_mutator(Node *node, PlannerInfo *root);
static Plan *inject_projection_plan(Plan *subplan, List *tlist, bool parallel_safe);

/* copied verbatim from prepunion.c */
void
ts_make_inh_translation_list(Relation oldrelation, Relation newrelation, Index newvarno,
							 List **translated_vars)
{
	List *vars = NIL;
	TupleDesc old_tupdesc = RelationGetDescr(oldrelation);
	TupleDesc new_tupdesc = RelationGetDescr(newrelation);
	int oldnatts = old_tupdesc->natts;
	int newnatts = new_tupdesc->natts;
	int old_attno;

	for (old_attno = 0; old_attno < oldnatts; old_attno++)
	{
		Form_pg_attribute att;
		char *attname;
		Oid atttypid;
		int32 atttypmod;
		Oid attcollation;
		int new_attno;

		att = TupleDescAttr(old_tupdesc, old_attno);
		if (att->attisdropped)
		{
			/* Just put NULL into this list entry */
			vars = lappend(vars, NULL);
			continue;
		}
		attname = NameStr(att->attname);
		atttypid = att->atttypid;
		atttypmod = att->atttypmod;
		attcollation = att->attcollation;

		/*
		 * When we are generating the "translation list" for the parent table
		 * of an inheritance set, no need to search for matches.
		 */
		if (oldrelation == newrelation)
		{
			vars = lappend(vars,
						   makeVar(newvarno,
								   (AttrNumber)(old_attno + 1),
								   atttypid,
								   atttypmod,
								   attcollation,
								   0));
			continue;
		}

		/*
		 * Otherwise we have to search for the matching column by name.
		 * There's no guarantee it'll have the same column position, because
		 * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
		 * However, in simple cases it will be the same column number, so try
		 * that before we go groveling through all the columns.
		 *
		 * Note: the test for (att = ...) != NULL cannot fail, it's just a
		 * notational device to include the assignment into the if-clause.
		 */
		if (old_attno < newnatts && (att = TupleDescAttr(new_tupdesc, old_attno)) != NULL &&
			!att->attisdropped && strcmp(attname, NameStr(att->attname)) == 0)
			new_attno = old_attno;
		else
		{
			for (new_attno = 0; new_attno < newnatts; new_attno++)
			{
				att = TupleDescAttr(new_tupdesc, new_attno);
				if (!att->attisdropped && strcmp(attname, NameStr(att->attname)) == 0)
					break;
			}
			if (new_attno >= newnatts)
				elog(ERROR,
					 "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname,
					 RelationGetRelationName(newrelation));
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR,
				 "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname,
				 RelationGetRelationName(newrelation));
		if (attcollation != att->attcollation)
			elog(ERROR,
				 "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname,
				 RelationGetRelationName(newrelation));

		vars = lappend(vars,
					   makeVar(newvarno,
							   (AttrNumber)(new_attno + 1),
							   atttypid,
							   atttypmod,
							   attcollation,
							   0));
	}

	*translated_vars = vars;
}

/* copied verbatim from planner.c */
struct PathTarget *
ts_make_partial_grouping_target(struct PlannerInfo *root, PathTarget *grouping_target)
{
	struct Query *parse = root->parse;
	PathTarget *partial_target;
	struct List *non_group_cols;
	struct List *non_group_exprs;
	int i;
	ListCell *lc;

	partial_target = create_empty_pathtarget();
	non_group_cols = NIL;

	i = 0;
	foreach (lc, grouping_target->exprs)
	{
		struct Expr *expr = (struct Expr *) lfirst(lc);
		unsigned int sgref = get_pathtarget_sortgroupref(grouping_target, i);

		if (sgref && parse->groupClause &&
			get_sortgroupref_clause_noerr(sgref, parse->groupClause) != NULL)
		{
			/*
			 * It's a grouping column, so add it to the partial_target as-is.
			 * (This allows the upper agg step to repeat the grouping calcs.)
			 */
			add_column_to_pathtarget(partial_target, expr, sgref);
		}
		else
		{
			/*
			 * Non-grouping column, so just remember the expression for later
			 * call to pull_var_clause.
			 */
			non_group_cols = lappend(non_group_cols, expr);
		}

		i++;
	}

	/*
	 * If there's a HAVING clause, we'll need the Vars/Aggrefs it uses, too.
	 */
	if (parse->havingQual)
		non_group_cols = lappend(non_group_cols, parse->havingQual);

	/*
	 * Pull out all the Vars, PlaceHolderVars, and Aggrefs mentioned in
	 * non-group cols (plus HAVING), and add them to the partial_target if not
	 * already present.  (An expression used directly as a GROUP BY item will
	 * be present already.)  Note this includes Vars used in resjunk items, so
	 * we are covering the needs of ORDER BY and window specifications.
	 */
	non_group_exprs = pull_var_clause((struct Node *) non_group_cols,
									  PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS |
										  PVC_INCLUDE_PLACEHOLDERS);

	add_new_columns_to_pathtarget(partial_target, non_group_exprs);

	/*
	 * Adjust Aggrefs to put them in partial mode.  At this point all Aggrefs
	 * are at the top level of the target list, so we can just scan the list
	 * rather than recursing through the expression trees.
	 */
	foreach (lc, partial_target->exprs)
	{
		struct Aggref *aggref = (struct Aggref *) lfirst(lc);

		if (IsA(aggref, Aggref))
		{
			struct Aggref *newaggref;

			/*
			 * We shouldn't need to copy the substructure of the Aggref node,
			 * but flat-copy the node itself to avoid damaging other trees.
			 */
			newaggref = makeNode(Aggref);
			memcpy(newaggref, aggref, sizeof(struct Aggref));

			/* For now, assume serialization is required */
			mark_partial_aggref(newaggref, AGGSPLIT_INITIAL_SERIAL);

			lfirst(lc) = newaggref;
		}
	}

	/* clean up cruft */
	list_free(non_group_exprs);
	list_free(non_group_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, partial_target);
}

/* copied verbatim from selfuncs.c */
bool
ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop, Datum *min,
					  Datum *max)
{
	Datum tmin = 0;
	Datum tmax = 0;
	bool have_data = false;
	int16 typLen;
	bool typByVal;
	Oid opfuncoid;
	AttStatsSlot sslot;
	int i;

	/*
	 * XXX It's very tempting to try to use the actual column min and max, if
	 * we can get them relatively-cheaply with an index probe.  However, since
	 * this function is called many times during join planning, that could
	 * have unpleasant effects on planning speed.  Need more investigation
	 * before enabling this.
	 */
#ifdef NOT_USED
	if (get_actual_variable_range(root, vardata, sortop, min, max))
		return true;
#endif

	if (!HeapTupleIsValid(vardata->statsTuple))
	{
		/* no stats available, so default result */
		return false;
	}

	/*
	 * If we can't apply the sortop to the stats data, just fail.  In
	 * principle, if there's a histogram and no MCVs, we could return the
	 * histogram endpoints without ever applying the sortop ... but it's
	 * probably not worth trying, because whatever the caller wants to do with
	 * the endpoints would likely fail the security check too.
	 */
	if (!statistic_proc_security_check(vardata, (opfuncoid = get_opcode(sortop))))
		return false;

	get_typlenbyval(vardata->atttype, &typLen, &typByVal);

	/*
	 * If there is a histogram, grab the first and last values.
	 *
	 * If there is a histogram that is sorted with some other operator than
	 * the one we want, fail --- this suggests that there is data we can't
	 * use.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_HISTOGRAM,
						 sortop,
						 ATTSTATSSLOT_VALUES))
	{
		if (sslot.nvalues > 0)
		{
			tmin = datumCopy(sslot.values[0], typByVal, typLen);
			tmax = datumCopy(sslot.values[sslot.nvalues - 1], typByVal, typLen);
			have_data = true;
		}
		free_attstatsslot(&sslot);
	}
	else if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, 0))
	{
		free_attstatsslot(&sslot);
		return false;
	}

	/*
	 * If we have most-common-values info, look for extreme MCVs.  This is
	 * needed even if we also have a histogram, since the histogram excludes
	 * the MCVs.  However, usually the MCVs will not be the extreme values, so
	 * avoid unnecessary data copying.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_MCV,
						 InvalidOid,
						 ATTSTATSSLOT_VALUES))
	{
		bool tmin_is_mcv = false;
		bool tmax_is_mcv = false;
		FmgrInfo opproc;

		fmgr_info(opfuncoid, &opproc);

		for (i = 0; i < sslot.nvalues; i++)
		{
			if (!have_data)
			{
				tmin = tmax = sslot.values[i];
				tmin_is_mcv = tmax_is_mcv = have_data = true;
				continue;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, sslot.values[i], tmin)))
			{
				tmin = sslot.values[i];
				tmin_is_mcv = true;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, tmax, sslot.values[i])))
			{
				tmax = sslot.values[i];
				tmax_is_mcv = true;
			}
		}
		if (tmin_is_mcv)
			tmin = datumCopy(tmin, typByVal, typLen);
		if (tmax_is_mcv)
			tmax = datumCopy(tmax, typByVal, typLen);
		free_attstatsslot(&sslot);
	}

	*min = tmin;
	*max = tmax;
	return have_data;
}

#if PG_VERSION_NUM < 130003
/*
 * find_ec_member_matching_expr
 *		Locate an EquivalenceClass member matching the given expr, if any;
 *		return NULL if no match.
 *
 * "Matching" is defined as "equal after stripping RelabelTypes".
 * This is used for identifying sort expressions, and we need to allow
 * binary-compatible relabeling for some cases involving binary-compatible
 * sort operators.
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 */
static EquivalenceMember *
find_ec_member_matching_expr(EquivalenceClass *ec, Expr *expr, Relids relids)
{
	ListCell *lc;

	/* We ignore binary-compatible relabeling on both ends */
	while (expr && IsA(expr, RelabelType))
		expr = ((RelabelType *) expr)->arg;

	foreach (lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the requested rel.
		 */
		if (em->em_is_child && !bms_is_subset(em->em_relids, relids))
			continue;

		/*
		 * Match if same expression (after stripping relabel).
		 */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, expr))
			return em;
	}

	return NULL;
}

/*
 * is_exprlist_member
 *	  Subroutine for find_computable_ec_member: is "node" in "exprs"?
 *
 * Per the requirements of that function, "exprs" might or might not have
 * TargetEntry superstructure.
 */
static bool
is_exprlist_member(Expr *node, List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);

		if (expr && IsA(expr, TargetEntry))
			expr = ((TargetEntry *) expr)->expr;

		if (equal(node, expr))
			return true;
	}
	return false;
}

/*
 * find_computable_ec_member
 *		Locate an EquivalenceClass member that can be computed from the
 *		expressions appearing in "exprs"; return NULL if no match.
 *
 * "exprs" can be either a list of bare expression trees, or a list of
 * TargetEntry nodes.  Either way, it should contain Vars and possibly
 * Aggrefs and WindowFuncs, which are matched to the corresponding elements
 * of the EquivalenceClass's expressions.
 *
 * Unlike find_ec_member_matching_expr, there's no special provision here
 * for binary-compatible relabeling.  This is intentional: if we have to
 * compute an expression in this way, setrefs.c is going to insist on exact
 * matches of Vars to the source tlist.
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 * Also, non-parallel-safe expressions are ignored if 'require_parallel_safe'.
 *
 * Note: some callers pass root == NULL for notational reasons.  This is OK
 * when require_parallel_safe is false.
 */
static EquivalenceMember *
find_computable_ec_member(PlannerInfo *root, EquivalenceClass *ec, List *exprs, Relids relids,
						  bool require_parallel_safe)
{
	ListCell *lc;

	foreach (lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		List *exprvars;
		ListCell *lc2;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the requested rel.
		 */
		if (em->em_is_child && !bms_is_subset(em->em_relids, relids))
			continue;

		/*
		 * Match if all Vars and quasi-Vars are available in "exprs".
		 */
		exprvars = pull_var_clause((Node *) em->em_expr,
								   PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
		foreach (lc2, exprvars)
		{
			if (!is_exprlist_member(lfirst(lc2), exprs))
				break;
		}
		list_free(exprvars);
		if (lc2)
			continue; /* we hit a non-available Var */

		/*
		 * If requested, reject expressions that are not parallel-safe.  We
		 * check this last because it's a rather expensive test.
		 */
		if (require_parallel_safe && !is_parallel_safe(root, (Node *) em->em_expr))
			continue;

		return em; /* found usable expression */
	}

	return NULL;
}
#endif

/*
 * make_pathkey_from_sortinfo
 *    Given an expression and sort-order information, create a PathKey.
 *    The result is always a "canonical" PathKey, but it might be redundant.
 *
 * expr is the expression, and nullable_relids is the set of base relids
 * that are potentially nullable below it.
 *
 * If the PathKey is being generated from a SortGroupClause, sortref should be
 * the SortGroupClause's SortGroupRef; otherwise zero.
 *
 * If rel is not NULL, it identifies a specific relation we're considering
 * a path for, and indicates that child EC members for that relation can be
 * considered.  Otherwise child members are ignored.  (See the comments for
 * get_eclass_for_sort_expr.)
 *
 * create_it is true if we should create any missing EquivalenceClass
 * needed to represent the sort key.  If it's false, we return NULL if the
 * sort key isn't already present in any EquivalenceClass.
 */
PathKey *
ts_make_pathkey_from_sortinfo(PlannerInfo *root, Expr *expr, Relids nullable_relids, Oid opfamily,
							  Oid opcintype, Oid collation, bool reverse_sort, bool nulls_first,
							  Index sortref, Relids rel, bool create_it)
{
	int16 strategy;
	Oid equality_op;
	List *opfamilies;
	EquivalenceClass *eclass;

	strategy = reverse_sort ? BTGreaterStrategyNumber : BTLessStrategyNumber;

	/*
	 * EquivalenceClasses need to contain opfamily lists based on the family
	 * membership of mergejoinable equality operators, which could belong to
	 * more than one opfamily.  So we have to look up the opfamily's equality
	 * operator and get its membership.
	 */
	equality_op = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
	if (!OidIsValid(equality_op)) /* shouldn't happen */
		elog(ERROR,
			 "missing operator %d(%u,%u) in opfamily %u",
			 BTEqualStrategyNumber,
			 opcintype,
			 opcintype,
			 opfamily);
	opfamilies = get_mergejoin_opfamilies(equality_op);
	if (!opfamilies) /* certainly should find some */
		elog(ERROR, "could not find opfamilies for equality operator %u", equality_op);

	/* Now find or (optionally) create a matching EquivalenceClass */
	eclass = get_eclass_for_sort_expr(root,
									  expr,
									  nullable_relids,
									  opfamilies,
									  opcintype,
									  collation,
									  sortref,
									  rel,
									  create_it);

	/* Fail if no EC and !create_it */
	if (!eclass)
		return NULL;

	/* And finally we can find or create a PathKey node */
	return make_canonical_pathkey(root, eclass, opfamily, strategy, nulls_first);
}

/*
 * make_pathkey_from_sortop
 *    Like make_pathkey_from_sortinfo, but work from a sort operator.
 *
 * This should eventually go away, but we need to restructure SortGroupClause
 * first.
 */
PathKey *
ts_make_pathkey_from_sortop(PlannerInfo *root, Expr *expr, Relids nullable_relids, Oid ordering_op,
							bool nulls_first, Index sortref, bool create_it)
{
	Oid opfamily, opcintype, collation;
	int16 strategy;

	/* Find the operator in pg_amop --- failure shouldn't happen */
	if (!get_ordering_op_properties(ordering_op, &opfamily, &opcintype, &strategy))
		elog(ERROR, "operator %u is not a valid ordering operator", ordering_op);

	/* Because SortGroupClause doesn't carry collation, consult the expr */
	collation = exprCollation((Node *) expr);

	return ts_make_pathkey_from_sortinfo(root,
										 expr,
										 nullable_relids,
										 opfamily,
										 opcintype,
										 collation,
										 (strategy == BTGreaterStrategyNumber),
										 nulls_first,
										 sortref,
										 NULL,
										 create_it);
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx, sortOperators, collations, and
 * nullsFirst arrays already.
 */
static Sort *
make_sort(Plan *lefttree, int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations,
		  bool *nullsFirst)
{
	Sort *node = makeNode(Sort);
	Plan *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

	return node;
}

/*
 * make_sort_from_pathkeys
 *    Create sort plan to sort according to given pathkeys
 *
 *    'lefttree' is the node which yields input tuples
 *    'pathkeys' is the list of pathkeys by which the result is to be sorted
 *    'relids' is the set of relations required by prepare_sort_from_pathkeys()
 */
Sort *
ts_make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids)
{
	int numsortkeys;
	AttrNumber *sortColIdx;
	Oid *sortOperators;
	Oid *collations;
	bool *nullsFirst;

	/* Compute sort column info, and adjust lefttree as needed */
	lefttree = ts_prepare_sort_from_pathkeys(lefttree,
											 pathkeys,
											 relids,
											 NULL,
											 false,
											 &numsortkeys,
											 &sortColIdx,
											 &sortOperators,
											 &collations,
											 &nullsFirst);

	/* Now build the Sort node */
	return make_sort(lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst);
}

/*
 * prepare_sort_from_pathkeys
 *	  Prepare to sort according to given pathkeys
 *
 * This is used to set up for Sort, MergeAppend, and Gather Merge nodes.  It
 * calculates the executor's representation of the sort key information, and
 * adjusts the plan targetlist if needed to add resjunk sort columns.
 *
 * Input parameters:
 *	  'lefttree' is the plan node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' identifies the child relation being sorted, if any
 *	  'reqColIdx' is NULL or an array of required sort key column numbers
 *	  'adjust_tlist_in_place' is true if lefttree must be modified in-place
 *
 * We must convert the pathkey information into arrays of sort key column
 * numbers, sort operator OIDs, collation OIDs, and nulls-first flags,
 * which is the representation the executor wants.  These are returned into
 * the output parameters *p_numsortkeys etc.
 *
 * When looking for matches to an EquivalenceClass's members, we will only
 * consider child EC members if they belong to given 'relids'.  This protects
 * against possible incorrect matches to child expressions that contain no
 * Vars.
 *
 * If reqColIdx isn't NULL then it contains sort key column numbers that
 * we should match.  This is used when making child plans for a MergeAppend;
 * it's an error if we can't match the columns.
 *
 * If the pathkeys include expressions that aren't simple Vars, we will
 * usually need to add resjunk items to the input plan's targetlist to
 * compute these expressions, since a Sort or MergeAppend node itself won't
 * do any such calculations.  If the input plan type isn't one that can do
 * projections, this means adding a Result node just to do the projection.
 * However, the caller can pass adjust_tlist_in_place = true to force the
 * lefttree tlist to be modified in-place regardless of whether the node type
 * can project --- we use this for fixing the tlist of MergeAppend itself.
 *
 * Returns the node which is to be the input to the Sort (either lefttree,
 * or a Result stacked atop lefttree).
 *
 * static function copied from createplan.c
 */
Plan *
ts_prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids,
							  const AttrNumber *reqColIdx, bool adjust_tlist_in_place,
							  int *p_numsortkeys, AttrNumber **p_sortColIdx, Oid **p_sortOperators,
							  Oid **p_collations, bool **p_nullsFirst)
{
	List *tlist = lefttree->targetlist;
	ListCell *i;
	int numsortkeys;
	AttrNumber *sortColIdx;
	Oid *sortOperators;
	Oid *collations;
	bool *nullsFirst;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	foreach (i, pathkeys)
	{
		PathKey *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid pk_datatype = InvalidOid;
		Oid sortop;
		ListCell *j;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0) /* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else if (reqColIdx != NULL)
		{
			/*
			 * If we are given a sort column number to match, only consider
			 * the single TLE at that position.  It's possible that there is
			 * no such TLE, in which case fall through and generate a resjunk
			 * targetentry (we assume this must have happened in the parent
			 * plan as well).  If there is a TLE but it doesn't match the
			 * pathkey's EC, we do the same, which is probably the wrong thing
			 * but we'll leave it to caller to complain about the mismatch.
			 */
			tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
			if (tle)
			{
				em = find_ec_member_matching_expr(ec, tle->expr, relids);
				if (em)
				{
					/* found expr at right place in tlist */
					pk_datatype = em->em_datatype;
				}
				else
					tle = NULL;
			}
		}
		else
		{
			/*
			 * Otherwise, we can sort by any non-constant expression listed in
			 * the pathkey's EquivalenceClass.  For now, we take the first
			 * tlist item found in the EC. If there's no match, we'll generate
			 * a resjunk entry using the first EC member that is an expression
			 * in the input's vars.  (The non-const restriction only matters
			 * if the EC is below_outer_join; but if it isn't, it won't
			 * contain consts anyway, else we'd have discarded the pathkey as
			 * redundant.)
			 *
			 * XXX if we have a choice, is there any way of figuring out which
			 * might be cheapest to execute?  (For example, int4lt is likely
			 * much cheaper to execute than numericlt, but both might appear
			 * in the same equivalence class...)  Not clear that we ever will
			 * have an interesting choice in practice, so it may not matter.
			 */
			foreach (j, tlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_matching_expr(ec, tle->expr, relids);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
		{
			/*
			 * No matching tlist item; look for a computable expression.
			 */
			em = find_computable_ec_member(NULL, ec, tlist, relids, false);
			if (!em)
				elog(ERROR, "could not find pathkey item to sort");
			pk_datatype = em->em_datatype;

			/*
			 * Do we need to insert a Result node?
			 */
			if (!adjust_tlist_in_place && !is_projection_capable_plan(lefttree))
			{
				/* copy needed so we don't modify input's tlist below */
				tlist = copyObject(tlist);
				lefttree = inject_projection_plan(lefttree, tlist, lefttree->parallel_safe);
			}

			/* Don't bother testing is_projection_capable_plan again */
			adjust_tlist_in_place = true;

			/*
			 * Add resjunk entry to input's tlist
			 */
			tle = makeTargetEntry(copyObject(em->em_expr), list_length(tlist) + 1, NULL, true);
			tlist = lappend(tlist, tle);
			lefttree->targetlist = tlist; /* just in case NIL before */
		}

		/*
		 * Look up the correct sort operator from the PathKey's slightly
		 * abstracted representation.
		 */
		sortop = get_opfamily_member(pathkey->pk_opfamily,
									 pk_datatype,
									 pk_datatype,
									 pathkey->pk_strategy);
		if (!OidIsValid(sortop)) /* should not happen */
			elog(ERROR,
				 "missing operator %d(%u,%u) in opfamily %u",
				 pathkey->pk_strategy,
				 pk_datatype,
				 pk_datatype,
				 pathkey->pk_opfamily);

		/* Add the column to the sort arrays */
		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortop;
		collations[numsortkeys] = ec->ec_collation;
		nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
		numsortkeys++;
	}

	/* Return results */
	*p_numsortkeys = numsortkeys;
	*p_sortColIdx = sortColIdx;
	*p_sortOperators = sortOperators;
	*p_collations = collations;
	*p_nullsFirst = nullsFirst;

	return lefttree;
}

/*
 * copied verbatim from createplan.c
 */
List *
ts_build_path_tlist(PlannerInfo *root, Path *path)
{
	List *tlist = NIL;
	Index *sortgrouprefs = path->pathtarget->sortgrouprefs;
	int resno = 1;
	ListCell *v;

	foreach (v, path->pathtarget->exprs)
	{
		Node *node = (Node *) lfirst(v);
		TargetEntry *tle;

		/*
		 * If it's a parameterized path, there might be lateral references in
		 * the tlist, which need to be replaced with Params.  There's no need
		 * to remake the TargetEntry nodes, so apply this to each list item
		 * separately.
		 */
		if (path->param_info)
			node = replace_nestloop_params(root, node);

		tle = makeTargetEntry((Expr *) node, resno, NULL, false);
		if (sortgrouprefs)
			tle->ressortgroupref = sortgrouprefs[resno - 1];

		tlist = lappend(tlist, tle);
		resno++;
	}
	return tlist;
}

/*
 * replace_nestloop_params
 *    Replace outer-relation Vars and PlaceHolderVars in the given expression
 *    with nestloop Params
 *
 * All Vars and PlaceHolderVars belonging to the relation(s) identified by
 * root->curOuterRels are replaced by Params, and entries are added to
 * root->curOuterParams if not already present.
 */
static Node *
replace_nestloop_params(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_nestloop_params_mutator(expr, root);
}

static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if (!bms_is_member(var->varno, root->curOuterRels))
			return node;
		/* Replace the Var with a nestloop Param */
		return (Node *) replace_nestloop_param_var(root, var);
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		/* Upper-level PlaceHolderVars should be long gone at this point */
		Assert(phv->phlevelsup == 0);

		/*
		 * Check whether we need to replace the PHV.  We use bms_overlap as a
		 * cheap/quick test to see if the PHV might be evaluated in the outer
		 * rels, and then grab its PlaceHolderInfo to tell for sure.
		 */
		if (!bms_overlap(phv->phrels, root->curOuterRels) ||
			!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at, root->curOuterRels))
		{
			/*
			 * We can't replace the whole PHV, but we might still need to
			 * replace Vars or PHVs within its expression, in case it ends up
			 * actually getting evaluated here.  (It might get evaluated in
			 * this plan node, or some child node; in the latter case we don't
			 * really need to process the expression here, but we haven't got
			 * enough info to tell if that's the case.)  Flat-copy the PHV
			 * node and then recurse on its expression.
			 *
			 * Note that after doing this, we might have different
			 * representations of the contents of the same PHV in different
			 * parts of the plan tree.  This is OK because equal() will just
			 * match on phid/phlevelsup, so setrefs.c will still recognize an
			 * upper-level reference to a lower-level copy of the same PHV.
			 */
			PlaceHolderVar *newphv = makeNode(PlaceHolderVar);

			memcpy(newphv, phv, sizeof(PlaceHolderVar));
			newphv->phexpr = (Expr *) replace_nestloop_params_mutator((Node *) phv->phexpr, root);
			return (Node *) newphv;
		}
		/* Replace the PlaceHolderVar with a nestloop Param */
		return (Node *) replace_nestloop_param_placeholdervar(root, phv);
	}
	return expression_tree_mutator(node, replace_nestloop_params_mutator, (void *) root);
}

/*
 * make_result
 *	  Build a Result plan node
 */
static Result *
make_result(List *tlist, Node *resconstantqual, Plan *subplan)
{
	Result *node = makeNode(Result);
	Plan *plan = &node->plan;

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->resconstantqual = resconstantqual;

	return node;
}

/*
 * Copy cost and size info from a lower plan node to an inserted node.
 * (Most callers alter the info after copying it.)
 */
static void
copy_plan_costsize(Plan *dest, Plan *src)
{
	dest->startup_cost = src->startup_cost;
	dest->total_cost = src->total_cost;
	dest->plan_rows = src->plan_rows;
	dest->plan_width = src->plan_width;
	/* Assume the inserted node is not parallel-aware. */
	dest->parallel_aware = false;
	/* Assume the inserted node is parallel-safe, if child plan is. */
	dest->parallel_safe = src->parallel_safe;
}

/*
 * inject_projection_plan
 *	  Insert a Result node to do a projection step.
 *
 * This is used in a few places where we decide on-the-fly that we need a
 * projection step as part of the tree generated for some Path node.
 * We should try to get rid of this in favor of doing it more honestly.
 *
 * One reason it's ugly is we have to be told the right parallel_safe marking
 * to apply (since the tlist might be unsafe even if the child plan is safe).
 */
static Plan *
inject_projection_plan(Plan *subplan, List *tlist, bool parallel_safe)
{
	Plan *plan;

	plan = (Plan *) make_result(tlist, NULL, subplan);

	/*
	 * In principle, we should charge tlist eval cost plus cpu_per_tuple per
	 * row for the Result node.  But the former has probably been factored in
	 * already and the latter was not accounted for during Path construction,
	 * so being formally correct might just make the EXPLAIN output look less
	 * consistent not more so.  Hence, just copy the subplan's cost.
	 */
	copy_plan_costsize(plan, subplan);
	plan->parallel_safe = parallel_safe;

	return plan;
}
