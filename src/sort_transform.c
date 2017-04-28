#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <utils/guc.h>
#include <optimizer/planner.h>
#include <optimizer/paths.h>
#include <utils/lsyscache.h>

/* This optimizations allows GROUP BY clauses that transform time in
 * order-preserving ways to use indexes on the time field. It works
 * by transforming sorting clauses from their more complex versions
 * to simplified ones that can use the plain index, if the transform
 * is order preserving.
 *
 * For example, an ordering on date_trunc('minute', time) can be transformed
 * to an ordering on time.
 */

extern void sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

/* sort_transforms_expr returns a simplified sort expression in a form
 * more common for indexes. Must return same data type & collation too.
 *
 * Sort transforms have the following correctness condition:
 *	Any ordering provided by the returned expression is a valid
 *	ordering under the original expression. The reverse need not
 *	be true.
 *
 * Namely if orig_expr(X) > orig_expr(Y) then
 *			 new_expr(X) > new_expr(Y).
 *
 * Note that if orig_expr(X) = orig_expr(Y) then
 *			 the ordering under new_expr is unconstrained.
 * */
static Expr *
sort_transform_expr(Expr *orig_expr)
{
	/*
	 * date_trunc (const, var) => var
	 *
	 * proof: date_trunc(c, time1) > date_trunc(c,time2) iff time1 > time2
	 */
	if (IsA(orig_expr, FuncExpr))
	{
		FuncExpr   *func = (FuncExpr *) orig_expr;
		char	   *func_name = get_func_name(func->funcid);
		Var		   *v;

		if (strncmp(func_name, "date_trunc", NAMEDATALEN) != 0)
			return NULL;

		if (!IsA(linitial(func->args), Const) ||!IsA(lsecond(func->args), Var))
			return NULL;

		v = lsecond(func->args);

		return (Expr *) copyObject(v);
	}
	return NULL;
}

/*	sort_transform_ec creates a new EquivalenceClass with transformed
 *	expressions if any of the members of the original EC can be transformed for the sort.
 */

static EquivalenceClass *
sort_transform_ec(PlannerInfo *root, EquivalenceClass *orig)
{
	ListCell   *lc_member;
	EquivalenceClass *newec = NULL;

	/* check all members, adding only tranformable members to new ec */
	foreach(lc_member, orig->ec_members)
	{
		EquivalenceMember *ec_mem = (EquivalenceMember *) lfirst(lc_member);
		Expr	   *transformed_expr = sort_transform_expr(ec_mem->em_expr);

		if (transformed_expr != NULL)
		{
			EquivalenceMember *em;

			/*
			 * if the transform already exists for even one member, assume
			 * exists for all
			 */
			EquivalenceClass *exist =
			get_eclass_for_sort_expr(root, transformed_expr, ec_mem->em_nullable_relids,
									 orig->ec_opfamilies, ec_mem->em_datatype,
									 orig->ec_collation, orig->ec_sortref,
									 ec_mem->em_relids, false);

			if (exist != NULL)
			{
				return exist;
			}

			em = makeNode(EquivalenceMember);

			em->em_expr = transformed_expr;
			em->em_relids = bms_copy(ec_mem->em_relids);
			em->em_nullable_relids = bms_copy(ec_mem->em_nullable_relids);
			em->em_is_const = ec_mem->em_is_const;
			em->em_is_child = ec_mem->em_is_child;
			em->em_datatype = ec_mem->em_datatype;

			if (newec == NULL)
			{
				/* lazy create the ec. */
				newec = makeNode(EquivalenceClass);
				newec->ec_opfamilies = list_copy(orig->ec_opfamilies);
				newec->ec_collation = orig->ec_collation;
				newec->ec_members = NIL;
				newec->ec_sources = list_copy(orig->ec_sources);
				newec->ec_derives = list_copy(orig->ec_derives);
				newec->ec_relids = bms_copy(orig->ec_relids);
				newec->ec_has_const = orig->ec_has_const;
				newec->ec_has_volatile = orig->ec_has_volatile;
				newec->ec_below_outer_join = orig->ec_below_outer_join;
				newec->ec_broken = orig->ec_broken;
				newec->ec_sortref = orig->ec_sortref;
				newec->ec_merged = orig->ec_merged;
			}

			newec->ec_members = lappend(newec->ec_members, em);
		}
	}
	/* if any transforms were found return new ec */
	if (newec != NULL)
	{
		root->eq_classes = lappend(root->eq_classes, newec);
		return newec;
	}
	return NULL;
}

/*
 *	This optimization transforms between equivalent sort operations to try
 *	to find useful indexes.
 *
 *	For example: an ORDER BY date_trunc('minute', time) can be implemented by
 *	an ordering of time.
 */
void
sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel)
{
	/*
	 * We attack this problem in three steps:
	 *
	 * 1) Create a pathkey for the transformed (simplified) sort.
	 *
	 * 2) Use the transformed pathkey to find new useful index paths.
	 *
	 * 3) Transform the  pathkey of the new paths back into the original form
	 * to make this transparent to upper levels in the planner.
	 *
	 * */
	ListCell   *lc_pathkey;
	List	   *transformed_query_pathkey = NIL;
	bool		was_transformed = false;

	/* build transformed query pathkeys */
	foreach(lc_pathkey, root->query_pathkeys)
	{
		PathKey    *pk = lfirst(lc_pathkey);
		EquivalenceClass *transformed = sort_transform_ec(root, pk->pk_eclass);

		if (transformed != NULL)
		{
			PathKey    *newpk = make_canonical_pathkey(root,
													   transformed, pk->pk_opfamily, pk->pk_strategy, pk->pk_nulls_first);

			was_transformed = true;
			transformed_query_pathkey = lappend(transformed_query_pathkey, newpk);
		}
		else
		{
			transformed_query_pathkey = lappend(transformed_query_pathkey, pk);

		}
	}

	if (was_transformed)
	{
		ListCell   *lc_plan;

		/* search for indexes on transformed pathkeys */
		List	   *orig_query_pathkeys = root->query_pathkeys;

		root->query_pathkeys = transformed_query_pathkey;
		create_index_paths(root, rel);
		root->query_pathkeys = orig_query_pathkeys;

		/*
		 * change returned paths to use original pathkeys. have to go through
		 * all paths since create_index_paths might have modified existing
		 * pathkey. Always safe to do transform since ordering of
		 * transformed_query_pathkey implements ordering of
		 * orig_query_pathkeys.
		 */
		foreach(lc_plan, rel->pathlist)
		{
			Path	   *path = lfirst(lc_plan);

			if (compare_pathkeys(path->pathkeys, transformed_query_pathkey) == PATHKEYS_EQUAL)
			{
				path->pathkeys = orig_query_pathkeys;
			}
		}
	}

}
