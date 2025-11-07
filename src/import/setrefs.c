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

#include "setrefs.h"

#include <nodes/makefuncs.h>
#include <optimizer/tlist.h>
#include <rewrite/rewriteDefine.h>

/* Methods for efficient rewrite of query expressions with subquery targets,
 * borrowed from PostgreSQL and simplified for our use
 * as we call these methods before planning is done.
 */

typedef struct
{
	ts_indexed_tlist *subplan_itlist;
	int newvarno;
	int rtoffset;
	bool match;
} replace_tlist_expr_context;

ts_indexed_tlist *
ts_build_tlist_index(List *tlist)
{
	ts_indexed_tlist *itlist;
	ts_tlist_vinfo *vinfo;
	ListCell *l;

	/* Create data structure with enough slots for all tlist entries */
	itlist = (ts_indexed_tlist *) palloc(offsetof(ts_indexed_tlist, vars) +
										 list_length(tlist) * sizeof(ts_tlist_vinfo));

	itlist->tlist = tlist;
	itlist->has_non_vars = false;

	/* Find the Vars and fill in the index array */
	vinfo = itlist->vars;
	foreach (l, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);
		if (tle->resjunk)
			continue;

		if (tle->expr && IsA(tle->expr, Var))
		{
			Var *var = (Var *) tle->expr;

			vinfo->varno = var->varno;
			vinfo->varattno = var->varattno;
			vinfo->resno = tle->resno;
			vinfo++;
		}
		else
			itlist->has_non_vars = true;
	}

	itlist->num_vars = (vinfo - itlist->vars);

	return itlist;
}

static Var *
search_indexed_tlist_for_var(Var *var, ts_indexed_tlist *itlist, int newvarno, int rtoffset)
{
	int varno = var->varno;
	AttrNumber varattno = var->varattno;
	ts_tlist_vinfo *vinfo;
	int i;

	vinfo = itlist->vars;
	i = itlist->num_vars;
	while (i-- > 0)
	{
		if (vinfo->varno == varno && vinfo->varattno == varattno)
		{
			/* Found a match */
			Var *newvar = makeVar(newvarno,
								  vinfo->resno,
								  var->vartype,
								  var->vartypmod,
								  var->varcollid,
								  var->varlevelsup);
			return newvar;
		}
		vinfo++;
	}
	return NULL; /* no match */
}

static Var *
search_indexed_tlist_for_non_var(Expr *node, ts_indexed_tlist *itlist, int newvarno)
{
	TargetEntry *tle;

	/*
	 * If it's a simple Const, replacing it with a Var is silly, even if there
	 * happens to be an identical Const below; a Var is more expensive to
	 * execute than a Const.  What's more, replacing it could confuse some
	 * places in the executor that expect to see simple Consts for, eg,
	 * dropped columns.
	 */
	if (IsA(node, Const))
		return NULL;

	tle = tlist_member(node, itlist->tlist);
	if (tle)
	{
		/* Found a matching subplan output expression */
		Var *newvar;

		newvar = makeVarFromTargetEntry(newvarno, tle);
		newvar->varnosyn = 0; /* wasn't ever a plain Var */
		newvar->varattnosyn = 0;
		return newvar;
	}
	return NULL; /* no match */
}

static Node *
replace_tlist_expr_mutator(Node *node, replace_tlist_expr_context *context)
{
	Var *newvar;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		newvar = search_indexed_tlist_for_var(var,
											  context->subplan_itlist,
											  context->newvarno,
											  context->rtoffset);
		if (!newvar)
		{
			context->match = false;
			return node;
		}
		return (Node *) newvar;
	}
	/* Try matching more complex expressions too, if tlist has any */
	if (context->subplan_itlist->has_non_vars)
	{
		newvar = search_indexed_tlist_for_non_var((Expr *) node,
												  context->subplan_itlist,
												  context->newvarno);
		if (newvar)
			return (Node *) newvar;
	}
	return expression_tree_mutator(node, replace_tlist_expr_mutator, context);
}

/* Will try to rewrite query expression with subquery targets,
 * returns NULL if can't fully rewrite the expression.
 */
Node *
ts_replace_tlist_expr(Node *node, ts_indexed_tlist *subplan_itlist, int newvarno, int rtoffset)
{
	replace_tlist_expr_context context;

	context.subplan_itlist = subplan_itlist;
	context.newvarno = newvarno;
	context.rtoffset = rtoffset;
	context.match = true;
	Node *result = replace_tlist_expr_mutator(node, &context);
	if (!context.match)
		return NULL;
	else
		return result;
}
