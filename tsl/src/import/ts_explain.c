/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

#include "ts_explain.h"

#include <commands/explain.h>
#include <nodes/makefuncs.h>
#include <utils/ruleutils.h>

/*
 * Show a generic expression
 */
static void
ts_show_expression(Node *node, const char *qlabel, PlanState *planstate, List *ancestors,
				   bool useprefix, ExplainState *es)
{
	List *context;
	char *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt, planstate->plan, ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
ts_show_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, bool useprefix,
			 ExplainState *es)
{
	Node *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	ts_show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
void
ts_show_scan_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors,
				  ExplainState *es)
{
	bool useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) || es->verbose);
	ts_show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}
