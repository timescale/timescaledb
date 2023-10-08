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

#include "ts_explain.h"

#include <commands/explain.h>
#include <nodes/makefuncs.h>
#include <utils/ruleutils.h>

#include "compat/compat.h"

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
#if PG13_LT
	context = set_deparse_context_planstate(es->deparse_cxt, (Node *) planstate, ancestors);
#else
	context = set_deparse_context_plan(es->deparse_cxt, planstate->plan, ancestors);
#endif

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

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
void
ts_show_instrumentation_count(const char *qlabel, int which, PlanState *planstate, ExplainState *es)
{
	double nfiltered;
	double nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (nloops > 0)
			ExplainPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
		else
			ExplainPropertyFloat(qlabel, NULL, 0.0, 0, es);
	}
}
