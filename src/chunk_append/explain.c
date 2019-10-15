/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_collation.h>
#include <commands/explain.h>
#include <nodes/execnodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/ruleutils.h>
#include <utils/typcache.h>

#include "chunk_append/exec.h"
#include "chunk_append/explain.h"
#include "compat.h"

static void show_sort_group_keys(ChunkAppendState *planstate, List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation,
								   bool nullsFirst);

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 * This callback is optional. Common data stored in the ScanState,
 * such as the target list and scan relation, will be shown even without
 * this callback, but the callback allows the display of additional,
 * private state.
 */
void
ts_chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ChunkAppendState *state = (ChunkAppendState *) node;

	if (state->sort_options != NIL)
		show_sort_group_keys(state, ancestors, es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Startup Exclusion", state->startup_exclusion, es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Runtime Exclusion", state->runtime_exclusion, es);

	if (state->startup_exclusion)
		ExplainPropertyIntegerCompat("Chunks excluded during startup",
									 NULL,
									 list_length(state->initial_subplans) -
										 list_length(node->custom_ps),
									 es);

	if (state->runtime_exclusion && state->runtime_number_loops > 0)
	{
		int avg_excluded = state->runtime_number_exclusions / state->runtime_number_loops;
		ExplainPropertyIntegerCompat("Chunks excluded during runtime", NULL, avg_excluded, es);
	}
}

/*
 * adjusted from postgresql explain.c
 * since we have to keep the state in custom_private our sort state
 * is in lists instead of arrays
 */
static void
show_sort_group_keys(ChunkAppendState *state, List *ancestors, ExplainState *es)
{
	Plan *plan = state->csstate.ss.ps.plan;
	List *context;
	List *result = NIL;
	StringInfoData sortkeybuf;
	bool useprefix;
	int keyno;
	int nkeys = list_length(linitial(state->sort_options));
	List *sort_indexes = linitial(state->sort_options);
	List *sort_ops = lsecond(state->sort_options);
	List *sort_collations = lthird(state->sort_options);
	List *sort_nulls = lfourth(state->sort_options);

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt, (Node *) state, ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber keyresno = list_nth_oid(sort_indexes, keyno);
		TargetEntry *target =
			get_tle_by_resno(castNode(CustomScan, plan)->custom_scan_tlist, keyresno);
		char *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context, useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sort_ops != NIL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   list_nth_oid(sort_ops, keyno),
								   list_nth_oid(sort_collations, keyno),
								   list_nth_oid(sort_nulls, keyno));
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ExplainPropertyList("Order", result, es);
}

/* copied verbatim from postgresql explain.c */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation,
					   bool nullsFirst)
{
	Oid sortcoltype = exprType(sortexpr);
	bool reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}
