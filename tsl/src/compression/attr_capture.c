/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <executor/executor.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>

#include "attr_capture.h"
#include "compression/arrow_tts.h"

struct CaptureAttributesContext
{
	List *rtable;	   /* Range table for the statement */
	TupleDesc tupdesc; /* Tuple descriptor for the relation */
	Relation rel;	   /* Relation for the attributes */
	Bitmapset *atts;   /* Attributes referenced */
};

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static void
capture_var(Var *node, struct CaptureAttributesContext *context)
{
	elog(DEBUG2,
		 "%s - relid: %d, rd_id: %d, varno: %d, varattno: %d",
		 __func__,
		 rt_fetch(node->varno, context->rtable)->relid,
		 context->rel->rd_id,
		 node->varno,
		 node->varattno);

	/* If the relid does not match the relid of the relation, we exit early */
	if (rt_fetch(node->varno, context->rtable)->relid != context->rel->rd_id ||
		node->varlevelsup != 0)
		return;

	if (node->varattno > 0)
		context->atts = bms_add_member(context->atts, node->varattno);
	if (node->varattno == 0)
		context->atts = bms_add_range(context->atts, 1, context->tupdesc->natts);
}

/* Similar to pull_varattnos, but also handles varattno == 0 and uses the
 * relation OID rather than the range table index to filter out attributes. */
static bool
capture_expr(Node *node, struct CaptureAttributesContext *context)
{
	if (!node)
		return false;

	if (IsA(node, Var))
	{
		capture_var(castNode(Var, node), context);
		return false;
	}

	return expression_tree_walker(node, capture_expr, context);
}

static void
collect_references(List *clauses, struct CaptureAttributesContext *context)
{
	ListCell *cell;
	foreach (cell, clauses)
	{
		Node *qual = lfirst(cell);
		elog(DEBUG2, "%s - qual: %s", __func__, nodeToString(qual));
		capture_expr(qual, context);
	}
}

static void
collect_targets(List *targetlist, struct CaptureAttributesContext *context)
{
	ListCell *cell;
	foreach (cell, targetlist)
	{
		TargetEntry *tle = lfirst(cell);
		elog(DEBUG2, "%s - expr: %s", __func__, nodeToString(tle->expr));
		if (!tle->resjunk)
			capture_expr((Node *) tle->expr, context);
	}
}

static bool
capture_attributes(PlanState *planstate, void *ptr)
{
	struct CaptureAttributesContext *context = ptr;
	ScanState *state = (ScanState *) planstate;

	if (!planstate)
		return false;

	switch (nodeTag(planstate))
	{
		/* There are some scan states missing here */
		case T_IndexScanState:
		case T_CustomScanState:
		case T_SeqScanState:
		case T_BitmapHeapScanState:
			/* If this is an Arrow TTS, update the attributes that are referenced so that
			 * we do not decompress attributes that are not used. */
			if (TTS_IS_ARROWTUPLE(state->ss_ScanTupleSlot))
			{
				/* If this is a baserel, ss_currentRelation should not be null. */
				Assert(state->ss_currentRelation);
				context->tupdesc = state->ss_ScanTupleSlot->tts_tupleDescriptor;
				context->rel = state->ss_currentRelation;

				collect_references(state->ps.plan->qual, context);
				collect_targets(state->ps.plan->targetlist, context);
				arrow_slot_set_referenced_attrs(state->ss_ScanTupleSlot, context->atts);

				/* Just a precaution */
				context->tupdesc = 0;
				context->rel = 0;
			}
			break;

		default:
			/* Do nothing */
			break;
	}

	/* It seems the states above does not have left and right trees, but we
	 * recurse anyway. */
	return planstate_tree_walker(planstate, capture_attributes, context);
}

/*
 * Using mixed snake_case and CamelCase to follow convention of naming hooks
 * and standard functions.
 */
static void
capture_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	ListCell *cell;

	/* Call the standard executor start function to set up plan states. */
	standard_ExecutorStart(queryDesc, eflags);

	struct CaptureAttributesContext context = { .rtable = queryDesc->plannedstmt->rtable,
												.atts = NULL };

	foreach (cell, queryDesc->plannedstmt->rtable)
	{
		RangeTblEntry *rte = lfirst(cell);
		elog(DEBUG2,
			 "%s - rtable #%d: %s (relid: %d)",
			 __func__,
			 foreach_current_index(cell),
			 get_rel_name(rte->relid),
			 rte->relid);
	}

	capture_attributes(queryDesc->planstate, &context);
}

void
_attr_capture_init(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = capture_ExecutorStart;
}
