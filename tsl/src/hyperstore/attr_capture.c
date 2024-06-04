/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_attribute.h>
#include <executor/executor.h>
#include <nodes/bitmapset.h>
#include <nodes/execnodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/ruleutils.h>

#include "arrow_tts.h"
#include "attr_capture.h"
#include <utils.h>

struct CaptureAttributesContext
{
	List *rtable;	   /* Range table for the statement */
	TupleDesc tupdesc; /* Tuple descriptor for the relation */
	Relation rel;	   /* Relation for the attributes */
	Bitmapset *atts;   /* Attributes referenced */
#ifdef TS_DEBUG
	List *deparse_cxt; /* Deparse context for debug printouts */
#endif
};

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static void
capture_var(Var *node, struct CaptureAttributesContext *context)
{
	TS_DEBUG_LOG("relid: %d, rd_id: %d, varno: %d, varattno: %d",
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
#ifdef TS_DEBUG
		char *exprstr = deparse_expression(qual, context->deparse_cxt, false, false);
		TS_DEBUG_LOG("qualifier %s", exprstr);
		pfree(exprstr);
#endif
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
#ifdef TS_DEBUG
		char *exprstr = deparse_expression((Node *) tle->expr, context->deparse_cxt, false, false);
		TS_DEBUG_LOG("target %s", exprstr);
		pfree(exprstr);
#endif

		if (!tle->resjunk)
			capture_expr((Node *) tle->expr, context);
	}
}

/*
 * Capture index attributes.
 *
 * The attributes referenced by an index is captured so that the hyperstore
 * TAM can later identify the index as a segmentby index (one that only
 * indexes compressed segments/tuples). When a segmentby index is identified
 * by hyperstore, it will "unwrap" the compressed tuples on-the-fly into
 * individual (uncompressed) tuples even though the index only references the
 * compressed segments.
 */
static void
capture_index_attributes(ScanState *state, Relation indexrel)
{
	Bitmapset *attrs = NULL;

	/* Index relation can be NULL in case of, e.g., EXPLAINs. It is OK to not
	 * capture index attributes in this case, because no scan is actually
	 * run. */
	if (NULL == indexrel)
		return;

	const int2vector *indkeys = &indexrel->rd_index->indkey;

	for (int i = 0; i < indkeys->dim1; i++)
	{
		const AttrNumber attno = indkeys->values[i];
		attrs = bms_add_member(attrs, attno);
	}

	arrow_slot_set_index_attrs(state->ss_ScanTupleSlot, attrs);
}

static void
collect_refs_and_targets(ScanState *state, struct CaptureAttributesContext *context)
{
	Assert(TTS_IS_ARROWTUPLE(state->ss_ScanTupleSlot));
	Assert(state->ss_currentRelation);
	context->tupdesc = state->ss_ScanTupleSlot->tts_tupleDescriptor;
	context->rel = state->ss_currentRelation;

	collect_references(state->ps.plan->qual, context);
	collect_targets(state->ps.plan->targetlist, context);

	/* For custom scan nodes, qualifiers are removed and stored into
	 * custom_exprs when setting up the custom scan node, so we include any
	 * qualifiers or expressions when extracting referenced attributes. For
	 * ColumnarScan nodes, these qualifiers are the vectorized qualifiers, but
	 * the concept applies generically to all custom scan nodes. */
	if (IsA(state->ps.plan, CustomScan))
	{
		CustomScan *cscan = castNode(CustomScan, state->ps.plan);
		if (cscan->custom_exprs)
			collect_references(cscan->custom_exprs, context);
	}

	arrow_slot_set_referenced_attrs(state->ss_ScanTupleSlot, context->atts);

	/* Just a precaution */
	context->tupdesc = 0;
	context->rel = 0;
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
			if (TTS_IS_ARROWTUPLE(state->ss_ScanTupleSlot))
			{
				const IndexScanState *istate = castNode(IndexScanState, planstate);
				capture_index_attributes(state, istate->iss_RelationDesc);
				collect_refs_and_targets(state, context);
			}
			break;
		case T_IndexOnlyScanState:
			if (TTS_IS_ARROWTUPLE(state->ss_ScanTupleSlot))
			{
				IndexOnlyScanState *istate = castNode(IndexOnlyScanState, planstate);
				capture_index_attributes(state, istate->ioss_RelationDesc);
				collect_refs_and_targets(state, context);
			}
			break;
		case T_CustomScanState:
		case T_SeqScanState:
		case T_BitmapHeapScanState:
			/* If this is an Arrow TTS, update the attributes that are referenced so that
			 * we do not decompress attributes that are not used. */
			if (TTS_IS_ARROWTUPLE(state->ss_ScanTupleSlot))
				collect_refs_and_targets(state, context);
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
#ifdef TS_DEBUG
	ListCell *cell;
#endif

	/* Call the standard executor start function to set up plan states. */
	standard_ExecutorStart(queryDesc, eflags);

	struct CaptureAttributesContext context = {
		.rtable = queryDesc->plannedstmt->rtable,
#ifdef TS_DEBUG
		.deparse_cxt =
			deparse_context_for_plan_tree(queryDesc->plannedstmt, queryDesc->plannedstmt->rtable),
#endif
		.atts = NULL
	};

#ifdef TS_DEBUG
	foreach (cell, queryDesc->plannedstmt->rtable)
	{
		RangeTblEntry *rte = lfirst(cell);
		TS_DEBUG_LOG("rtable #%d: %s (relid: %d)",
					 foreach_current_index(cell),
					 get_rel_name(rte->relid),
					 rte->relid);
	}
#endif

	capture_attributes(queryDesc->planstate, &context);
}

void
_attr_capture_init(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = capture_ExecutorStart;
}
