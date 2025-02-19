/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/sdir.h>
#include <access/skey.h>
#include <access/tableam.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <planner.h>
#include <planner/planner.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/typcache.h>

#include "columnar_scan.h"
#include "compression/compression.h"
#include "guc.h"
#include "hypercore/arrow_tts.h"
#include "hypercore/hypercore_handler.h"
#include "hypercore/vector_quals.h"
#include "import/ts_explain.h"

typedef struct SimpleProjInfo
{
	ProjectionInfo *pi;	 /* Original projection info for falling back to PG projection */
	int16 *projmap;		 /* Map of attribute numbers from scan tuple to projection tuple */
	int16 numprojattrs;	 /* Number of projected attributes */
	int16 maxprojattoff; /* Max attribute number in scan tuple that is part of
						  * projected tuple */
} SimpleProjInfo;

typedef struct ColumnarScanState
{
	CustomScanState css;
	VectorQualState vqstate;
	ExprState *segmentby_exprstate;
	ScanKey scankeys;
	int nscankeys;
	List *scankey_quals;
	List *quals_orig;
	List *vectorized_quals_orig;
	List *segmentby_quals;
	SimpleProjInfo sprojinfo;
} ColumnarScanState;

static bool
match_relvar(Expr *expr, Index relid)
{
	if (IsA(expr, Var))
	{
		Var *v = castNode(Var, expr);

		if ((Index) v->varno == relid)
			return true;
	}
	return false;
}

typedef struct QualProcessState
{
	const HypercoreInfo *hcinfo;
	Index relid;
	/*
	 * The original quals are split into scankey quals, vectorized and
	 * non-vectorized (regular) quals.
	 */
	List *scankey_quals;
	List *vectorized_quals;
	List *nonvectorized_quals;

	/*
	 * Further split nonvectorized quals into quals on segmentby columns and
	 * non-segmentby columns.
	 */
	List *segmentby_quals;
	List *nonsegmentby_quals;

	/* Scankeys created from scankey_quals */
	ScanKey scankeys;
	unsigned int scankeys_capacity;
	unsigned int nscankeys;

	/* Scratch area for processing */
	bool relvar_found;
} QualProcessState;

static bool
segmentby_qual_walker(Node *qual, QualProcessState *qpc)
{
	if (qual == NULL)
		return false;

	if (IsA(qual, Var) && (Index) castNode(Var, qual)->varno == qpc->relid)
	{
		const Var *v = castNode(Var, qual);

		if (AttrNumberIsForUserDefinedAttr(v->varattno))
		{
			const ColumnCompressionSettings *ccs =
				&qpc->hcinfo->columns[AttrNumberGetAttrOffset(v->varattno)];
			qpc->relvar_found = true;

			if (!ccs->is_segmentby)
				return true;
		}
	}

	return expression_tree_walker(qual, segmentby_qual_walker, qpc);
}

/*
 * Split "quals" into those that apply on segmentby columns and those that do
 * not.
 */
static void
process_segmentby_quals(QualProcessState *qpc, List *quals)
{
	ListCell *lc;

	qpc->relvar_found = false;

	foreach (lc, quals)
	{
		Expr *qual = lfirst(lc);

		if (!segmentby_qual_walker((Node *) qual, qpc) && qpc->relvar_found)
			qpc->segmentby_quals = lappend(qpc->segmentby_quals, qual);
		else
			qpc->nonsegmentby_quals = lappend(qpc->nonsegmentby_quals, qual);
	}
}

/*
 * Process OP-like expression.
 *
 * Returns true if the qual should be kept as a regular qual filter in the
 * scan, or false if it should not be kept.
 *
 * Note that only scankeys on segmentby columns can completely replace a
 * qual. Scankeys on other (compressed) columns will be transformed into
 * scankeys on metadata columns, and those filters are not 100% accurate so
 * the original qual must be kept.
 */
static bool
process_opexpr(QualProcessState *qpi, OpExpr *opexpr)
{
	Expr *leftop, *rightop, *expr = NULL;
	Var *relvar = NULL;
	Datum scanvalue = 0;
	bool argfound = false;
	Oid opno = opexpr->opno;
	Oid vartype_left = InvalidOid;
	Oid vartype_right = InvalidOid;
	Oid vartype = InvalidOid;

	/* OpExpr always has 1 or 2 arguments */
	Assert(is_opclause(opexpr) &&
		   (list_length(opexpr->args) == 1 || list_length(opexpr->args) == 2));

	if (opexpr->opresulttype != BOOLOID)
		return true;

	if (list_length(opexpr->args) != 2)
		return true;

	leftop = (Expr *) get_leftop(opexpr);
	rightop = (Expr *) get_rightop(opexpr);

	/* Strip any relabeling */
	if (IsA(leftop, RelabelType))
	{
		vartype_left = ((RelabelType *) leftop)->resulttype;
		leftop = ((RelabelType *) leftop)->arg;
	}
	if (IsA(rightop, RelabelType))
	{
		vartype_right = ((RelabelType *) rightop)->resulttype;
		rightop = ((RelabelType *) rightop)->arg;
	}

	/*
	 * Find a Var reference in either left or right operand. If an operand was
	 * relabeled, the vartype must be the original relabel result type instead
	 * of the Var's type.
	 */
	if (match_relvar(leftop, qpi->relid))
	{
		relvar = castNode(Var, leftop);
		expr = rightop;
		vartype = OidIsValid(vartype_left) ? vartype_left : relvar->vartype;
	}
	else if (match_relvar(rightop, qpi->relid))
	{
		relvar = castNode(Var, rightop);
		expr = leftop;
		vartype = OidIsValid(vartype_right) ? vartype_right : relvar->vartype;
		opno = get_commutator(opexpr->opno);
	}
	else
	{
		/* If neither right nor left argument is a variable, we
		 * don't use it as scan key */
		return true;
	}

	if (!OidIsValid(opno) || !op_strict(opno))
		return true;

	Assert(expr != NULL);

	if (IsA(expr, Const))
	{
		Const *c = castNode(Const, expr);
		scanvalue = c->constvalue;
		argfound = true;
	}

	const ColumnCompressionSettings *ccs =
		&qpi->hcinfo->columns[AttrNumberGetAttrOffset(relvar->varattno)];

	/* Add a scankey if this is a segmentby column or the column
	 * has min/max metadata */
	if (argfound && (ccs->is_segmentby || hypercore_column_has_minmax(ccs)))
	{
		TypeCacheEntry *tce = lookup_type_cache(vartype, TYPECACHE_BTREE_OPFAMILY);
		int op_strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

		if (op_strategy != InvalidStrategy)
		{
			Oid op_lefttype;
			Oid op_righttype;

			get_op_opfamily_properties(opno,
									   tce->btree_opf,
									   false,
									   &op_strategy,
									   &op_lefttype,
									   &op_righttype);

			Assert(relvar != NULL);

			if (qpi->scankeys != NULL)
			{
				ScanKeyEntryInitialize(&qpi->scankeys[qpi->nscankeys++],
									   0,
									   relvar->varattno,
									   op_strategy,
									   op_righttype,
									   opexpr->inputcollid,
									   opexpr->opfuncid,
									   scanvalue);
			}

			qpi->scankey_quals = lappend(qpi->scankey_quals, opexpr);

			return !ccs->is_segmentby;
		}
	}

	return true;
}

/*
 * Utility function to extract quals that can be used as scankeys.

 * Thus, this function is designed to be called over two passes: one at plan
 * time to split the quals into scankey quals and remaining quals, and one at
 * execution time to populate a scankey array with the scankey quals found in
 * the first pass.
 *
 * The scankey quals collected in pass 1 is used for EXPLAIN.
 *
 * Returns the remaining quals that cannot be made into scankeys. The scankey
 * quals are returned in the QualProcessInfo.
 */
static List *
process_scan_key_quals(QualProcessState *qpi, const List *quals)
{
	ListCell *lc;
	List *remaining_quals = NIL;

	foreach (lc, quals)
	{
		Expr *qual = lfirst(lc);
		bool keep_qual = true;

		if (!contain_volatile_functions((Node *) qual))
		{
			switch (nodeTag(qual))
			{
				case T_OpExpr:
					keep_qual = process_opexpr(qpi, castNode(OpExpr, qual));
					break;
				case T_ScalarArrayOpExpr:
					/*
					 * Currently cannot pushing down "foo IN (1, 3)". The
					 * expression can be transformed into a scankey, but it
					 * only works for index scans.
					 */
					break;
				case T_NullTest:
					/*
					 * "foo IS NULL"
					 *
					 * Not pushed down currently.
					 */
					break;
				default:
					break;
			}
		}

		if (keep_qual)
			remaining_quals = lappend(remaining_quals, qual);
	}

	return remaining_quals;
}

/*
 * Classify quals into the following:
 *
 * 1. Scankey quals: quals that can be pushed down to the TAM as
 *    scankeys. This includes most quals on segmentby columns, and orderby
 *    columns that have min/max metadata.
 *
 * 2. Vectorized quals: quals that can be executed as vectorized filters.
 *
 * 3. Non-vectorized quals: regular quals that should be executed in the
 *    columnar scan node.
 *
 * Note that category (3) might include quals from category (1) (scankeys)
 * because some scankeys that apply to compressed columns that have min/max
 * metadata might not exclude all data, so the regular quals need to be
 * applied after.
 */
static void
classify_quals(QualProcessState *qpi, const VectorQualInfo *vqinfo, List *quals)
{
	ListCell *lc;
	List *nonscankey_quals = quals;

	Assert(qpi->hcinfo && qpi->relid > 0);

	if (ts_guc_enable_hypercore_scankey_pushdown)
		nonscankey_quals = process_scan_key_quals(qpi, quals);

	foreach (lc, nonscankey_quals)
	{
		Node *source_qual = lfirst(lc);
		Node *vectorized_qual = vector_qual_make(source_qual, vqinfo);

		if (vectorized_qual)
		{
			TS_DEBUG_LOG("qual identified as vectorizable: %s", nodeToString(vectorized_qual));
			qpi->vectorized_quals = lappend(qpi->vectorized_quals, vectorized_qual);
		}
		else
		{
			TS_DEBUG_LOG("qual identified as non-vectorized qual: %s", nodeToString(source_qual));
			qpi->nonvectorized_quals = lappend(qpi->nonvectorized_quals, source_qual);
		}
	}

	process_segmentby_quals(qpi, qpi->nonvectorized_quals);
}

static ScanKey
create_scankeys_from_quals(const HypercoreInfo *hcinfo, Index relid, const List *quals)
{
	unsigned capacity = list_length(quals);
	QualProcessState qpi = {
		.hcinfo = hcinfo,
		.relid = relid,
		.scankeys = palloc0(sizeof(ScanKeyData) * capacity),
		.scankeys_capacity = capacity,
	};

	process_scan_key_quals(&qpi, quals);

	return qpi.scankeys;
}

static pg_attribute_always_inline TupleTableSlot *
exec_projection(SimpleProjInfo *spi)
{
	TupleTableSlot *result_slot = spi->pi->pi_state.resultslot;

	/* Check for special case when projecting zero scan attributes. This could
	 * be, e.g., count(*)-type queries. */
	if (spi->numprojattrs == 0)
	{
		if (TTS_EMPTY(result_slot))
			return ExecStoreVirtualTuple(result_slot);

		return result_slot;
	}

	/* If there's a projection map, it is possible to do a simple projection,
	 * i.e., return a subset of the scan attributes (or a different order). */
	if (spi->projmap != NULL)
	{
		TupleTableSlot *slot = spi->pi->pi_exprContext->ecxt_scantuple;

		slot_getsomeattrs(slot, AttrOffsetGetAttrNumber(spi->maxprojattoff));

		for (int i = 0; i < spi->numprojattrs; i++)
		{
			result_slot->tts_values[i] = slot->tts_values[spi->projmap[i]];
			result_slot->tts_isnull[i] = slot->tts_isnull[spi->projmap[i]];
		}

		ExecClearTuple(result_slot);
		return ExecStoreVirtualTuple(result_slot);
	}

	/* Fall back to regular projection */
	ResetExprContext(spi->pi->pi_exprContext);

	return ExecProject(spi->pi);
}

static pg_attribute_always_inline bool
getnextslot(TableScanDesc scandesc, ScanDirection direction, TupleTableSlot *slot)
{
	if (arrow_slot_try_getnext(slot, direction))
	{
		slot->tts_tableOid = RelationGetRelid(scandesc->rs_rd);
		return true;
	}

	return table_scan_getnextslot(scandesc, direction, slot);
}

static bool
should_project(const CustomScanState *state)
{
#if PG15_GE
	const CustomScan *scan = castNode(CustomScan, state->ss.ps.plan);
	return scan->flags & CUSTOMPATH_SUPPORT_PROJECTION;
#else
	return false;
#endif
}

static inline bool
should_check_segmentby_qual(ScanDirection direction, const TupleTableSlot *slot)
{
	if (likely(direction == ForwardScanDirection))
		return arrow_slot_is_first(slot);

	Assert(direction == BackwardScanDirection);
	return arrow_slot_is_last(slot);
}

/*
 * Filter tuple by segmentby quals.
 *
 * Quals on segmentby columns require no decompression and can filter all
 * values in the arrow slot in one go since the value of a segmentby column is
 * the same for the entire arrow array.
 *
 * Similarly, if the arrow slot passes the filter, the segmentby quals don't
 * need to be checked again until the next array is processed.
 */
static inline bool
ExecSegmentbyQual(ExprState *qual, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	ScanDirection direction = econtext->ecxt_estate->es_direction;

	Assert(direction == ForwardScanDirection || direction == BackwardScanDirection);
	Assert(TTS_IS_ARROWTUPLE(slot));

	if (qual == NULL || !should_check_segmentby_qual(direction, slot))
		return true;

	Assert(!arrow_slot_is_consumed(slot));
	return ExecQual(qual, econtext);
}

static TupleTableSlot *
columnar_scan_exec(CustomScanState *state)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;
	TableScanDesc scandesc;
	EState *estate;
	ExprContext *econtext;
	ExprState *qual;
	ScanDirection direction;
	TupleTableSlot *slot;
	bool has_vecquals = cstate->vqstate.vectorized_quals_constified != NIL;
	/*
	 * The VectorAgg node could have requested no projection by unsetting the
	 * "projection support flag", so only project if the flag is still set.
	 */
	ProjectionInfo *projinfo = should_project(state) ? state->ss.ps.ps_ProjInfo : NULL;

	scandesc = state->ss.ss_currentScanDesc;
	estate = state->ss.ps.state;
	econtext = state->ss.ps.ps_ExprContext;
	qual = state->ss.ps.qual;
	direction = estate->es_direction;
	slot = state->ss.ss_ScanTupleSlot;

	TS_DEBUG_LOG("relation: %s", RelationGetRelationName(state->ss.ss_currentRelation));

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(state->ss.ss_currentRelation,
								   estate->es_snapshot,
								   cstate->nscankeys,
								   cstate->scankeys);
		state->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * If no quals to check, do the fast path and just return the raw scan
	 * tuple or a projected one.
	 */
	if (!qual && !has_vecquals && !cstate->segmentby_exprstate)
	{
		bool gottuple = getnextslot(scandesc, direction, slot);

		if (!projinfo)
		{
			return gottuple ? slot : NULL;
		}
		else
		{
			if (!gottuple)
			{
				/* Nothing to return, but be careful to use the projection result
				 * slot so it has correct tupleDesc. */
				return ExecClearTuple(projinfo->pi_state.resultslot);
			}
			else
			{
				econtext->ecxt_scantuple = slot;
				return exec_projection(&cstate->sprojinfo);
			}
		}
	}

	ResetExprContext(econtext);

	/*
	 * Scan tuples and apply vectorized filters, followed by any remaining
	 * (non-vectorized) qual filters and projection.
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		slot = state->ss.ss_ScanTupleSlot;

		if (!getnextslot(scandesc, direction, slot))
		{
			/* Nothing to return, but be careful to use the projection result
			 * slot so it has correct tupleDesc. */
			if (projinfo)
				return ExecClearTuple(projinfo->pi_state.resultslot);
			else
				return NULL;
		}

		econtext->ecxt_scantuple = slot;

		if (likely(TTS_IS_ARROWTUPLE(slot)))
		{
			/*
			 * Filter on segmentby quals first. This is important for performance
			 * since segmentby quals don't need decompression and can filter all
			 * values in an arrow slot in one go.
			 */
			if (!ExecSegmentbyQual(cstate->segmentby_exprstate, econtext))
			{
				/* The slot didn't pass filters so read the next slot */
				const uint16 nrows = arrow_slot_total_row_count(slot);
				arrow_slot_mark_consumed(slot);
				InstrCountFiltered1(state, nrows);
				ResetExprContext(econtext);
				continue;
			}

			const uint16 nfiltered = ExecVectorQual(&cstate->vqstate, econtext);

			if (nfiltered > 0)
			{
				const uint16 total_nrows = arrow_slot_total_row_count(slot);

				TS_DEBUG_LOG("vectorized filtering of %u rows", nfiltered);

				/* Skip ahead with the amount filtered */
				if (direction == ForwardScanDirection)
					ExecIncrArrowTuple(slot, nfiltered);
				else
				{
					Assert(direction == BackwardScanDirection);
					ExecDecrArrowTuple(slot, nfiltered);
				}

				InstrCountFiltered1(state, nfiltered);

				if (nfiltered == total_nrows && total_nrows > 1)
				{
					/* A complete segment was filtered */
					Assert(arrow_slot_is_consumed(slot));
					InstrCountTuples2(state, 1);
				}

				/* If the whole segment was consumed, read next segment */
				if (arrow_slot_is_consumed(slot))
					continue;
			}
		}

		/* A row passed vectorized filters. Check remaining non-vectorized
		 * quals, if any, and do projection. */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			if (projinfo)
				return exec_projection(&cstate->sprojinfo);

			return slot;
		}
		else
			InstrCountFiltered1(state, 1);

		ResetExprContext(econtext);
	}
}

/*
 * Try to create simple projection state.
 *
 * A simple projection is one where it is possible to just copy a subset of
 * attribute values to the projection slot, or the attributes are in a
 * different order. No reason to fire up PostgreSQL's expression execution
 * engine for those simple cases.
 *
 * If simple projection is not possible (e.g., there are some non-Var
 * attributes), then fall back to PostgreSQL projection.
 */
static void
create_simple_projection_state_if_possible(ColumnarScanState *cstate)
{
	ScanState *ss = &cstate->css.ss;
	ProjectionInfo *projinfo = ss->ps.ps_ProjInfo;
	const TupleDesc projdesc = ss->ps.ps_ResultTupleDesc;
	const List *targetlist = ss->ps.plan->targetlist;
	SimpleProjInfo *sprojinfo = &cstate->sprojinfo;
	int16 *projmap;
	ListCell *lc;
	int i = 0;

	/* Should not try to create simple projection if there is no projection */
	Assert(projinfo);
	Assert(list_length(targetlist) == projdesc->natts);

	sprojinfo->numprojattrs = list_length(targetlist);
	sprojinfo->maxprojattoff = -1;
	sprojinfo->pi = projinfo;

	/* If there's nothing to projecct, just return */
	if (sprojinfo->numprojattrs == 0)
		return;

	projmap = palloc(sizeof(int16) * projdesc->natts);

	/* Check for attributes referenced in targetlist and create the simple
	 * projection map. */
	foreach (lc, targetlist)
	{
		const TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Expr *expr = tle->expr;

		switch (expr->type)
		{
			case T_Var:
			{
				const Var *var = castNode(Var, expr);
				AttrNumber attno = var->varattno;
				int16 attoff;

				if (!AttrNumberIsForUserDefinedAttr(attno))
				{
					/* Special Var, so assume simple projection is not possible */
					pfree(projmap);
					return;
				}

				attoff = AttrNumberGetAttrOffset(attno);
				Assert((Index) var->varno == ((Scan *) ss->ps.plan)->scanrelid);
				projmap[i++] = attoff;

				if (attoff > sprojinfo->maxprojattoff)
					sprojinfo->maxprojattoff = attoff;

				break;
			}
			default:
				/* Targetlist has a non-Var node, so simple projection not possible */
				pfree(projmap);
				return;
		}
	}

	Assert(i == sprojinfo->numprojattrs);
	sprojinfo->projmap = projmap;
}

static void
columnar_scan_begin(CustomScanState *state, EState *estate, int eflags)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;

#if PG16_LT
	/* Since PG16, one can specify state->slotOps to initialize a CustomScan
	 * with a custom scan slot. But pre-PG16, the CustomScan state always
	 * created a scan slot of type TTSOpsVirtual, even if one sets
	 * scan->scanrelid to a valid index to indicate scan of a base relation.
	 * To ensure the base relation's scan slot type is used, we recreate the
	 * scan slot here with the slot type used by the underlying base
	 * relation. It is not necessary (or possible) to drop the existing slot
	 * since it is registered in the tuple table and will be released when the
	 * executor finishes. */
	Relation rel = state->ss.ss_currentRelation;
	ExecInitScanTupleSlot(estate,
						  &state->ss,
						  RelationGetDescr(rel),
						  table_slot_callbacks(state->ss.ss_currentRelation));

	/* Must reinitialize projection for the new slot type as well, including
	 * ExecQual state for the new slot. */
	ExecInitResultTypeTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);
	state->ss.ps.qual = ExecInitQual(state->ss.ps.plan->qual, (PlanState *) state);
#endif
	List *vectorized_quals_constified = NIL;

	if (cstate->nscankeys > 0)
	{
		const HypercoreInfo *hsinfo = RelationGetHypercoreInfo(state->ss.ss_currentRelation);
		Scan *scan = (Scan *) state->ss.ps.plan;
		cstate->scankeys =
			create_scankeys_from_quals(hsinfo, scan->scanrelid, cstate->scankey_quals);
	}

	PlannerGlobal glob = {
		.boundParams = state->ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};
	ListCell *lc;
	foreach (lc, cstate->vectorized_quals_orig)
	{
		Node *constified = estimate_expression_value(&root, (Node *) lfirst(lc));
		vectorized_quals_constified = lappend(vectorized_quals_constified, constified);
	}

	/*
	 * Initialize the state to compute vectorized quals.
	 */
	vector_qual_state_init(&cstate->vqstate,
						   vectorized_quals_constified,
						   state->ss.ss_ScanTupleSlot);

	/* If the node is supposed to project, then try to make it a simple
	 * projection. If not possible, it will fall back to standard PostgreSQL
	 * projection. */
	if (cstate->css.ss.ps.ps_ProjInfo)
		create_simple_projection_state_if_possible(cstate);

	cstate->segmentby_exprstate = ExecInitQual(cstate->segmentby_quals, (PlanState *) state);

	/*
	 * After having initialized ExprState for processing regular and segmentby
	 * quals separately, add the segmentby quals list to the original quals in
	 * order for them to show up together in EXPLAINs.
	 */
	if (cstate->segmentby_quals != NIL)
		state->ss.ps.plan->qual = list_concat(state->ss.ps.plan->qual, cstate->segmentby_quals);
}

static void
columnar_scan_end(CustomScanState *state)
{
	TableScanDesc scandesc = state->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext. Not needed for PG17.
	 */
#if PG17_LT
	ExecFreeExprContext(&state->ss.ps);
#endif

	/*
	 * clean out the tuple table
	 */
	if (state->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(state->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(state->ss.ss_ScanTupleSlot);

	/*
	 * close the scan
	 */
	if (scandesc)
		table_endscan(scandesc);
}

static void
columnar_scan_rescan(CustomScanState *state)
{
	TableScanDesc scandesc = state->ss.ss_currentScanDesc;

	if (NULL != scandesc)
		table_rescan(scandesc, /* scan desc */
					 NULL);	   /* new scan keys */

	ExecScanReScan((ScanState *) state);
}

static void
columnar_scan_explain(CustomScanState *state, List *ancestors, ExplainState *es)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;

	if (cstate->scankey_quals != NIL)
		ts_show_scan_qual(cstate->scankey_quals, "Scankey", &state->ss.ps, ancestors, es);

	ts_show_scan_qual(cstate->vectorized_quals_orig,
					  "Vectorized Filter",
					  &state->ss.ps,
					  ancestors,
					  es);

	if (!state->ss.ps.plan->qual && cstate->vectorized_quals_orig)
	{
		/*
		 * The normal explain won't show this if there are no normal quals but
		 * only the vectorized ones.
		 */
		ts_show_instrumentation_count("Rows Removed by Filter", 1, &state->ss.ps, es);
	}

	if (es->analyze && es->verbose &&
		(state->ss.ps.instrument->ntuples2 > 0 || es->format != EXPLAIN_FORMAT_TEXT))
	{
		ExplainPropertyFloat("Batches Removed by Filter",
							 NULL,
							 state->ss.ps.instrument->ntuples2,
							 0,
							 es);
	}
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/*
 * Local version of table_beginscan_parallel() to pass on scankeys to the
 * table access method callback.
 *
 * The PostgreSQL version of this function does _not_ pass on the given
 * scankeys (key parameter) to the underlying table access method's
 * scan_begin() (last call in the function). The local version imported here
 * fixes that.
 */
static TableScanDesc
ts_table_beginscan_parallel(Relation relation, ParallelTableScanDesc pscan, int nkeys,
							struct ScanKeyData *key)
{
	Snapshot snapshot;
	uint32 flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

	Assert(RelationGetRelid(relation) == pscan->phs_relid);

	if (!pscan->phs_snapshot_any)
	{
		/* Snapshot was serialized -- restore it */
		snapshot = RestoreSnapshot((char *) pscan + pscan->phs_snapshot_off);
		RegisterSnapshot(snapshot);
		flags |= SO_TEMP_SNAPSHOT;
	}
	else
	{
		/* SnapshotAny passed by caller (not serialized) */
		snapshot = SnapshotAny;
	}

	return relation->rd_tableam->scan_begin(relation, snapshot, nkeys, key, pscan, flags);
}

static Size
columnar_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	EState *estate = node->ss.ps.state;
	return table_parallelscan_estimate(node->ss.ss_currentRelation, estate->es_snapshot);
}

static void
columnar_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *arg)
{
	ColumnarScanState *cstate = (ColumnarScanState *) node;
	EState *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	table_parallelscan_initialize(node->ss.ss_currentRelation, pscan, estate->es_snapshot);
	node->ss.ss_currentScanDesc = ts_table_beginscan_parallel(node->ss.ss_currentRelation,
															  pscan,
															  cstate->nscankeys,
															  cstate->scankeys);
}

static void
columnar_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *arg)
{
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

static void
columnar_scan_initialize_worker(CustomScanState *node, shm_toc *toc, void *arg)
{
	ColumnarScanState *cstate = (ColumnarScanState *) node;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	node->ss.ss_currentScanDesc = ts_table_beginscan_parallel(node->ss.ss_currentRelation,
															  pscan,
															  cstate->nscankeys,
															  cstate->scankeys);
}

static CustomExecMethods columnar_scan_state_methods = {
	.CustomName = "ColumnarScan",
	.BeginCustomScan = columnar_scan_begin,
	.ExecCustomScan = columnar_scan_exec,
	.EndCustomScan = columnar_scan_end,
	.ReScanCustomScan = columnar_scan_rescan,
	.ExplainCustomScan = columnar_scan_explain,
	.EstimateDSMCustomScan = columnar_scan_estimate_dsm,
	.InitializeDSMCustomScan = columnar_scan_initialize_dsm,
	.ReInitializeDSMCustomScan = columnar_scan_reinitialize_dsm,
	.InitializeWorkerCustomScan = columnar_scan_initialize_worker,
};

static Node *
columnar_scan_state_create(CustomScan *cscan)
{
	ColumnarScanState *cstate;

	cstate = (ColumnarScanState *) newNode(sizeof(ColumnarScanState), T_CustomScanState);
	cstate->css.methods = &columnar_scan_state_methods;
	cstate->vectorized_quals_orig = linitial(cscan->custom_exprs);
	cstate->scankey_quals = lsecond(cscan->custom_exprs);
	cstate->segmentby_quals = lthird(cscan->custom_exprs);
	cstate->nscankeys = list_length(cstate->scankey_quals);
	cstate->scankeys = NULL;
#if PG16_GE
	cstate->css.slotOps = &TTSOpsArrowTuple;
#endif
	cstate->quals_orig = list_concat_copy(cstate->vectorized_quals_orig, cscan->scan.plan.qual);

	return (Node *) cstate;
}

static CustomScanMethods columnar_scan_plan_methods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = columnar_scan_state_create,
};

bool
is_columnar_scan(const Plan *plan)
{
	return IsA(plan, CustomScan) &&
		   ((const CustomScan *) plan)->methods == &columnar_scan_plan_methods;
}

typedef struct VectorQualInfoHypercore
{
	VectorQualInfo vqinfo;
	const HypercoreInfo *hcinfo;
} VectorQualInfoHypercore;

static bool *
columnar_scan_build_vector_attrs(const ColumnCompressionSettings *columns, int numcolumns)
{
	bool *vector_attrs = palloc0(sizeof(bool) * (numcolumns + 1));

	for (int i = 0; i < numcolumns; i++)
	{
		const ColumnCompressionSettings *column = &columns[i];
		AttrNumber attnum = AttrOffsetGetAttrNumber(i);

		Assert(column->attnum == attnum || column->attnum == InvalidAttrNumber);
		vector_attrs[attnum] =
			(!column->is_segmentby && column->attnum != InvalidAttrNumber &&
			 tsl_get_decompress_all_function(compression_get_default_algorithm(column->typid),
											 column->typid) != NULL);
	}
	return vector_attrs;
}

static Plan *
columnar_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						  List *scan_clauses, List *custom_plans)
{
	CustomScan *columnar_scan_plan = makeNode(CustomScan);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	Relation relation = RelationIdGetRelation(rte->relid);
	HypercoreInfo *hcinfo = RelationGetHypercoreInfo(relation);
	QualProcessState qpi = {
		.hcinfo = hcinfo,
		.relid = rel->relid,
	};
	VectorQualInfoHypercore vqih = {
		.vqinfo = {
			.rti = rel->relid,
			.vector_attrs = columnar_scan_build_vector_attrs(hcinfo->columns, hcinfo->num_columns),
		},
		.hcinfo = hcinfo,
	};
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	columnar_scan_plan->flags = best_path->flags;
	columnar_scan_plan->methods = &columnar_scan_plan_methods;
	columnar_scan_plan->scan.scanrelid = rel->relid;

	/* output target list */
	columnar_scan_plan->scan.plan.targetlist = tlist;

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	classify_quals(&qpi, &vqih.vqinfo, scan_clauses);

	columnar_scan_plan->scan.plan.qual = qpi.nonsegmentby_quals;
	columnar_scan_plan->custom_exprs =
		list_make3(qpi.vectorized_quals, qpi.scankey_quals, qpi.segmentby_quals);

	RelationClose(relation);

	return &columnar_scan_plan->scan.plan;
}

static CustomPathMethods columnar_scan_path_methods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = columnar_scan_plan_create,
};

static void
cost_columnar_scan(Path *path, PlannerInfo *root, RelOptInfo *rel)
{
	cost_seqscan(path, root, rel, path->param_info);

	/* Just make it a bit cheaper than seqscan for now */
	path->startup_cost *= 0.9;
	path->total_cost *= 0.9;
}

ColumnarScanPath *
columnar_scan_path_create(PlannerInfo *root, RelOptInfo *rel, Relids required_outer,
						  int parallel_workers)
{
	ColumnarScanPath *cspath;
	Path *path;

	cspath = (ColumnarScanPath *) newNode(sizeof(ColumnarScanPath), T_CustomPath);
	path = &cspath->custom_path.path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;
	path->param_info = get_baserel_parampathinfo(root, rel, required_outer);
	path->parallel_aware = (parallel_workers > 0);
	path->parallel_safe = rel->consider_parallel;
	path->parallel_workers = parallel_workers;
	path->pathkeys = NIL; /* currently has unordered result, but if pathkeys
						   * match the orderby,segmentby settings we could do
						   * ordering */

	cspath->custom_path.flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
#if PG15_GE
	cspath->custom_path.flags |= CUSTOMPATH_SUPPORT_PROJECTION;
#endif
	cspath->custom_path.methods = &columnar_scan_path_methods;

	cost_columnar_scan(path, root, rel);

	return cspath;
}

void
columnar_scan_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	ColumnarScanPath *cspath;
	Relids required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;
	cspath = columnar_scan_path_create(root, rel, required_outer, 0);
	add_path(rel, &cspath->custom_path.path);

	if (rel->consider_parallel && required_outer == NULL)
	{
		int parallel_workers;

		parallel_workers =
			compute_parallel_worker(rel, rel->pages, -1, max_parallel_workers_per_gather);

		/* If any limit was set to zero, the user doesn't want a parallel scan. */
		if (parallel_workers > 0)
		{
			/* Add an unordered partial path based on a parallel sequential scan. */
			cspath = columnar_scan_path_create(root, rel, required_outer, parallel_workers);
			add_partial_path(rel, &cspath->custom_path.path);
		}
	}
}

void
_columnar_scan_init(void)
{
	TryRegisterCustomScanMethods(&columnar_scan_plan_methods);
}
