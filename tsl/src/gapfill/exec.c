/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#include <postgres.h>
#include <access/attnum.h>
#include <catalog/pg_type.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/primnodes.h>
#include <optimizer/var.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/typcache.h>

#include "compat.h"
#include "gapfill/gapfill.h"
#include "gapfill/locf.h"
#include "gapfill/interpolate.h"
#include "gapfill/exec.h"

typedef union GapFillColumnStateUnion
{
	GapFillColumnState *base;
	GapFillGroupColumnState *group;
	GapFillInterpolateColumnState *interpolate;
	GapFillLocfColumnState *locf;
} GapFillColumnStateUnion;

#define foreach_column(column,index,state) \
  for((index)=0,(column)=(state)->columns[index];(index)<(state)->ncolumns;(index)++,(column)=(state)->columns[index])

static void gapfill_begin(CustomScanState *node, EState *estate, int eflags);
static void gapfill_end(CustomScanState *node);
static void gapfill_rescan(CustomScanState *node);
static TupleTableSlot *gapfill_exec(CustomScanState *node);

static void gapfill_state_reset_group(GapFillState *state, TupleTableSlot *slot);
static TupleTableSlot *gapfill_state_gaptuple_create(GapFillState *state, int64 time);
static bool gapfill_state_is_new_group(GapFillState *state, TupleTableSlot *slot);
static void gapfill_state_set_next(GapFillState *state, TupleTableSlot *subslot);
static TupleTableSlot *gapfill_state_return_subplan_slot(GapFillState *state);
static TupleTableSlot *gapfill_fetch_next_tuple(GapFillState *state);
static TupleTableSlot *fetch_subplan_tuple(CustomScanState *node);
static void gapfill_state_initialize_columns(GapFillState *state);
static GapFillColumnState *gapfill_column_state_create(GapFillColumnType ctype, Oid typeid);
static bool gapfill_is_group_column(GapFillState *state, TargetEntry *tle);
static Node *gapfill_aggref_mutator(Node *node, void *context);

static CustomExecMethods gapfill_state_methods = {
	.BeginCustomScan = gapfill_begin,
	.ExecCustomScan = gapfill_exec,
	.EndCustomScan = gapfill_end,
	.ReScanCustomScan = gapfill_rescan,
};

/*
 * convert Datum to int64 according to type
 * internally we store all times as int64 in the
 * same format postgres does
 */
int64
gapfill_datum_get_internal(Datum value, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return DatumGetInt16(value);
		case DATEOID:
		case INT4OID:
			return DatumGetInt32(value);
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case INT8OID:
			return DatumGetInt64(value);
		default:

			/*
			 * should never happen since time_bucket_gapfill is not defined
			 * for other datatypes
			 */
			Assert(false);
			break;
	}
}

/*
 * convert int64 to Datum according to type
 * internally we store all times as int64 in the
 * same format postgres does
 */
static inline Datum
gapfill_internal_get_datum(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return Int16GetDatum(value);
		case DATEOID:
		case INT4OID:
			return Int32GetDatum(value);
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case INT8OID:
			return Int64GetDatum(value);
		default:

			/*
			 * should never happen since time_bucket_gapfill is not defined
			 * for other datatypes
			 */
			Assert(false);
			break;
	}
}

static inline int64
interval_to_usec(Interval *interval)
{
	return (interval->month * DAYS_PER_MONTH * USECS_PER_DAY)
		+ (interval->day * USECS_PER_DAY)
		+ interval->time;
}

static inline int64
gapfill_period_get_internal(Oid timetype, Oid argtype, Datum arg)
{
	switch (timetype)
	{
		case DATEOID:
			Assert(INTERVALOID == argtype);
			return interval_to_usec(DatumGetIntervalP(arg)) / USECS_PER_DAY;
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			Assert(INTERVALOID == argtype);
			return interval_to_usec(DatumGetIntervalP(arg));
			break;
		case INT2OID:
			Assert(INT2OID == argtype);
			return DatumGetInt16(arg);
		case INT4OID:
			Assert(INT4OID == argtype);
			return DatumGetInt32(arg);
		case INT8OID:
			Assert(INT8OID == argtype);
			return DatumGetInt64(arg);
		default:

			/*
			 * should never happen since time_bucket_gapfill is not defined
			 * for other datatypes
			 */
			Assert(false);
			break;
	}
}

/*
 * Create a GapFill node from this plan. This is the full execution
 * state that replaces the plan node as the plan moves from planning to
 * execution.
 */
Node *
gapfill_state_create(CustomScan *cscan)
{
	GapFillState *state = (GapFillState *) newNode(sizeof(GapFillState), T_CustomScanState);

	state->csstate.methods = &gapfill_state_methods;
	state->subplan = linitial(cscan->custom_plans);

	return (Node *) state;
}

/*
 * Initialize the scan state
 */
static void
gapfill_begin(CustomScanState *node, EState *estate, int eflags)
{
	GapFillState *state = (GapFillState *) node;
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);

	/*
	 * this is the time_bucket_gapfill call from the plan which is used to
	 * extract arguments and to align gapfill_start
	 */
	FuncExpr   *func = linitial(cscan->custom_private);
	FuncExpr   *call = copyObject(func);
	TupleDesc	tupledesc = state->csstate.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	List	   *targetlist = copyObject(state->csstate.ss.ps.plan->targetlist);
	Node	   *entry;
	bool		isnull;
	Datum		arg1,
				arg3,
				arg4;

	state->gapfill_typid = func->funcresulttype;
	state->state = FETCHED_NONE;
	state->subslot = NULL;
	state->scanslot = MakeSingleTupleTableSlot(tupledesc);

	/* time_bucket interval */
	arg1 = gapfill_exec_expr(state, linitial(func->args), &isnull);
	/* isnull should never be true because time_bucket_gapfill is STRICT */
	Assert(!isnull);
	state->gapfill_period = gapfill_period_get_internal(func->funcresulttype, exprType(linitial(func->args)), arg1);

	/*
	 * pass gapfill start through time_bucket so it is aligned with bucket
	 * start
	 */
	call->args = list_make2(linitial(func->args), lthird(func->args));
	arg3 = gapfill_exec_expr(state, (Expr *) call, &isnull);
	/* isnull should never be true because time_bucket_gapfill is STRICT */
	Assert(!isnull);
	state->gapfill_start = gapfill_datum_get_internal(arg3, func->funcresulttype);
	state->next_timestamp = state->gapfill_start;

	/* gap fill end */
	arg4 = gapfill_exec_expr(state, lfourth(func->args), &isnull);
	/* isnull should never be true because time_bucket_gapfill is STRICT */
	Assert(!isnull);
	state->gapfill_end = gapfill_datum_get_internal(arg4, func->funcresulttype);

	/* remove start and end argument from time_bucket call */
	func->args = list_make2(linitial(func->args), lsecond(func->args));

	gapfill_state_initialize_columns(state);

	/*
	 * Build ProjectionInfo that will be used for gap filled tuples only.
	 *
	 * For every NULL_COLUMN we take the original expression tree from the
	 * subplan and replace Aggref nodes with Const NULL nodes. This is
	 * necessary because the expression might be evaluated below the
	 * aggregation so we need to pull up expression from subplan into
	 * projection for gapfilled tuples so expressions like COALESCE work
	 * correctly for gapfilled tuples.
	 */
	for (int i = 0; i < state->ncolumns; i++)
	{
		if (state->columns[i]->ctype == NULL_COLUMN)
		{
			entry = copyObject(list_nth(cscan->custom_scan_tlist, i));
			entry = expression_tree_mutator(entry, gapfill_aggref_mutator, NULL);
			lfirst(list_nth_cell(targetlist, i)) = entry;
		}
	}
	state->pi = ExecBuildProjectionInfoCompat(targetlist, state->csstate.ss.ps.ps_ExprContext, MakeSingleTupleTableSlot(tupledesc), &state->csstate.ss.ps, NULL);

	state->csstate.custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
}

/*
 * This is the main loop of the node it is called whenever the upper node
 * wants to consume a new tuple. Returning NULL signals that the tuples
 * are exhausted. All gapfill state transitions happen in this function.
 */
static TupleTableSlot *
gapfill_exec(CustomScanState *node)
{
	GapFillState *state = (GapFillState *) node;
	TupleTableSlot *slot = NULL;

	while (true)
	{

		/* fetch next tuple from subplan */
		if (FETCHED_NONE == state->state)
		{
			slot = gapfill_fetch_next_tuple(state);
			if (slot)
			{
				if (state->multigroup && gapfill_state_is_new_group(state, slot))
					state->state = FETCHED_NEXT_GROUP;
				else
					state->state = FETCHED_ONE;

				gapfill_state_set_next(state, slot);
			}
			else
				state->state = FETCHED_LAST;
		}

		/* return any subplan tuples before gapfill_start */
		if (FETCHED_ONE == state->state && state->subslot_time < state->gapfill_start)
		{
			state->state = FETCHED_NONE;
			return gapfill_state_return_subplan_slot(state);
		}

		/* if we have tuple from subplan check if it needs to be inserted now */
		if (FETCHED_ONE == state->state && state->subslot_time == state->next_timestamp)
		{
			state->state = FETCHED_NONE;
			state->next_timestamp += state->gapfill_period;
			return gapfill_state_return_subplan_slot(state);
		}

		/* if we are within gapfill boundaries we need to insert tuple */
		if (state->next_timestamp < state->gapfill_end)
		{
			Assert(state->state != FETCHED_NONE);
			slot = gapfill_state_gaptuple_create(state, state->next_timestamp);
			state->next_timestamp += state->gapfill_period;
			return slot;
		}

		/* return any remaining subplan tuples after gapfill_end */
		if (FETCHED_ONE == state->state)
		{
			state->state = FETCHED_NONE;
			return gapfill_state_return_subplan_slot(state);
		}

		/*
		 * Done with current group, prepare for next
		 */
		if (FETCHED_NEXT_GROUP == state->state)
		{
			state->state = FETCHED_ONE;
			state->next_timestamp = state->gapfill_start;
			gapfill_state_reset_group(state, state->subslot);
			continue;
		}

		return NULL;
	}
}

static void
gapfill_end(CustomScanState *node)
{
	if (node->custom_ps != NIL)
	{
		ExecEndNode(linitial(node->custom_ps));
	}
}

static void
gapfill_rescan(CustomScanState *node)
{
#if PG96
	node->ss.ps.ps_TupFromTlist = false;
#endif
	if (node->custom_ps != NIL)
	{
		ExecReScan(linitial(node->custom_ps));
	}
}

static void
gapfill_state_reset_group(GapFillState *state, TupleTableSlot *slot)
{
	GapFillColumnStateUnion column;
	int			i;
	Datum		value;
	bool		isnull;

	foreach_column(column.base, i, state)
	{
		value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
		switch (column.base->ctype)
		{
			case INTERPOLATE_COLUMN:
				gapfill_interpolate_group_change(column.interpolate, state->subslot_time, value, isnull);
				break;
			case LOCF_COLUMN:
				gapfill_locf_group_change(column.locf);
				break;
			case GROUP_COLUMN:
				column.group->isnull = isnull;
				if (!isnull)
					column.group->value = datumCopy(value, column.base->typbyval, column.base->typlen);
				break;
			default:
				break;
		}
	}
}

/*
 * Create generated tuple according to column state
 */
static TupleTableSlot *
gapfill_state_gaptuple_create(GapFillState *state, int64 time)
{
	TupleTableSlot *slot = state->scanslot;
	GapFillColumnStateUnion column;
	int			i;

	ExecClearTuple(slot);

	/*
	 * we need to fill in group columns first because locf and interpolation
	 * might reference those columns when doing out of bounds lookup
	 */
	foreach_column(column.base, i, state)
	{
		switch (column.base->ctype)
		{
			case TIME_COLUMN:
				slot->tts_values[i] = gapfill_internal_get_datum(time, state->gapfill_typid);
				slot->tts_isnull[i] = false;
				break;
			case GROUP_COLUMN:
				slot->tts_values[i] = column.group->value;
				slot->tts_isnull[i] = column.group->isnull;
				break;
			case NULL_COLUMN:
				slot->tts_isnull[i] = true;
				break;
			default:
				break;
		}
	}

	/*
	 * mark slot as containing data so it can be used in locf and interpolate
	 * lookup expressions
	 */
	ExecStoreVirtualTuple(slot);

	foreach_column(column.base, i, state)
	{
		switch (column.base->ctype)
		{
			case LOCF_COLUMN:
				gapfill_locf_calculate(column.locf, state, time, &slot->tts_values[i], &slot->tts_isnull[i]);
				break;
			case INTERPOLATE_COLUMN:
				gapfill_interpolate_calculate(column.interpolate, state, time, &slot->tts_values[i], &slot->tts_isnull[i]);
				break;
			default:
				break;
		}
	}

	ResetExprContext(state->pi->pi_exprContext);
	state->pi->pi_exprContext->ecxt_scantuple = slot;
#if PG96
	return ExecProject(state->pi, NULL);
#else
	return ExecProject(state->pi);
#endif
}

/*
 * Returns true if tuple in the TupleTableSlot belongs to the next
 * aggregation group
 */
static bool
gapfill_state_is_new_group(GapFillState *state, TupleTableSlot *slot)
{
	GapFillColumnStateUnion column;
	int			i;
	Datum		value;
	bool		isnull;

	/* groups not initialized yet */
	if (!state->groups_initialized)
	{
		state->groups_initialized = true;
		gapfill_state_reset_group(state, slot);
		return false;
	}

	foreach_column(column.base, i, state)
	{
		if (column.base->ctype == GROUP_COLUMN)
		{
			value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
			if (isnull && column.group->isnull)
				continue;
			if (isnull != column.group->isnull ||
				!datumIsEqual(value, column.group->value, column.base->typbyval, column.base->typlen))
				return true;
		}
	}

	return false;
}

/*
 * Returns subslot tuple and adjusts column state accordingly
 */
static TupleTableSlot *
gapfill_state_return_subplan_slot(GapFillState *state)
{
	GapFillColumnStateUnion column;
	int			i;
	Datum		value;
	bool		isnull;

	foreach_column(column.base, i, state)
	{
		switch (column.base->ctype)
		{
			case LOCF_COLUMN:
				value = slot_getattr(state->subslot, AttrOffsetGetAttrNumber(i), &isnull);
				gapfill_locf_tuple_returned(column.locf, value, isnull);
				break;
			case INTERPOLATE_COLUMN:
				value = slot_getattr(state->subslot, AttrOffsetGetAttrNumber(i), &isnull);
				gapfill_interpolate_tuple_returned(column.interpolate, state->subslot_time, value, isnull);
				break;
			default:
				break;
		}
	}

	return state->subslot;
}

static void
gapfill_state_set_next(GapFillState *state, TupleTableSlot *subslot)
{
	GapFillColumnStateUnion column;
	int			i;
	Datum		value;
	bool		isnull;

	/*
	 * if this tuple is for next group we dont update column state yet
	 * updating of column state happens in gapfill_state_reset_group instead
	 */
	if (FETCHED_NEXT_GROUP == state->state)
		return;

	foreach_column(column.base, i, state)
	{
		/* nothing to do here for locf */
		if (INTERPOLATE_COLUMN == column.base->ctype)
		{
			value = slot_getattr(subslot, AttrOffsetGetAttrNumber(i), &isnull);
			gapfill_interpolate_tuple_fetched(column.interpolate, state->subslot_time, value, isnull);
		}
	}
}

static TupleTableSlot *
gapfill_fetch_next_tuple(GapFillState *state)
{
	Datum		time_value;
	bool		isnull;
	TupleTableSlot *subslot = fetch_subplan_tuple((CustomScanState *) state);

	if (!subslot)
		return NULL;

	state->subslot = subslot;
	time_value = slot_getattr(subslot, AttrOffsetGetAttrNumber(state->time_index), &isnull);
	Assert(!isnull);
	state->subslot_time = gapfill_datum_get_internal(time_value, state->gapfill_typid);

	return subslot;
}

/*
 * Fetch tuple from subplan
 */
static TupleTableSlot *
fetch_subplan_tuple(CustomScanState *node)
{
	TupleTableSlot *subslot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
#if PG96
	TupleTableSlot *resultslot;
	ExprDoneCond isDone;
#endif

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	ResetExprContext(econtext);

	while (true)
	{
		subslot = ExecProcNode(linitial(node->custom_ps));

		if (TupIsNull(subslot))
			return NULL;

		if (!node->ss.ps.ps_ProjInfo)
			return subslot;

		econtext->ecxt_scantuple = subslot;

#if PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#else
		return ExecProject(node->ss.ps.ps_ProjInfo);
#endif
	}
}

/*
 * Initialize column meta data
 */
static void
gapfill_state_initialize_columns(GapFillState *state)
{
	TupleDesc	tupledesc = state->csstate.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	TargetEntry *tle;
	Expr	   *expr;

	state->ncolumns = tupledesc->natts;
	state->columns = palloc(state->ncolumns * sizeof(GapFillColumnState *));

	for (int i = 0; i < state->ncolumns; i++)
	{
		tle = list_nth(cscan->custom_scan_tlist, i);
		expr = list_nth(lthird(cscan->custom_private), i);

		if (tle->ressortgroupref && gapfill_is_group_column(state, tle))
		{
			/*
			 * if there is time_bucket_gapfill function call this is our time
			 * column
			 */
			if (IsA(expr, FuncExpr) &&
				strncmp(get_func_name(castNode(FuncExpr, expr)->funcid), GAPFILL_FUNCTION, NAMEDATALEN) == 0)
			{
				state->columns[i] = gapfill_column_state_create(TIME_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
				state->time_index = i;
				continue;
			}

			/* otherwise this is a normal group column */
			state->columns[i] = gapfill_column_state_create(GROUP_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
			state->multigroup = true;
			state->groups_initialized = false;
			continue;
		}
		else if (IsA(expr, FuncExpr))
		{
			/* locf and interpolate need to be toplevel function calls */
			if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid), GAPFILL_LOCF_FUNCTION, NAMEDATALEN) == 0)
			{
				state->columns[i] = gapfill_column_state_create(LOCF_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
				gapfill_locf_initialize((GapFillLocfColumnState *) state->columns[i], state, (FuncExpr *) expr);
				continue;
			}
			if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid), GAPFILL_INTERPOLATE_FUNCTION, NAMEDATALEN) == 0)
			{
				state->columns[i] = gapfill_column_state_create(INTERPOLATE_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
				gapfill_interpolate_initialize((GapFillInterpolateColumnState *) state->columns[i], state, (FuncExpr *) expr);
				continue;
			}
		}

		/* column with no special action from gap fill node */
		state->columns[i] = gapfill_column_state_create(NULL_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
	}
}

/*
 * Create GapFillColumnState object, set proper type and fill in datatype information
 */
static GapFillColumnState *
gapfill_column_state_create(GapFillColumnType ctype, Oid typeid)
{
	TypeCacheEntry *tce = lookup_type_cache(typeid, 0);
	GapFillColumnState *column;
	size_t		size;

	switch (ctype)
	{
		case GROUP_COLUMN:
			size = sizeof(GapFillGroupColumnState);
			break;
		case LOCF_COLUMN:
			size = sizeof(GapFillLocfColumnState);
			break;
		case INTERPOLATE_COLUMN:
			size = sizeof(GapFillInterpolateColumnState);
			break;
		default:
			size = sizeof(GapFillColumnState);
			break;
	}

	column = palloc0(size);
	column->ctype = ctype;
	column->typid = tce->type_id;
	column->typbyval = tce->typbyval;
	column->typlen = tce->typlen;

	return column;
}

/*
 * check if the target entry is a GROUP BY column, we need
 * this check because ressortgroupref will be nonzero for
 * ORDER BY and GROUP BY columns but we are only interested
 * in actual GROUP BY columns
 */
static bool
gapfill_is_group_column(GapFillState *state, TargetEntry *tle)
{
	ListCell   *lc;
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	List	   *groups = lsecond(cscan->custom_private);

	foreach(lc, groups)
	{
		if (tle->ressortgroupref == ((SortGroupClause *) lfirst(lc))->tleSortGroupRef)
			return true;
	}

	return false;
}

/*
 * Replace Aggref with const NULL
 */
static Node *
gapfill_aggref_mutator(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
		return (Node *) makeConst(((Aggref *) node)->aggtype, -1, InvalidOid, -2, (Datum) 0, true, false);

	return expression_tree_mutator((Node *) node, gapfill_aggref_mutator, context);
}

/*
 * Execute expression and return result of expression
 */
Datum
gapfill_exec_expr(GapFillState *state, Expr *expr, bool *isnull)
{
	ExprState  *exprstate = ExecInitExpr(expr, &state->csstate.ss.ps);
	ExprContext *exprcontext = GetPerTupleExprContext(state->csstate.ss.ps.state);

	exprcontext->ecxt_scantuple = state->scanslot;

#if PG96
	return ExecEvalExprSwitchContext(exprstate, exprcontext, isnull, NULL);
#else
	return ExecEvalExprSwitchContext(exprstate, exprcontext, isnull);
#endif
}

/*
 * Adjust attribute number of all Var nodes in an expression to have the
 * proper index into the gap filled tuple. This is necessary to make column
 * references in correlated subqueries in lookup queries work.
 */
Expr *
gapfill_adjust_varnos(GapFillState *state, Expr *expr)
{
	ListCell   *lc_var,
			   *lc_tle;
	List	   *vars = pull_var_clause((Node *) expr, 0);
	List	   *tlist = castNode(CustomScan, state->csstate.ss.ps.plan)->custom_scan_tlist;

	foreach(lc_var, vars)
	{
		Var		   *var = lfirst(lc_var);

		foreach(lc_tle, tlist)
		{
			TargetEntry *tle = lfirst(lc_tle);

			/*
			 * subqueries in aggregate queries can only reference columns so
			 * we only need to look for targetlist toplevel column references
			 */
			if (IsA(tle->expr, Var) &&castNode(Var, tle->expr)->varattno == var->varattno)
			{
				var->varattno = tle->resno;
			}
		}
	}
	return expr;
}
