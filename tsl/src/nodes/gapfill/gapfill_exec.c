/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <c.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/primnodes.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <utils/typcache.h>

#include <compat/compat.h>
#include "gapfill.h"
#include "gapfill_internal.h"
#include "interpolate.h"
#include "locf.h"
#include "time_bucket.h"
#include <annotations.h>

typedef enum GapFillBoundary
{
	GAPFILL_START,
	GAPFILL_END,
} GapFillBoundary;

typedef union GapFillColumnStateUnion
{
	GapFillColumnState *base;
	GapFillGroupColumnState *group;
	GapFillInterpolateColumnState *interpolate;
	GapFillLocfColumnState *locf;
} GapFillColumnStateUnion;

#define foreach_column(column, index, state)                                                       \
	Assert((state)->ncolumns > 0);                                                                 \
	for ((index) = 0, (column) = (state)->columns[index];                                          \
		 (index) < (state)->ncolumns && ((column) = (state)->columns[index], true);                \
		 (index)++)

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
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for time_bucket_gapfill: %s",
							format_type_be(type))));
			pg_unreachable();
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
			return Int64GetDatum(0);
	}
}

static Expr *
get_start_arg(GapFillState *state)
{
	if (!state->have_timezone)
		return lthird(state->args);
	else
		return lfourth(state->args);
}

static Expr *
get_finish_arg(GapFillState *state)
{
	if (!state->have_timezone)
		return lfourth(state->args);
	else
		return lfifth(state->args);
}

static Expr *
get_timezone_arg(GapFillState *state)
{
	Assert(state->have_timezone);
	return lthird(state->args);
}

static inline int64
gapfill_period_get_internal(Oid timetype, Oid argtype, Datum arg, Interval **interval)
{
	switch (timetype)
	{
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			Assert(INTERVALOID == argtype);
			Interval *interval_arg = DatumGetIntervalP(arg);
			if (interval_arg->time < 0 || interval_arg->day < 0 || interval_arg->month < 0 ||
				interval_arg->time + interval_arg->day + interval_arg->month == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid time_bucket_gapfill argument: bucket_width must be "
								"greater than 0")));
			}

			*interval = interval_arg;
			return 0;

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
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for time_bucket_gapfill: %s",
							format_type_be(timetype))));
			pg_unreachable();
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
	state->args = lfourth(cscan->custom_private);
	state->have_timezone = list_length(state->args) == 5;

	return (Node *) state;
}

static bool
is_const_null(Expr *expr)
{
	return IsA(expr, Const) && castNode(Const, expr)->constisnull;
}

/*
 * lookup cast func oid in pg_cast
 *
 * throws an error if no cast can be found
 */
static Oid
get_cast_func(Oid source, Oid target)
{
	Oid result = InvalidOid;
	HeapTuple casttup;

	casttup = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(source), ObjectIdGetDatum(target));
	if (HeapTupleIsValid(casttup))
	{
		Form_pg_cast castform = (Form_pg_cast) GETSTRUCT(casttup);

		result = castform->castfunc;
		ReleaseSysCache(casttup);
	}

	if (!OidIsValid(result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not find cast from %s to %s",
						format_type_be(source),
						format_type_be(target))));

	return result;
}

/*
 * returns true if v1 and v2 reference the same object
 */
static bool
var_equal(Var *v1, Var *v2)
{
	return v1->varno == v2->varno && v1->varattno == v2->varattno && v1->vartype == v2->vartype;
}

static bool
is_simple_expr_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	/*
	 * since expression_tree_walker does early exit on true
	 * logic is reverted and return value of true means expression
	 * is not simple, this is reverted in parent
	 */
	switch (nodeTag(node))
	{
			/*
			 * whitelist expression types we deem safe to execute in a
			 * separate expression context
			 */
		case T_Const:
		case T_FuncExpr:
		case T_NamedArgExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_BoolExpr:
		case T_CoerceViaIO:
		case T_CaseExpr:
		case T_CaseWhen:
			break;
		case T_Param:
			if (castNode(Param, node)->paramkind != PARAM_EXTERN)
				return true;
			break;
		default:
			return true;
	}
	return expression_tree_walker(node, is_simple_expr_walker, context);
}

/*
 * check if expression is simple expression and contains only simple
 * subexpressions
 */
static bool
is_simple_expr(Expr *node)
{
	/*
	 * since expression_tree_walker does early exit on true and we use that to
	 * skip processing on first non-simple expression we invert return value
	 * from expression_tree_walker here
	 */
	return !is_simple_expr_walker((Node *) node, NULL);
}

/*
 * align a value with the bucket boundary
 * even though we use int64 as our internal representation we cannot call
 * ts_int64_bucket here because int variants of time_bucket align differently
 * then non-int variants because the bucket start is on monday for the latter
 */
static int64
align_with_time_bucket(GapFillState *state, Expr *expr)
{
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	FuncExpr *time_bucket = copyObject(linitial(cscan->custom_private));
	Datum value;
	bool isnull;

	if (!is_simple_expr(expr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg(
					 "invalid time_bucket_gapfill argument: start must be a simple expression")));

	if (state->have_timezone)
	{
		if (IsA(get_timezone_arg(state), Const) &&
			castNode(Const, get_timezone_arg(state))->constisnull)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid time_bucket_gapfill argument: timezone cannot be NULL")));
		}

		time_bucket->args =
			list_make3(linitial(time_bucket->args), expr, lthird(time_bucket->args));
	}
	else
	{
		time_bucket->args = list_make2(linitial(time_bucket->args), expr);
	}
	value = gapfill_exec_expr(state, (Expr *) time_bucket, &isnull);

	/* start expression must not evaluate to NULL */
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: start cannot be NULL"),
				 errhint("Specify start and finish as arguments or in the WHERE clause.")));

	return gapfill_datum_get_internal(value, state->gapfill_typid);
}

static int64
get_boundary_expr_value(GapFillState *state, GapFillBoundary boundary, Expr *expr)
{
	Datum arg_value;
	bool isnull;

	/*
	 * add an explicit cast here if types do not match
	 */
	if (exprType((Node *) expr) != state->gapfill_typid)
	{
		Oid cast_oid = get_cast_func(exprType((Node *) expr), state->gapfill_typid);

		expr = (Expr *) makeFuncExpr(cast_oid,
									 state->gapfill_typid,
									 list_make1(expr),
									 InvalidOid,
									 InvalidOid,
									 0);
	}

	arg_value = gapfill_exec_expr(state, expr, &isnull);

	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: %s cannot be NULL",
						boundary == GAPFILL_START ? "start" : "finish"),
				 errhint("Specify start and finish as arguments or in the WHERE clause.")));

	return gapfill_datum_get_internal(arg_value, state->gapfill_typid);
}

typedef struct CollectBoundaryContext
{
	List *quals;
	Var *ts_var;
} CollectBoundaryContext;

/*
 * expression references our gapfill time column and could be
 * a boundary expression, more thorough check is in
 * infer_gapfill_boundary
 */
static bool
is_boundary_expr(Node *node, CollectBoundaryContext *context)
{
	OpExpr *op;
	Node *left, *right;

	if (!IsA(node, OpExpr))
		return false;

	op = castNode(OpExpr, node);

	if (op->args->length != 2)
		return false;

	left = linitial(op->args);
	right = llast(op->args);

	/* Var OP Var is not useful here because we are not yet at a point
	 * where we could evaluate them */
	if (IsA(left, Var) && IsA(right, Var))
		return false;

	if (IsA(left, Var) && var_equal(castNode(Var, left), context->ts_var))
		return true;

	if (IsA(right, Var) && var_equal(castNode(Var, right), context->ts_var))
		return true;

	return false;
}

static bool
collect_boundary_walker(Node *node, CollectBoundaryContext *context)
{
	Node *quals = NULL;

	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		quals = castNode(FromExpr, node)->quals;
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *j = castNode(JoinExpr, node);

		/* don't descend into outer join */
		if (IS_OUTER_JOIN(j->jointype))
			return false;

		quals = j->quals;
	}

	if (quals)
	{
		ListCell *lc;

		foreach (lc, castNode(List, quals))
		{
			if (is_boundary_expr(lfirst(lc), context))
				context->quals = lappend(context->quals, lfirst(lc));
		}
	}

	return expression_tree_walker(node, collect_boundary_walker, context);
}

/*
 * traverse jointree to look for expressions referencing
 * the time column of our gapfill call
 */
static List *
collect_boundary_expressions(Node *node, Var *ts_var)
{
	CollectBoundaryContext context = { .quals = NIL, .ts_var = ts_var };

	collect_boundary_walker(node, &context);

	return context.quals;
}

static int64
infer_gapfill_boundary(GapFillState *state, GapFillBoundary boundary)
{
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	FuncExpr *func = linitial(cscan->custom_private);
	FromExpr *jt = lthird(cscan->custom_private);
	ListCell *lc;
	Var *ts_var;
	TypeCacheEntry *tce = lookup_type_cache(state->gapfill_typid, TYPECACHE_BTREE_OPFAMILY);
	int strategy;
	Oid lefttype, righttype;
	List *quals;

	int64 boundary_value = 0;
	bool boundary_found = false;

	/*
	 * if the second argument to time_bucket_gapfill is not a column reference
	 * we cannot match WHERE clause to the time column
	 */
	if (!IsA(lsecond(func->args), Var))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: ts needs to refer to a single "
						"column if no start or finish is supplied"),
				 errhint("Specify start and finish as arguments or in the WHERE clause.")));

	ts_var = castNode(Var, lsecond(func->args));

	quals = collect_boundary_expressions((Node *) jt, ts_var);

	foreach (lc, quals)
	{
		OpExpr *opexpr = lfirst_node(OpExpr, lc);
		Var *var;
		Expr *expr;
		Oid op;
		int64 value;

		if (IsA(linitial(opexpr->args), Var))
		{
			var = linitial(opexpr->args);
			expr = lsecond(opexpr->args);
			op = opexpr->opno;
		}
		else if (IsA(lsecond(opexpr->args), Var))
		{
			var = lsecond(opexpr->args);
			expr = linitial(opexpr->args);
			op = get_commutator(opexpr->opno);
		}
		else
		{
			/* collect_boundary_expressions has filtered those out already */
			Assert(false);
			continue;
		}

		if (!op_in_opfamily(op, tce->btree_opf))
			continue;

		/*
		 * only allow simple expressions because Params have not been set up
		 * at this stage and Vars will not work either because we execute in
		 * separate execution context
		 */
		if (!is_simple_expr(expr) || !var_equal(ts_var, var))
			continue;

		get_op_opfamily_properties(op, tce->btree_opf, false, &strategy, &lefttype, &righttype);

		if (boundary == GAPFILL_START && strategy != BTGreaterStrategyNumber &&
			strategy != BTGreaterEqualStrategyNumber)
			continue;
		if (boundary == GAPFILL_END && strategy != BTLessStrategyNumber &&
			strategy != BTLessEqualStrategyNumber)
			continue;

		value = get_boundary_expr_value(state, boundary, expr);

		/*
		 * if the boundary expression operator does not match the operator
		 * used by the gapfill node we adjust the value by 1 here
		 *
		 * the operators for the gapfill node are >= for start and < for end
		 * column > value becomes start >= value + 1 column <= value becomes
		 * end < value + 1
		 */
		if (strategy == BTGreaterStrategyNumber || strategy == BTLessEqualStrategyNumber)
			value += 1;

		if (!boundary_found)
		{
			boundary_found = true;
			boundary_value = value;
		}
		else
		{
			if (boundary == GAPFILL_START)
				boundary_value = Max(boundary_value, value);
			else
				boundary_value = Min(boundary_value, value);
		}
	}

	if (boundary_found)
		return boundary_value;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("missing time_bucket_gapfill argument: could not infer %s from WHERE clause",
					boundary == GAPFILL_START ? "start" : "finish"),
			 errhint("Specify start and finish as arguments or in the WHERE clause.")));
	pg_unreachable();
}

static Const *
make_const_value_for_gapfill_internal(Oid typid, int64 value)
{
	TypeCacheEntry *tce = lookup_type_cache(typid, 0);
	Datum d = gapfill_internal_get_datum(value, typid);

	return makeConst(typid, -1, InvalidOid, tce->typlen, d, false, tce->typbyval);
}

static void
gapfill_advance_timestamp(GapFillState *state)
{
	Datum next;

	switch (state->gapfill_typid)
	{
		case DATEOID:
			next = DirectFunctionCall2(date_pl_interval,
									   DateADTGetDatum(state->gapfill_start),
									   IntervalPGetDatum(state->next_offset));
			next = DirectFunctionCall1(timestamp_date, next);
			state->next_timestamp = DatumGetDateADT(next);
			break;
		case TIMESTAMPOID:
			next = DirectFunctionCall2(timestamp_pl_interval,
									   TimestampGetDatum(state->gapfill_start),
									   IntervalPGetDatum(state->next_offset));
			state->next_timestamp = DatumGetTimestamp(next);
			break;
		case TIMESTAMPTZOID:
			/*
			 * To be consistent with time_bucket we do UTC bucketing unless
			 * a different timezone got explicitly passed to the function
			 * and we are bucketing by non-fixed intervals.
			 */
			if (state->have_timezone &&
				(state->next_offset->day != 0 || state->next_offset->month != 0))
			{
				bool isnull;
				/* TODO: optimize by constifying and caching the datum if possible */
				Datum tzname = gapfill_exec_expr(state, get_timezone_arg(state), &isnull);
				Assert(!isnull);

				/* Convert to local timestamp */
				next = DirectFunctionCall2(timestamptz_zone,
										   tzname,
										   TimestampTzGetDatum(state->gapfill_start));

				/* Add interval */
				next = DirectFunctionCall2(timestamp_pl_interval,
										   next,
										   IntervalPGetDatum(state->next_offset));

				/* Convert back to specified timezone */
				next = DirectFunctionCall2(timestamp_zone, tzname, next);
			}
			else
			{
				next = DirectFunctionCall2(timestamp_pl_interval,
										   TimestampTzGetDatum(state->gapfill_start),
										   IntervalPGetDatum(state->next_offset));
			}
			state->next_timestamp = DatumGetTimestampTz(next);
			break;
		default:
			state->next_timestamp += state->gapfill_period;
			break;
	}
	/* Advance the interval offset if necessary */
	if (state->gapfill_interval)
	{
		Datum tspan = DirectFunctionCall2(interval_pl,
										  IntervalPGetDatum(state->gapfill_interval),
										  IntervalPGetDatum(state->next_offset));
		state->next_offset = DatumGetIntervalP(tspan);
	}
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
	FuncExpr *func = linitial(cscan->custom_private);
	TupleDesc tupledesc = state->csstate.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	List *targetlist = copyObject(state->csstate.ss.ps.plan->targetlist);
	Node *entry;
	bool isnull;
	Datum arg_value;
	int i;

	state->gapfill_typid = func->funcresulttype;
	state->state = FETCHED_NONE;
	state->subslot = MakeSingleTupleTableSlot(tupledesc, &TTSOpsVirtual);
	state->scanslot = MakeSingleTupleTableSlot(tupledesc, &TTSOpsVirtual);

	/* bucket_width */
	if (!is_simple_expr(linitial(state->args)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: bucket_width must be a simple "
						"expression")));

	arg_value = gapfill_exec_expr(state, linitial(state->args), &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: bucket_width cannot be NULL")));

	state->gapfill_period = gapfill_period_get_internal(func->funcresulttype,
														exprType(linitial(state->args)),
														arg_value,
														&state->gapfill_interval);

	/*
	 * this would error when trying to align start and stop to bucket_width as well below
	 * but checking this explicitly here will make a nicer error message
	 */
	if (state->gapfill_period <= 0 && !state->gapfill_interval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg(
					 "invalid time_bucket_gapfill argument: bucket_width must be greater than 0")));

	/*
	 * check if gapfill start was left out so we have to infer from WHERE
	 * clause
	 */
	if (is_const_null(get_start_arg(state)))
	{
		int64 start = infer_gapfill_boundary(state, GAPFILL_START);
		Const *expr = make_const_value_for_gapfill_internal(state->gapfill_typid, start);

		state->gapfill_start = align_with_time_bucket(state, (Expr *) expr);
	}
	else
	{
		/*
		 * pass gapfill start through time_bucket so it is aligned with bucket
		 * start
		 */
		state->gapfill_start = align_with_time_bucket(state, get_start_arg(state));
	}
	state->next_timestamp = state->gapfill_start;
	state->next_offset = state->gapfill_interval;

	/* gap fill end */
	if (is_const_null(get_finish_arg(state)))
		state->gapfill_end = infer_gapfill_boundary(state, GAPFILL_END);
	else
	{
		if (!is_simple_expr(get_finish_arg(state)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid time_bucket_gapfill argument: finish must be a simple "
							"expression")));
		arg_value = gapfill_exec_expr(state, get_finish_arg(state), &isnull);

		/*
		 * the default value for finish is NULL but this is checked above,
		 * when a non-Const is passed here that evaluates to NULL we bail
		 */
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid time_bucket_gapfill argument: finish cannot be NULL"),
					 errhint("Specify start and finish as arguments or in the WHERE clause.")));

		state->gapfill_end = gapfill_datum_get_internal(arg_value, func->funcresulttype);
	}

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
	for (i = 0; i < state->ncolumns; i++)
	{
		if (state->columns[i]->ctype == NULL_COLUMN)
		{
			entry = copyObject(list_nth(cscan->custom_scan_tlist, i));
			entry = gapfill_aggref_mutator(entry, NULL);
			lfirst(list_nth_cell(targetlist, i)) = entry;
		}
	}
	state->pi = ExecBuildProjectionInfo(targetlist,
										state->csstate.ss.ps.ps_ExprContext,
										MakeSingleTupleTableSlot(tupledesc, &TTSOpsVirtual),
										&state->csstate.ss.ps,
										NULL);

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
		CHECK_FOR_INTERRUPTS();

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
			{
				/*
				 * if GROUP BY has non time_bucket_gapfill columns but the
				 * query has not initialized the groups there is nothing we
				 * can do here
				 */
				if (state->multigroup && !state->groups_initialized)
					return NULL;
				else
					state->state = FETCHED_LAST;
			}
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
			gapfill_advance_timestamp(state);
			return gapfill_state_return_subplan_slot(state);
		}

		/* if we are within gapfill boundaries we need to insert tuple */
		if (state->next_timestamp < state->gapfill_end)
		{
			Assert(state->state != FETCHED_NONE);
			slot = gapfill_state_gaptuple_create(state, state->next_timestamp);
			gapfill_advance_timestamp(state);
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
	if (node->custom_ps != NIL)
	{
		ExecReScan(linitial(node->custom_ps));
	}
	((GapFillState *) node)->state = FETCHED_NONE;
}

static void
gapfill_state_reset_group(GapFillState *state, TupleTableSlot *slot)
{
	GapFillColumnStateUnion column;
	int i;
	Datum value;
	bool isnull;

	foreach_column(column.base, i, state)
	{
		value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
		switch (column.base->ctype)
		{
			case INTERPOLATE_COLUMN:
				gapfill_interpolate_group_change(column.interpolate,
												 state->subslot_time,
												 value,
												 isnull);
				break;
			case LOCF_COLUMN:
				gapfill_locf_group_change(column.locf);
				break;
			case GROUP_COLUMN:
			case DERIVED_COLUMN:
				column.group->isnull = isnull;
				if (!isnull)
					column.group->value =
						datumCopy(value, column.base->typbyval, column.base->typlen);
				break;
			default:
				break;
		}
	}
	state->next_offset = state->gapfill_interval;
}

/*
 * Create generated tuple according to column state
 */
static TupleTableSlot *
gapfill_state_gaptuple_create(GapFillState *state, int64 time)
{
	TupleTableSlot *slot = state->scanslot;
	GapFillColumnStateUnion column;
	int i;

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
			case DERIVED_COLUMN:
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
				gapfill_locf_calculate(column.locf,
									   state,
									   time,
									   &slot->tts_values[i],
									   &slot->tts_isnull[i]);
				break;
			case INTERPOLATE_COLUMN:
				gapfill_interpolate_calculate(column.interpolate,
											  state,
											  time,
											  &slot->tts_values[i],
											  &slot->tts_isnull[i]);
				break;
			default:
				break;
		}
	}

	ResetExprContext(state->pi->pi_exprContext);
	state->pi->pi_exprContext->ecxt_scantuple = slot;
	return ExecProject(state->pi);
}

/*
 * Returns true if tuple in the TupleTableSlot belongs to the next
 * aggregation group
 */
static bool
gapfill_state_is_new_group(GapFillState *state, TupleTableSlot *slot)
{
	GapFillColumnStateUnion column;
	int i;
	Datum value;
	bool isnull;

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
			if (isnull != column.group->isnull)
				return true;
			/* We need to use FunctionCall2Coll here since equality comparison
			 * functions can try to access flinfo (see arrayfuncs.c). */
			if (!DatumGetBool(FunctionCall2Coll(&column.group->eq_func,
												column.group->collation,
												value,
												column.group->value)))
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
	CustomScanState *node = castNode(CustomScanState, state);
	int i;
	Datum value;
	bool isnull;

	foreach_column(column.base, i, state)
	{
		switch (column.base->ctype)
		{
			case LOCF_COLUMN:
				value = slot_getattr(state->subslot, AttrOffsetGetAttrNumber(i), &isnull);
				if (isnull && column.locf->treat_null_as_missing)
					gapfill_locf_calculate(column.locf,
										   state,
										   state->subslot_time,
										   &state->subslot->tts_values[i],
										   &state->subslot->tts_isnull[i]);
				else
					gapfill_locf_tuple_returned(column.locf, value, isnull);
				break;
			case INTERPOLATE_COLUMN:
				value = slot_getattr(state->subslot, AttrOffsetGetAttrNumber(i), &isnull);
				gapfill_interpolate_tuple_returned(column.interpolate,
												   state->subslot_time,
												   value,
												   isnull);
				break;
			default:
				break;
		}
	}

	if (node->ss.ps.ps_ProjInfo)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;
		ResetExprContext(econtext);
		econtext->ecxt_scantuple = state->subslot;
		return ExecProject(node->ss.ps.ps_ProjInfo);
	}

	return state->subslot;
}

static void
gapfill_state_set_next(GapFillState *state, TupleTableSlot *subslot)
{
	GapFillColumnStateUnion column;
	int i;
	Datum value;
	bool isnull;

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
			gapfill_interpolate_tuple_fetched(column.interpolate,
											  state->subslot_time,
											  value,
											  isnull);
		}
	}
}

static TupleTableSlot *
gapfill_fetch_next_tuple(GapFillState *state)
{
	Datum time_value;
	bool isnull;
	PlanState *subplan = linitial(castNode(CustomScanState, state)->custom_ps);
	TupleTableSlot *subslot = ExecProcNode(subplan);

	if (TupIsNull(subslot))
		return NULL;

	/* we cannot simply treat an arbitrary source slot as virtual,
	 * instead we must copy the data into our own slot in order to be able to
	 * modify it
	 */
	ExecCopySlot(state->subslot, subslot);
	time_value = slot_getattr(subslot, AttrOffsetGetAttrNumber(state->time_index), &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time_bucket_gapfill argument: ts cannot be NULL")));

	state->subslot_time = gapfill_datum_get_internal(time_value, state->gapfill_typid);

	return state->subslot;
}

/*
 * Initialize column meta data
 */
static void
gapfill_state_initialize_columns(GapFillState *state)
{
	TupleDesc tupledesc = state->csstate.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	TargetEntry *tle;
	Expr *expr;
	int i;

	state->ncolumns = tupledesc->natts;
	state->columns = palloc(state->ncolumns * sizeof(GapFillColumnState *));

	for (i = 0; i < state->ncolumns; i++)
	{
		tle = list_nth(cscan->custom_scan_tlist, i);
		expr = tle->expr;

		if (tle->ressortgroupref && gapfill_is_group_column(state, tle))
		{
			/*
			 * if there is time_bucket_gapfill function call this is our time
			 * column
			 */
			if (IsA(expr, FuncExpr) && strncmp(get_func_name(castNode(FuncExpr, expr)->funcid),
											   GAPFILL_FUNCTION,
											   NAMEDATALEN) == 0)
			{
				state->columns[i] =
					gapfill_column_state_create(TIME_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
				state->time_index = i;
				continue;
			}

			/* otherwise this is a normal group column */
			state->columns[i] =
				gapfill_column_state_create(GROUP_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
			state->multigroup = true;
			state->groups_initialized = false;
			continue;
		}
		else if (IsA(expr, FuncExpr))
		{
			/* locf and interpolate will be toplevel function calls in the gapfill node */
			if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid),
						GAPFILL_LOCF_FUNCTION,
						NAMEDATALEN) == 0)
			{
				state->columns[i] =
					gapfill_column_state_create(LOCF_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
				gapfill_locf_initialize((GapFillLocfColumnState *) state->columns[i],
										state,
										(FuncExpr *) expr);
				continue;
			}
			if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid),
						GAPFILL_INTERPOLATE_FUNCTION,
						NAMEDATALEN) == 0)
			{
				state->columns[i] =
					gapfill_column_state_create(INTERPOLATE_COLUMN,
												TupleDescAttr(tupledesc, i)->atttypid);
				gapfill_interpolate_initialize((GapFillInterpolateColumnState *) state->columns[i],
											   state,
											   (FuncExpr *) expr);
				continue;
			}
		}

		/*
		 * any column that does not have an aggregation function and is not
		 * an explicit GROUP BY column has to be derived from a GROUP BY
		 * column so we treat those similar to GROUP BY column for gapfill
		 * purposes.
		 */
		if (!contain_agg_clause((Node *) expr) && contain_var_clause((Node *) expr))
		{
			state->columns[i] =
				gapfill_column_state_create(DERIVED_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
			state->multigroup = true;
			state->groups_initialized = false;
			continue;
		}

		/* column with no special action from gap fill node */
		state->columns[i] =
			gapfill_column_state_create(NULL_COLUMN, TupleDescAttr(tupledesc, i)->atttypid);
	}
}

/*
 * Create GapFillColumnState object, set proper type and fill in datatype information
 */
static GapFillColumnState *
gapfill_column_state_create(GapFillColumnType ctype, Oid typeid)
{
	TypeCacheEntry *tce;
	int tc_flags = 0;
	GapFillColumnState *column;
	size_t size;

	switch (ctype)
	{
		case GROUP_COLUMN:
			tc_flags |= TYPECACHE_EQ_OPR;
			TS_FALLTHROUGH;
		case DERIVED_COLUMN:
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
	tce = lookup_type_cache(typeid, tc_flags);

	column = palloc0(size);
	column->ctype = ctype;
	column->typid = tce->type_id;
	column->typbyval = tce->typbyval;
	column->typlen = tce->typlen;

	if (ctype == GROUP_COLUMN)
	{
		GapFillGroupColumnState *gcolumn = (GapFillGroupColumnState *) column;
		Oid eq_opr_func = get_opcode(tce->eq_opr);
		fmgr_info_cxt(eq_opr_func, &gcolumn->eq_func, CurrentMemoryContext);
		gcolumn->collation = tce->typcollation;
	}

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
	ListCell *lc;
	CustomScan *cscan = castNode(CustomScan, state->csstate.ss.ps.plan);
	List *groups = lsecond(cscan->custom_private);

	foreach (lc, groups)
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
		return NULL;

	if (IsA(node, Aggref))
		return (Node *)
			makeConst(((Aggref *) node)->aggtype, -1, InvalidOid, -2, (Datum) 0, true, false);

	return expression_tree_mutator(node, gapfill_aggref_mutator, context);
}

/*
 * Execute expression and return result of expression
 */
Datum
gapfill_exec_expr(GapFillState *state, Expr *expr, bool *isnull)
{
	ExprState *exprstate = ExecInitExpr(expr, &state->csstate.ss.ps);
	ExprContext *exprcontext = GetPerTupleExprContext(state->csstate.ss.ps.state);

	exprcontext->ecxt_scantuple = state->scanslot;

	return ExecEvalExprSwitchContext(exprstate, exprcontext, isnull);
}

/*
 * Adjust attribute number of all Var nodes in an expression to have the
 * proper index into the gap filled tuple. This is necessary to make column
 * references in correlated subqueries in lookup queries work.
 */
Expr *
gapfill_adjust_varnos(GapFillState *state, Expr *expr)
{
	ListCell *lc_var, *lc_tle;
	List *vars = pull_var_clause((Node *) expr, 0);
	List *tlist = castNode(CustomScan, state->csstate.ss.ps.plan)->custom_scan_tlist;

	foreach (lc_var, vars)
	{
		Var *var = lfirst(lc_var);

		foreach (lc_tle, tlist)
		{
			TargetEntry *tle = lfirst(lc_tle);

			/*
			 * subqueries in aggregate queries can only reference columns so
			 * we only need to look for targetlist toplevel column references
			 */
			if (IsA(tle->expr, Var) && castNode(Var, tle->expr)->varattno == var->varattno)
			{
				var->varattno = tle->resno;
			}
		}
	}
	return expr;
}
