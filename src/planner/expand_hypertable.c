/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/* This planner optimization reduces planning times when a hypertable has many chunks.
 * It does this by expanding hypertable chunks manually, eliding the `expand_inherited_tables`
 * logic used by PG.
 *
 * Slow planning time were previously seen because `expand_inherited_tables` expands all chunks of
 * a hypertable, without regard to constraints present in the query. Then, `get_relation_info` is
 * called on all chunks before constraint exclusion. Getting the statistics on many chunks ends
 * up being expensive because RelationGetNumberOfBlocks has to open the file for each relation.
 * This gets even worse under high concurrency.
 *
 * This logic solves this by expanding only the chunks needed to fulfil the query instead of all
 * chunks. In effect, it moves chunk exclusion up in the planning process. But, we actually don't
 * use constraint exclusion here, but rather a variant of range exclusion implemented by
 * HypertableRestrictInfo.
 * */

#include <postgres.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_func.h>
#include <parser/parsetree.h>
#include <partitioning/partbounds.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/errcodes.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/syscache.h>

#include "chunk.h"
#include "compat/compat.h"
#include "cross_module_fn.h"
#include "extension.h"
#include "extension_constants.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "import/planner.h"
#include "nodes/chunk_append/chunk_append.h"
#include "partitioning.h"
#include "partialize.h"
#include "planner.h"
#include "time_utils.h"

typedef struct CollectQualCtx
{
	PlannerInfo *root;
	RelOptInfo *rel;
	List *restrictions;
	FuncExpr *chunk_exclusion_func;
	List *join_conditions;
	List *propagate_conditions;
	List *all_quals;
	int join_level;
} CollectQualCtx;

static void propagate_join_quals(PlannerInfo *root, RelOptInfo *rel, CollectQualCtx *ctx);

static Oid chunk_exclusion_func = InvalidOid;

static Oid ts_chunks_arg_types[] = { RECORDOID, INT4ARRAYOID };

static void
init_chunk_exclusion_func()
{
	if (!OidIsValid(chunk_exclusion_func))
	{
		List *l = list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(CHUNK_EXCL_FUNC_NAME));
		chunk_exclusion_func =
			LookupFuncName(l, lengthof(ts_chunks_arg_types), ts_chunks_arg_types, false);
	}
	Assert(OidIsValid(chunk_exclusion_func));
}

static bool
is_chunk_exclusion_func(Expr *node)
{
	if (IsA(node, FuncExpr) && castNode(FuncExpr, node)->funcid == chunk_exclusion_func)
		return true;

	return false;
}

static bool
is_time_bucket_function(Expr *node)
{
	if (IsA(node, FuncExpr) &&
		strncmp(get_func_name(castNode(FuncExpr, node)->funcid), "time_bucket", NAMEDATALEN) == 0)
		return true;

	return false;
}

static void
ts_add_append_rel_infos(PlannerInfo *root, List *appinfos)
{
	ListCell *lc;

	root->append_rel_list = list_concat(root->append_rel_list, appinfos);

	/* root->append_rel_array is required to be able to hold all the
	 * additional entries by previous call to expand_planner_arrays */
	Assert(root->append_rel_array);

	foreach (lc, appinfos)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);
		int child_relid = appinfo->child_relid;
		Assert(child_relid < root->simple_rel_array_size);

		root->append_rel_array[child_relid] = appinfo;
	}
}

/*
 * Pre-check to determine if an expression is eligible for constification.
 * A more thorough check is in constify_timestamptz_op_interval.
 */
static bool
is_timestamptz_op_interval(Expr *expr)
{
	OpExpr *op;
	Const *c1, *c2;

	if (!IsA(expr, OpExpr))
		return false;

	op = castNode(OpExpr, expr);

	if (op->opresulttype != TIMESTAMPTZOID || op->args->length != 2 ||
		!IsA(linitial(op->args), Const) || !IsA(llast(op->args), Const))
		return false;

	c1 = linitial_node(Const, op->args);
	c2 = llast_node(Const, op->args);

	return (c1->consttype == TIMESTAMPTZOID && c2->consttype == INTERVALOID) ||
		   (c1->consttype == INTERVALOID && c2->consttype == TIMESTAMPTZOID);
}

static Datum
int_get_datum(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return Int16GetDatum(value);
		case INT4OID:
			return Int32GetDatum(value);
		case INT8OID:
			return Int64GetDatum(value);
		case TIMESTAMPOID:
			return TimestampGetDatum(value);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(value);
	}

	elog(ERROR, "unsupported datatype in int_get_datum: %s", format_type_be(type));
	pg_unreachable();
}

static int64
const_datum_get_int(Const *cnst)
{
	Assert(!cnst->constisnull);

	switch (cnst->consttype)
	{
		case INT2OID:
			return (int64) (DatumGetInt16(cnst->constvalue));
		case INT4OID:
			return (int64) (DatumGetInt32(cnst->constvalue));
		case INT8OID:
			return DatumGetInt64(cnst->constvalue);
		case DATEOID:
			return DatumGetDateADT(cnst->constvalue);
		case TIMESTAMPOID:
			return DatumGetTimestamp(cnst->constvalue);
		case TIMESTAMPTZOID:
			return DatumGetTimestampTz(cnst->constvalue);
	}

	elog(ERROR, "unsupported datatype in const_datum_get_int: %s", format_type_be(cnst->consttype));
	pg_unreachable();
}

/*
 * Constify expressions of the following form in WHERE clause:
 *
 * column OP timestamptz - interval
 * column OP timestamptz + interval
 * column OP interval + timestamptz
 *
 * Iff interval has no month component.
 *
 * Since the operators for timestamptz OP interval are marked
 * as stable they will not be constified during planning.
 * However, intervals without a month component can be safely
 * constified during planning as the result of those calculations
 * do not depend on the timezone setting.
 */
static OpExpr *
constify_timestamptz_op_interval(PlannerInfo *root, OpExpr *constraint)
{
	Expr *left, *right;
	OpExpr *op;
	bool var_on_left = false;
	Interval *interval;
	Const *c_ts, *c_int;
	Datum constified;
	PGFunction opfunc;
	Oid ts_pl_int, ts_mi_int, int_pl_ts;

	/* checked in caller already so only asserting */
	Assert(constraint->args->length == 2);

	left = linitial(constraint->args);
	right = llast(constraint->args);

	if (IsA(left, Var) && IsA(right, OpExpr))
	{
		op = castNode(OpExpr, right);
		var_on_left = true;
	}
	else if (IsA(left, OpExpr) && IsA(right, Var))
	{
		op = castNode(OpExpr, left);
	}
	else
		return constraint;

	ts_pl_int = ts_get_operator("+", PG_CATALOG_NAMESPACE, TIMESTAMPTZOID, INTERVALOID);
	ts_mi_int = ts_get_operator("-", PG_CATALOG_NAMESPACE, TIMESTAMPTZOID, INTERVALOID);
	int_pl_ts = ts_get_operator("+", PG_CATALOG_NAMESPACE, INTERVALOID, TIMESTAMPTZOID);

	if (op->opno == ts_pl_int)
	{
		/* TIMESTAMPTZ + INTERVAL */
		opfunc = timestamptz_pl_interval;
		c_ts = linitial_node(Const, op->args);
		c_int = llast_node(Const, op->args);
	}
	else if (op->opno == ts_mi_int)
	{
		/* TIMESTAMPTZ - INTERVAL */
		opfunc = timestamptz_mi_interval;
		c_ts = linitial_node(Const, op->args);
		c_int = llast_node(Const, op->args);
	}
	else if (op->opno == int_pl_ts)
	{
		/* INTERVAL + TIMESTAMPTZ */
		opfunc = timestamptz_pl_interval;
		c_int = linitial_node(Const, op->args);
		c_ts = llast_node(Const, op->args);
	}
	else
		return constraint;

	/*
	 * arg types should match operator and were checked in precheck
	 * so only asserting here
	 */
	Assert(c_ts->consttype == TIMESTAMPTZOID);
	Assert(c_int->consttype == INTERVALOID);
	if (c_ts->constisnull || c_int->constisnull)
		return constraint;

	interval = DatumGetIntervalP(c_int->constvalue);

	/*
	 * constification is only safe when the interval has no month component
	 * because month length is variable and calculation depends on local timezone
	 */
	if (interval->month != 0)
		return constraint;

	constified = DirectFunctionCall2(opfunc, c_ts->constvalue, c_int->constvalue);

	/*
	 * Since constifying intervals with day component does depend on the timezone
	 * this can lead to different results around daylight saving time switches.
	 * So we add a safety buffer when the interval has day components to counteract.
	 */
	if (interval->day != 0)
	{
		bool add;
		TimestampTz constified_tstz = DatumGetTimestampTz(constified);

		switch (constraint->opfuncid)
		{
			case F_TIMESTAMPTZ_LE:
			case F_TIMESTAMPTZ_LT:
				add = true;
				break;
			case F_TIMESTAMPTZ_GE:
			case F_TIMESTAMPTZ_GT:
				add = false;
				break;
			default:
				return constraint;
		}
		/*
		 * If Var is on wrong side reverse the direction.
		 */
		if (!var_on_left)
			add = !add;

		/*
		 * The safety buffer is chosen to be 4 hours because daylight saving time
		 * changes seem to be in the range between -1 and 2 hours.
		 */
		if (add)
			constified_tstz += 4 * USECS_PER_HOUR;
		else
			constified_tstz -= 4 * USECS_PER_HOUR;

		constified = TimestampTzGetDatum(constified_tstz);
	}

	c_ts = copyObject(c_ts);
	c_ts->constvalue = constified;

	if (var_on_left)
		right = (Expr *) c_ts;
	else
		left = (Expr *) c_ts;

	return (OpExpr *) make_opclause(constraint->opno,
									constraint->opresulttype,
									constraint->opretset,
									left,
									right,
									constraint->opcollid,
									constraint->inputcollid);
}

/*
 * Transform time_bucket calls of the following form in WHERE clause:
 *
 * time_bucket(width, column) OP value
 *
 * Since time_bucket always returns the lower bound of the bucket
 * for lower bound comparisons the width is not relevant and the
 * following transformation can be applied:
 *
 * time_bucket(width, column) > value
 * column > value
 *
 * Example with values:
 *
 * time_bucket(10, column) > 109
 * column > 109
 *
 * For upper bound comparisons width needs to be taken into account
 * and we need to extend the upper bound by width to capture all
 * possible values.
 *
 * time_bucket(width, column) < value
 * column < value + width
 *
 * Example with values:
 *
 * time_bucket(10, column) < 100
 * column < 100 + 10
 *
 * Expressions with value on the left side will be switched around
 * when building the expression for RestrictInfo.
 *
 * Caller must ensure that only 2 argument time_bucket versions
 * are used.
 */
static OpExpr *
transform_time_bucket_comparison(PlannerInfo *root, OpExpr *op)
{
	Expr *left = linitial(op->args);
	Expr *right = lsecond(op->args);

	FuncExpr *time_bucket = castNode(FuncExpr, (IsA(left, FuncExpr) ? left : right));
	Expr *value = IsA(right, Const) ? right : left;

	Const *width = linitial(time_bucket->args);
	Oid opno = op->opno;
	TypeCacheEntry *tce;
	int strategy;

	if (list_length(time_bucket->args) != 2 || !IsA(value, Const) || !IsA(width, Const))
		return op;

	/*
	 * if time_bucket call is on wrong side we switch operator
	 */
	if (IsA(right, FuncExpr))
	{
		opno = get_commutator(op->opno);

		if (!OidIsValid(opno))
			return op;
	}

	tce = lookup_type_cache(exprType((Node *) time_bucket), TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

	if (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)
	{
		/* column > value */
		op = copyObject(op);
		op->args = list_make2(lsecond(time_bucket->args), value);

		/*
		 * if we switched operator we need to adjust OpExpr as well
		 */
		if (IsA(right, FuncExpr))
		{
			op->opno = opno;
			op->opfuncid = InvalidOid;
		}

		return op;
	}
	else if (strategy == BTLessStrategyNumber || strategy == BTLessEqualStrategyNumber)
	{
		/* column < value + width */
		Expr *subst;
		Datum datum;
		int64 integralValue, integralWidth;

		if (castNode(Const, value)->constisnull || width->constisnull)
			return op;

		switch (tce->type_id)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
				integralValue = const_datum_get_int(castNode(Const, value));
				integralWidth = const_datum_get_int(width);

				if (integralValue >= ts_time_get_max(tce->type_id) - integralWidth)
					return op;

				/*
				 * When the time_bucket constraint matches the start of the bucket
				 * and we have a less than constraint we can skip adding the
				 * full bucket.
				 */
				if (strategy == BTLessStrategyNumber && integralValue % integralWidth == 0)
					datum = int_get_datum(integralValue, tce->type_id);
				else
					datum = int_get_datum(integralValue + integralWidth, tce->type_id);

				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);
				break;

			case DATEOID:
			{
				Assert(width->consttype == INTERVALOID);
				Interval *interval = DatumGetIntervalP(width->constvalue);

				/*
				 * Optimization can't be applied when interval has month component.
				 */
				if (interval->month != 0)
					return op;

				/* bail out if interval->time can't be exactly represented as a double */
				if (interval->time >= 0x3FFFFFFFFFFFFFll)
					return op;

				integralValue = const_datum_get_int(castNode(Const, value));
				integralWidth =
					interval->day + ceil((double) interval->time / (double) USECS_PER_DAY);

				if (integralValue >= (TS_DATE_END - integralWidth))
					return op;

				/*
				 * When the time_bucket constraint matches the start of the bucket
				 * and we have a less than constraint we can skip adding the
				 * full bucket.
				 */
				if (strategy == BTLessStrategyNumber && integralValue % integralWidth == 0)
					datum = DateADTGetDatum(integralValue);
				else
					datum = DateADTGetDatum(integralValue + integralWidth);

				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			}
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			{
				Assert(width->consttype == INTERVALOID);
				Interval *interval = DatumGetIntervalP(width->constvalue);

				/*
				 * Optimization can't be applied when interval has month component.
				 */
				if (interval->month != 0)
					return op;

				/*
				 * If width interval has day component we merge it with time component
				 */

				integralWidth = interval->time;
				if (interval->day != 0)
				{
					/*
					 * if our transformed restriction would overflow we skip adding it
					 */
					if (interval->time >= TS_TIMESTAMP_END - interval->day * USECS_PER_DAY)
						return op;

					integralWidth += interval->day * USECS_PER_DAY;
				}

				integralValue = const_datum_get_int(castNode(Const, value));

				if (integralValue >= (TS_TIMESTAMP_END - integralWidth))
					return op;

				/*
				 * When the time_bucket constraint matches the start of the bucket
				 * and we have a less than constraint we can skip adding the
				 * full bucket.
				 */
				if (strategy == BTLessStrategyNumber && integralValue % integralWidth == 0)
					datum = int_get_datum(integralValue, tce->type_id);
				else
					datum = int_get_datum(integralValue + integralWidth, tce->type_id);

				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			}

			default:
				return op;
				break;
		}

		/*
		 * adjust toplevel expression if datatypes changed
		 * this can happen when comparing int4 values against int8 time_bucket
		 */
		if (tce->type_id != castNode(Const, value)->consttype)
		{
			opno =
				ts_get_operator(get_opname(opno), PG_CATALOG_NAMESPACE, tce->type_id, tce->type_id);

			if (!OidIsValid(opno))
				return op;
		}

		op = copyObject(op);

		/*
		 * if we changed operator we need to adjust OpExpr as well
		 */
		if (op->opno != opno)
		{
			op->opno = opno;
			op->opfuncid = get_opcode(opno);
		}

		op->args = list_make2(lsecond(time_bucket->args), subst);
	}

	return op;
}

/* Since baserestrictinfo is not yet set by the planner, we have to derive
 * it ourselves. It's safe for us to miss some restrict info clauses (this
 * will just result in more chunks being included) so this does not need
 * to be as comprehensive as the PG native derivation. This is inspired
 * by the derivation in `deconstruct_recurse` in PG
 *
 * When we detect explicit chunk exclusion with the chunks_in function
 * we stop further processing and do an early exit.
 *
 * This function removes chunks_in from the list of quals, because chunks_in is
 * just used as marker function to trigger explicit chunk exclusion and the function
 * will throw an error when executed.
 */
static Node *
process_quals(Node *quals, CollectQualCtx *ctx, bool is_outer_join)
{
	ListCell *lc;

	ListCell *prev pg_attribute_unused() = NULL;
	List *additional_quals = NIL;

	for (lc = list_head((List *) quals); lc != NULL; prev = lc, lc = lnext((List *) quals, lc))
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos_compat(ctx->root, (Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		if (is_chunk_exclusion_func(qual))
		{
			FuncExpr *func_expr = (FuncExpr *) qual;

			/* validation */
			Assert(func_expr->args->length == 2);
			if (!IsA(linitial(func_expr->args), Var))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("first parameter for chunks_in function needs to be record")));

			ctx->chunk_exclusion_func = func_expr;
			ctx->restrictions = NIL;
			return quals;
		}

		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			/*
			 * check for constraints with TIMESTAMPTZ OP INTERVAL calculations
			 */
			if ((IsA(left, Var) && is_timestamptz_op_interval(right)) ||
				(IsA(right, Var) && is_timestamptz_op_interval(left)))
				qual = (Expr *) constify_timestamptz_op_interval(ctx->root, op);

			/*
			 * check for time_bucket comparisons
			 * time_bucket(Const, time_colum) > Const
			 */
			if ((IsA(left, FuncExpr) && IsA(right, Const) &&
				 list_length(castNode(FuncExpr, left)->args) == 2 &&
				 is_time_bucket_function(left)) ||
				(IsA(left, Const) && IsA(right, FuncExpr) &&
				 list_length(castNode(FuncExpr, right)->args) == 2 &&
				 is_time_bucket_function(right)))
			{
				qual = (Expr *) transform_time_bucket_comparison(ctx->root, op);
				/*
				 * if we could transform the expression we add it to the list of
				 * quals so it can be used as an index condition
				 */
				if (qual != (Expr *) op)
					additional_quals = lappend(additional_quals, qual);
			}
		}

		/* Do not include this restriction if this is an outer join. Including
		 * the restriction would exclude chunks and thus rows of the outer
		 * relation when it should show all rows */
		if (!is_outer_join)
			ctx->restrictions =
				lappend(ctx->restrictions, make_simple_restrictinfo_compat(ctx->root, qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

static List *
remove_exclusion_fns(List *restrictinfo)
{
	ListCell *lc = list_head(restrictinfo);

	while (lc != NULL)
	{
		RestrictInfo *rinfo = lfirst(lc);
		Expr *qual = rinfo->clause;

		if (is_chunk_exclusion_func(qual))
		{
			FuncExpr *func_expr = (FuncExpr *) qual;

			/* validation */
			Assert(func_expr->args->length == 2);
			if (!IsA(linitial(func_expr->args), Var))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("first parameter for chunks_in function needs to be record")));

			restrictinfo = list_delete_cell(restrictinfo, lc);
			return restrictinfo;
		}
		lc = lnext(restrictinfo, lc);
	}
	return restrictinfo;
}

static Node *
timebucket_annotate(Node *quals, CollectQualCtx *ctx)
{
	ListCell *lc;
	List *additional_quals = NIL;

	foreach (lc, castNode(List, quals))
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos_compat(ctx->root, (Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			/*
			 * check for time_bucket comparisons
			 * time_bucket(Const, time_colum) > Const
			 */
			if ((IsA(left, FuncExpr) && IsA(right, Const) &&
				 list_length(castNode(FuncExpr, left)->args) == 2 &&
				 is_time_bucket_function(left)) ||
				(IsA(left, Const) && IsA(right, FuncExpr) &&
				 list_length(castNode(FuncExpr, right)->args) == 2 &&
				 is_time_bucket_function(right)))
			{
				qual = (Expr *) transform_time_bucket_comparison(ctx->root, op);
				/*
				 * if we could transform the expression we add it to the list of
				 * quals so it can be used as an index condition
				 */
				if (qual != (Expr *) op)
					additional_quals = lappend(additional_quals, qual);
			}
		}

		ctx->restrictions =
			lappend(ctx->restrictions, make_simple_restrictinfo_compat(ctx->root, qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

/*
 * collect JOIN information
 *
 * This function adds information to two lists in the CollectQualCtx
 *
 * join_conditions
 *
 * This list contains all equality join conditions and is used by
 * ChunkAppend to decide whether the ordered append optimization
 * can be applied.
 *
 * propagate_conditions
 *
 * This list contains toplevel or INNER JOIN equality conditions.
 * This list is used for propagating quals to the other side of
 * a JOIN.
 */
static void
collect_join_quals(Node *quals, CollectQualCtx *ctx, bool can_propagate)
{
	ListCell *lc;

	foreach (lc, (List *) quals)
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos_compat(ctx->root, (Node *) qual);
		int num_rels = bms_num_members(relids);

		/*
		 * collect quals to propagate to join relations
		 */
		if (num_rels == 1 && can_propagate && IsA(qual, OpExpr) &&
			list_length(castNode(OpExpr, qual)->args) == 2)
			ctx->all_quals = lappend(ctx->all_quals, qual);

		if (!bms_is_member(ctx->rel->relid, relids))
			continue;

		/* collect equality JOIN conditions for current rel */
		if (num_rels == 2 && IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			if (IsA(left, Var) && IsA(right, Var))
			{
				Var *ht_var =
					castNode(Var,
							 (Index) castNode(Var, left)->varno == ctx->rel->relid ? left : right);
				TypeCacheEntry *tce = lookup_type_cache(ht_var->vartype, TYPECACHE_EQ_OPR);

				if (op->opno == tce->eq_opr)
				{
					ctx->join_conditions = lappend(ctx->join_conditions, op);

					if (can_propagate)
						ctx->propagate_conditions = lappend(ctx->propagate_conditions, op);
				}
			}
			continue;
		}
	}
}

static bool
collect_quals_walker(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		FromExpr *f = castNode(FromExpr, node);
		f->quals = process_quals(f->quals, ctx, false);
		/* if this is a nested join we don't propagate join quals */
		collect_join_quals(f->quals, ctx, ctx->join_level == 0);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *j = castNode(JoinExpr, node);
		j->quals = process_quals(j->quals, ctx, IS_OUTER_JOIN(j->jointype));
		collect_join_quals(j->quals, ctx, ctx->join_level == 0 && !IS_OUTER_JOIN(j->jointype));

		if (IS_OUTER_JOIN(j->jointype))
		{
			ctx->join_level++;
			bool result = expression_tree_walker(node, collect_quals_walker, ctx);
			ctx->join_level--;
			return result;
		}
	}

	/* skip processing if we found a chunks_in call for current relation */
	if (ctx->chunk_exclusion_func != NULL)
		return true;

	return expression_tree_walker(node, collect_quals_walker, ctx);
}

static int
chunk_cmp_chunk_reloid(const void *c1, const void *c2)
{
	return (*(Chunk **) c1)->table_id - (*(Chunk **) c2)->table_id;
}

static Chunk **
find_children_chunks(HypertableRestrictInfo *hri, Hypertable *ht, unsigned int *num_chunks)
{
	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		/*
		 * Chunk lookup doesn't work for internal compression tables, have to
		 * fall back to the regular postgres method.
		 */
		List *chunk_oids = find_inheritance_children(ht->main_table_relid, AccessShareLock);
		if (chunk_oids == NIL)
		{
			*num_chunks = 0;
			return 0;
		}

		*num_chunks = list_length(chunk_oids);
		Chunk **chunks = (Chunk **) palloc(sizeof(Chunk *) * *num_chunks);

		for (unsigned int i = 0; i < *num_chunks; i++)
		{
			chunks[i] = ts_chunk_get_by_relid(list_nth_oid(chunk_oids, i),
											  /* fail_if_not_found = */ true);
		}

		return chunks;
	}

	/*
	 * Unlike find_all_inheritors we do not include parent because if there
	 * are restrictions the parent table cannot fulfill them and since we do
	 * have a trigger blocking inserts on the parent table it cannot contain
	 * any rows.
	 */
	Chunk **chunks = ts_hypertable_restrict_info_get_chunks(hri, ht, num_chunks);

	/*
	 * Sort the chunks by oid ascending to roughly match the order provided
	 * by find_inheritance_children. This is mostly needed to avoid test
	 * reference changes.
	 */
	qsort(chunks, *num_chunks, sizeof(Chunk *), chunk_cmp_chunk_reloid);

	return chunks;
}

static bool
should_order_append(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, List *join_conditions,
					int *order_attno, bool *reverse)
{
	/* check if optimizations are enabled */
	if (!ts_guc_enable_optimizations || !ts_guc_enable_ordered_append ||
		!ts_guc_enable_chunk_append)
		return false;

	/*
	 * only do this optimization for hypertables with 1 dimension and queries
	 * with an ORDER BY clause
	 */
	if (root->parse->sortClause == NIL)
		return false;

	return ts_ordered_append_should_optimize(root, rel, ht, join_conditions, order_attno, reverse);
}

/*
 *  get chunk oids specified by explicit chunk exclusion function
 *
 *  Similar to the regular get_chunk_oids, we also populate the fdw_private
 *  structure appropriately if ordering info is present.
 */
static Chunk **
get_explicit_chunks(CollectQualCtx *ctx, PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
					unsigned int *num_chunks)
{
	Const *chunks_arg;
	ArrayIterator chunk_id_iterator;
	ArrayType *chunk_id_arr;
	unsigned int chunk_id_arr_size;
	Datum elem = (Datum) NULL;
	bool isnull;
	Expr *expr;
	bool reverse;
	int order_attno;
	Chunk **unlocked_chunks = NULL;
	Chunk **chunks = NULL;
	int unlocked_chunk_count = 0;
	Oid prev_chunk_oid = InvalidOid;
	bool chunk_sort_needed = false;
	int i;

	*num_chunks = 0;

	Assert(ctx->chunk_exclusion_func->args->length == 2);
	expr = lsecond(ctx->chunk_exclusion_func->args);
	if (!IsA(expr, Const))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("second argument to chunk_in should contain only integer consts")));

	chunks_arg = (Const *) expr;

	/* function marked as STRICT so argument can't be NULL */
	Assert(!chunks_arg->constisnull);

	chunk_id_arr = DatumGetArrayTypeP(chunks_arg->constvalue);
	if (ARR_NDIM(chunk_id_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number of array dimensions for chunks_in")));

	chunk_id_arr_size = ArrayGetNItems(ARR_NDIM(chunk_id_arr), ARR_DIMS(chunk_id_arr));

	if (chunk_id_arr_size == 0)
		return NULL;

	/* allocate an array of "Chunk *" and set it up below */
	unlocked_chunks = (Chunk **) palloc(sizeof(Chunk *) * chunk_id_arr_size);

	chunk_id_iterator = array_create_iterator(chunk_id_arr, 0, NULL);
	while (array_iterate(chunk_id_iterator, &elem, &isnull))
	{
		if (!isnull)
		{
			int32 chunk_id = DatumGetInt32(elem);
			Chunk *chunk = ts_chunk_get_by_id(chunk_id, false);

			if (chunk == NULL)
				ereport(ERROR, (errmsg("chunk id %d not found", chunk_id)));

			if (chunk->fd.hypertable_id != ht->fd.id)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("chunk id %d does not belong to hypertable \"%s\"",
								chunk_id,
								NameStr(ht->fd.table_name))));

			if (OidIsValid(prev_chunk_oid) && prev_chunk_oid > chunk->table_id)
				chunk_sort_needed = true;

			prev_chunk_oid = chunk->table_id;
			unlocked_chunks[unlocked_chunk_count++] = chunk;
		}
		else
			elog(ERROR, "chunk id can't be NULL");
	}
	array_free_iterator(chunk_id_iterator);

	/*
	 * Sort chunks if needed for locking in Oid order in order to avoid
	 * deadlocks. In most cases, the access node sends the chunk ID array in
	 * Oid order, so no sorting is needed. (Note that chunk ID and Oid are
	 * different, but often result in the same order.)
	 */
	if (unlocked_chunk_count > 1 && chunk_sort_needed)
		qsort(unlocked_chunks, unlocked_chunk_count, sizeof(Chunk *), ts_chunk_oid_cmp);

	chunks = palloc(sizeof(Chunk *) * unlocked_chunk_count);

	for (i = 0; i < unlocked_chunk_count; i++)
	{
		if (ts_chunk_lock_if_exists(unlocked_chunks[i]->table_id, AccessShareLock))
			chunks[(*num_chunks)++] = unlocked_chunks[i];
	}

	pfree(unlocked_chunks);

	/*
	 * Chunks could have been concurrently removed or locking was not
	 * successful. If no chunks could be locked, then return.
	 */
	if (*num_chunks == 0)
	{
		pfree(chunks);
		return NULL;
	}

	/*
	 * If fdw_private has not been setup by caller there is no point checking
	 * for ordered append as we can't pass the required metadata in fdw_private
	 * to signal that this is safe to transform in ordered append plan in
	 * set_rel_pathlist.
	 */
	if (rel->fdw_private != NULL &&
		should_order_append(root, rel, ht, ctx->join_conditions, &order_attno, &reverse))
	{
		TimescaleDBPrivate *priv = ts_get_private_reloptinfo(rel);
		List **nested_oids = NULL;

		priv->appends_ordered = true;
		priv->order_attno = order_attno;

		/*
		 * for space partitioning we need extra information about the
		 * time slices of the chunks
		 */
		if (ht->space->num_dimensions > 1)
			nested_oids = &priv->nested_oids;

		/* we don't need "hri" here since we already have the chunks */
		return ts_hypertable_restrict_info_get_chunks_ordered(NULL,
															  ht,
															  chunks,
															  reverse,
															  nested_oids,
															  num_chunks);
	}

	return chunks;
}

/**
 * Get chunks from either restrict info or explicit chunk exclusion. Explicit chunk exclusion
 * takes precedence.
 *
 * If appends are returned in order appends_ordered on rel->fdw_private is set to true.
 * To make verifying pathkeys easier in set_rel_pathlist the attno of the column ordered by
 * is
 * If the hypertable uses space partitioning the nested oids are stored in nested_oids
 * on rel->fdw_private when appends are ordered.
 */
static Chunk **
get_chunks(CollectQualCtx *ctx, PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
		   unsigned int *num_chunks)
{
	bool reverse;
	int order_attno;

	if (ctx->chunk_exclusion_func != NULL)
	{
		return get_explicit_chunks(ctx, root, rel, ht, num_chunks);
	}

	HypertableRestrictInfo *hri = ts_hypertable_restrict_info_create(rel, ht);

	/*
	 * This is where the magic happens: use our HypertableRestrictInfo
	 * infrastructure to deduce the appropriate chunks using our range
	 * exclusion
	 */
	ts_hypertable_restrict_info_add(hri, root, ctx->restrictions);

	/*
	 * If fdw_private has not been setup by caller there is no point checking
	 * for ordered append as we can't pass the required metadata in fdw_private
	 * to signal that this is safe to transform in ordered append plan in
	 * set_rel_pathlist.
	 */
	if (rel->fdw_private != NULL &&
		should_order_append(root, rel, ht, ctx->join_conditions, &order_attno, &reverse))
	{
		TimescaleDBPrivate *priv = ts_get_private_reloptinfo(rel);
		List **nested_oids = NULL;

		priv->appends_ordered = true;
		priv->order_attno = order_attno;

		/*
		 * for space partitioning we need extra information about the
		 * time slices of the chunks
		 */
		if (ht->space->num_dimensions > 1)
			nested_oids = &priv->nested_oids;

		return ts_hypertable_restrict_info_get_chunks_ordered(hri,
															  ht,
															  NULL,
															  reverse,
															  nested_oids,
															  num_chunks);
	}

	return find_children_chunks(hri, ht, num_chunks);
}

/*
 * Create partition expressions for a hypertable.
 *
 * Build an array of partition expressions where each element represents valid
 * expressions on a particular partitioning key.
 *
 * The partition expressions are used by, e.g., group_by_has_partkey() to check
 * whether a GROUP BY clause covers all partitioning dimensions.
 *
 * For dimensions with a partitioning function, we can support either
 * expressions on the plain key (column) or the partitioning function applied
 * to the key. For instance, the queries
 *
 * SELECT time, device, avg(temp)
 * FROM hypertable
 * GROUP BY 1, 2;
 *
 * and
 *
 * SELECT time_func(time), device, avg(temp)
 * FROM hypertable
 * GROUP BY 1, 2;
 *
 * are both amenable to aggregate push down if "time" is supported by the
 * partitioning function "time_func" and "device" is also a partitioning
 * dimension.
 */
static List **
get_hypertable_partexprs(Hypertable *ht, Query *parse, Index varno)
{
	int i;
	List **partexprs;

	Assert(NULL != ht->space);

	partexprs = palloc0(sizeof(List *) * ht->space->num_dimensions);

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];

		partexprs[i] = ts_dimension_get_partexprs(dim, varno);
	}

	return partexprs;
}

#define PARTITION_STRATEGY_MULTIDIM 'm'

/*
 * Partition info for hypertables.
 *
 * Build a "fake" partition scheme for a hypertable that makes the planner
 * believe this is a PostgreSQL partitioned table for planning purposes. In
 * particular, this will make the planner consider partitionwise aggregations
 * when applicable.
 *
 * Partitionwise aggregation can either be FULL or PARTIAL. The former means
 * that the aggregation can be performed independently on each partition
 * (chunk) without a finalize step which is needed in PARTIAL. FULL requires
 * that the GROUP BY clause contains all hypertable partitioning
 * dimensions. This requirement is enforced by creating a partitioning scheme
 * that covers multiple attributes, i.e., one per dimension. This works well
 * since the "shallow" (one-level hierarchy) of a multi-dimensional hypertable
 * is similar to a one-level partitioned PostgreSQL table where the
 * partitioning key covers multiple attributes.
 *
 * Note that we use a partition scheme with a strategy that does not exist in
 * PostgreSQL. This makes PostgreSQL raise errors when this partition scheme is
 * used in places that require a valid partition scheme with a supported
 * strategy.
 */
static void
build_hypertable_partition_info(Hypertable *ht, PlannerInfo *root, RelOptInfo *hyper_rel,
								int nparts)
{
	PartitionScheme part_scheme = palloc0(sizeof(PartitionSchemeData));
	PartitionBoundInfo boundinfo = palloc0(sizeof(PartitionBoundInfoData));

	/* We only set the info needed for planning */
	part_scheme->partnatts = ht->space->num_dimensions;
	part_scheme->strategy = PARTITION_STRATEGY_MULTIDIM;
	hyper_rel->nparts = nparts;
	part_scheme->partopfamily = palloc0(part_scheme->partnatts * sizeof(Oid));
	part_scheme->partopcintype = palloc0(part_scheme->partnatts * sizeof(Oid));
	part_scheme->partcollation = palloc0(part_scheme->partnatts * sizeof(Oid));
	hyper_rel->part_scheme = part_scheme;
	hyper_rel->partexprs = get_hypertable_partexprs(ht, root->parse, hyper_rel->relid);
	hyper_rel->nullable_partexprs = (List **) palloc0(sizeof(List *) * part_scheme->partnatts);

	/* PartitionBoundInfo is used for ordered append. We use a strategy that
	 * will avoid triggering an ordered append. */
	boundinfo->strategy = PARTITION_STRATEGY_MULTIDIM;
	boundinfo->default_index = -1;
	boundinfo->null_index = -1;

	hyper_rel->boundinfo = boundinfo;
	hyper_rel->part_rels = palloc0(sizeof(*hyper_rel->part_rels) * nparts);
}

static bool
timebucket_annotate_walker(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		FromExpr *f = castNode(FromExpr, node);
		f->quals = timebucket_annotate(f->quals, ctx);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *j = castNode(JoinExpr, node);
		j->quals = timebucket_annotate(j->quals, ctx);
	}

	/* skip processing if we found a chunks_in call for current relation */
	if (ctx->chunk_exclusion_func != NULL)
		return true;

	return expression_tree_walker(node, timebucket_annotate_walker, ctx);
}

void
ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel)
{
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.chunk_exclusion_func = NULL,
		.all_quals = NIL,
		.join_conditions = NIL,
		.propagate_conditions = NIL,
	};

	init_chunk_exclusion_func();

	/* Walk the tree and find restrictions or chunk exclusion functions */
	timebucket_annotate_walker((Node *) root->parse->jointree, &ctx);

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);
}

/* Inspired by expand_inherited_rtentry but expands
 * a hypertable chunks into an append relation. */
void
ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel)
{
	TimescaleDBPrivate *priv = rel->fdw_private;
	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
	Oid parent_oid = rte->relid;
	List *inh_oids = NIL;
	ListCell *l;
	Relation oldrelation;
	Query *parse = root->parse;
	Index rti = rel->relid;
	List *appinfos = NIL;
	PlanRowMark *oldrc;
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.chunk_exclusion_func = NULL,
		.all_quals = NIL,
		.join_conditions = NIL,
		.propagate_conditions = NIL,
		.join_level = 0,
	};
	Index first_chunk_index = 0;

	/* double check our permissions are valid */
	Assert(rti != (Index) parse->resultRelation);

	oldrc = get_plan_rowmark(root->rowMarks, rti);

	if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType))
		elog(ERROR, "unexpected permissions requested");

	init_chunk_exclusion_func();

	/* Walk the tree and find restrictions or chunk exclusion functions */
	collect_quals_walker((Node *) root->parse->jointree, &ctx);
	/* check join_level bookkeeping is balanced */
	Assert(ctx.join_level == 0);

	rel->baserestrictinfo = remove_exclusion_fns(rel->baserestrictinfo);

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);

	Chunk **chunks = NULL;
	unsigned int num_chunks = 0;
	chunks = get_chunks(&ctx, root, rel, ht, &num_chunks);
	/* Can have zero chunks. */
	Assert(num_chunks == 0 || chunks != NULL);

	for (unsigned int i = 0; i < num_chunks; i++)
	{
		inh_oids = lappend_oid(inh_oids, chunks[i]->table_id);

		/*
		 * Add the information about chunks to the baserel info cache for
		 * classify_relation().
		 */
		ts_add_baserel_cache_entry_for_chunk(chunks[i]->table_id, ht);
	}

	/* nothing to do here if we have no chunks and no data nodes */
	if (list_length(inh_oids) + list_length(ht->data_nodes) == 0)
		return;

	oldrelation = table_open(parent_oid, NoLock);

	/*
	 * the simple_*_array structures have already been set, we need to add the
	 * children to them. We include potential data node rels we might need to
	 * create in case of a distributed hypertable.
	 */
	expand_planner_arrays(root, list_length(inh_oids) + list_length(ht->data_nodes));

	/* Adding partition info will make PostgreSQL consider the inheritance
	 * children as part of a partitioned relation. This will enable
	 * partitionwise aggregation. */
	if ((enable_partitionwise_aggregate &&
		 !has_partialize_function((Node *) root->parse->targetList, TS_DO_NOT_FIX_AGGSPLIT)) ||
		hypertable_is_distributed(ht))
	{
		build_hypertable_partition_info(ht, root, rel, list_length(inh_oids));
	}

	foreach (l, inh_oids)
	{
		Oid child_oid = lfirst_oid(l);
		Relation newrelation;
		RangeTblEntry *childrte;
		Index child_rtindex;
		AppendRelInfo *appinfo;
		LOCKMODE chunk_lock = rte->rellockmode;

		/* Open rel if needed */

		if (child_oid != parent_oid)
			newrelation = table_open(child_oid, chunk_lock);
		else
			newrelation = oldrelation;

		/* chunks cannot be temp tables */
		Assert(!RELATION_IS_OTHER_TEMP(newrelation));

		/*
		 * Build an RTE for the child, and attach to query's rangetable list.
		 * We copy most fields of the parent's RTE, but replace relation OID
		 * and relkind, and set inh = false.  Also, set requiredPerms to zero
		 * since all required permissions checks are done on the original RTE.
		 * Likewise, set the child's securityQuals to empty, because we only
		 * want to apply the parent's RLS conditions regardless of what RLS
		 * properties individual children may have.  (This is an intentional
		 * choice to make inherited RLS work like regular permissions checks.)
		 * The parent securityQuals will be propagated to children along with
		 * other base restriction clauses, so we don't need to do it here.
		 */
		childrte = copyObject(rte);
		childrte->relid = child_oid;
		childrte->relkind = newrelation->rd_rel->relkind;
		childrte->inh = false;
		/* clear the magic bit */
		childrte->ctename = NULL;
#if PG16_LT
		childrte->requiredPerms = 0;
#else
		/* Since PG16, the permission info is maintained separetely. Unlink
		 * the old perminfo from the RTE to disable permission checking.
		 */
		childrte->perminfoindex = 0;
#endif
		childrte->securityQuals = NIL;
		parse->rtable = lappend(parse->rtable, childrte);
		child_rtindex = list_length(parse->rtable);
		if (first_chunk_index == 0)
			first_chunk_index = child_rtindex;
		root->simple_rte_array[child_rtindex] = childrte;
		Assert(root->simple_rel_array[child_rtindex] == NULL);

		appinfo = makeNode(AppendRelInfo);
		appinfo->parent_relid = rti;
		appinfo->child_relid = child_rtindex;
		appinfo->parent_reltype = oldrelation->rd_rel->reltype;
		appinfo->child_reltype = newrelation->rd_rel->reltype;
		ts_make_inh_translation_list(oldrelation,
									 newrelation,
									 child_rtindex,
									 &appinfo->translated_vars);
		appinfo->parent_reloid = parent_oid;
		appinfos = lappend(appinfos, appinfo);

		/* Close child relations, but keep locks */
		if (child_oid != parent_oid)
			table_close(newrelation, NoLock);
	}

	table_close(oldrelation, NoLock);

	priv->serverids = ts_hypertable_get_data_node_serverids_list(ht);

	/* For distributed hypertables, we'd like to turn per-chunk plans into
	 * per-data_node plans. We proactively add RTEs for the per-data_node rels here
	 * because the PostgreSQL planning code that we call to replan the
	 * per-data_node queries assumes there are RTEs for each rel that is considered
	 * a "partition."
	 *
	 * Note that each per-data_node RTE reuses the relid (OID) of the parent
	 * hypertable relation. This makes sense since each data node's
	 * hypertable is an identical (albeit partial) version of the access node's
	 * hypertable. The upside of this is that the planner can plan remote
	 * queries to take into account the indexes on the hypertable to produce
	 * more efficient remote queries. In contrast, chunks are foreign tables so
	 * they do not have indexes.
	 */
	foreach (l, priv->serverids)
	{
		RangeTblEntry *data_node_rte = copyObject(rte);

		data_node_rte->inh = false;
		data_node_rte->ctename = NULL;
		data_node_rte->securityQuals = NIL;
#if PG16_LT
		data_node_rte->requiredPerms = 0;
#else
		/* Since PG16, the permission info is maintained separetely. Unlink
		 * the old perminfo from the RTE to disable permission checking.
		 */
		data_node_rte->perminfoindex = 0;
#endif
		parse->rtable = lappend(parse->rtable, data_node_rte);
		rti = list_length(parse->rtable);
		root->simple_rte_array[rti] = data_node_rte;
		root->simple_rel_array[rti] = NULL;
		priv->server_relids = bms_add_member(priv->server_relids, rti);
	}

	ts_add_append_rel_infos(root, appinfos);

	/* PostgreSQL will not set up the child rels for use, due to the games
	 * we're playing with inheritance, so we must do it ourselves.
	 * build_simple_rel will look things up in the append_rel_array, so we can
	 * only use it after that array has been set up.
	 */
	for (int i = 0; i < list_length(inh_oids); i++)
	{
		Index child_rtindex = first_chunk_index + i;
		/* build_simple_rel will add the child to the relarray */
		RelOptInfo *child_rel = build_simple_rel(root, child_rtindex, rel);

		/* if we're performing partitionwise aggregation, we must populate part_rels */
		if (rel->part_rels != NULL)
		{
			rel->part_rels[i] = child_rel;
#if PG15_GE
			rel->live_parts = bms_add_member(rel->live_parts, i);
#endif
		}

		ts_get_private_reloptinfo(child_rel)->cached_chunk_struct = chunks[i];
		Assert(chunks[i]->table_id == root->simple_rte_array[child_rtindex]->relid);
	}
}

void
propagate_join_quals(PlannerInfo *root, RelOptInfo *rel, CollectQualCtx *ctx)
{
	ListCell *lc;

	if (!ts_guc_enable_qual_propagation)
		return;

	/* propagate join constraints */
	foreach (lc, ctx->propagate_conditions)
	{
		ListCell *lc_qual;
		OpExpr *op = lfirst(lc);
		Var *rel_var, *other_var;

		/*
		 * join_conditions only has OpExpr with 2 Var as arguments
		 * this is enforced in process_quals
		 */
		Assert(IsA(op, OpExpr) && list_length(castNode(OpExpr, op)->args) == 2);
		Assert(IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var));

		/*
		 * check this join condition refers to current hypertable
		 * our Var might be on either side of the expression
		 */
		if ((Index) linitial_node(Var, op->args)->varno == rel->relid)
		{
			rel_var = linitial_node(Var, op->args);
			other_var = lsecond_node(Var, op->args);
		}
		else if ((Index) lsecond_node(Var, op->args)->varno == rel->relid)
		{
			rel_var = lsecond_node(Var, op->args);
			other_var = linitial_node(Var, op->args);
		}
		else
			continue;

		foreach (lc_qual, ctx->all_quals)
		{
			OpExpr *qual = lfirst(lc_qual);
			Expr *left = linitial(qual->args);
			Expr *right = lsecond(qual->args);
			OpExpr *propagated;
			ListCell *lc_ri;
			bool new_qual = true;

			/*
			 * check this is Var OP Expr / Expr OP Var
			 * Var needs to reference the relid of the JOIN condition and
			 * Expr must not contain volatile functions
			 */
			if (IsA(left, Var) && castNode(Var, left)->varno == other_var->varno &&
				castNode(Var, left)->varattno == other_var->varattno && !IsA(right, Var) &&
				!contain_volatile_functions((Node *) right))
			{
				propagated = copyObject(qual);
				propagated->args = list_make2(rel_var, lsecond(propagated->args));
			}
			else if (IsA(right, Var) && castNode(Var, right)->varno == other_var->varno &&
					 castNode(Var, right)->varattno == other_var->varattno && !IsA(left, Var) &&
					 !contain_volatile_functions((Node *) left))
			{
				propagated = copyObject(qual);
				propagated->args = list_make2(linitial(propagated->args), rel_var);
			}
			else
				continue;

			/*
			 * check if this is a new qual
			 */
			foreach (lc_ri, ctx->restrictions)
			{
				if (equal(castNode(RestrictInfo, lfirst(lc_ri))->clause, propagated))
				{
					new_qual = false;
					break;
				}
			}

			if (new_qual)
			{
				Relids relids = pull_varnos_compat(ctx->root, (Node *) propagated);
				RestrictInfo *restrictinfo;

				restrictinfo = make_restrictinfo_compat(root,
														(Expr *) propagated,
														true,
														false,
														false,
														false,
														false,
														ctx->root->qual_security_level,
														relids,
														NULL,
														NULL,
														NULL);
				ctx->restrictions = lappend(ctx->restrictions, restrictinfo);
				/*
				 * since hypertable expansion happens later, the propagated
				 * constraints will not be pushed down to the actual scans but stay
				 * as join filter. So we add them either as join filter or to
				 * baserestrictinfo depending on whether they reference only
				 * the currently processed relation or multiple relations.
				 */
				if (bms_num_members(relids) == 1 && bms_is_member(rel->relid, relids))
				{
					if (!list_member(rel->baserestrictinfo, restrictinfo))
						rel->baserestrictinfo = lappend(rel->baserestrictinfo, restrictinfo);
				}
				else
				{
					root->parse->jointree->quals =
						(Node *) lappend((List *) root->parse->jointree->quals, propagated);
				}
			}
		}
	}
}
