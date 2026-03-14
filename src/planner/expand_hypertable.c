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
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_func.h>
#include <parser/parse_type.h>
#include <parser/parsetree.h>
#include <partitioning/partbounds.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/errcodes.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <utils/typcache.h>
#include <utils/uuid.h>

#include "compat/compat.h"
#include "annotations.h"
#include "chunk.h"
#include "cross_module_fn.h"
#include "guc.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "import/planner.h"
#include "nodes/chunk_append/chunk_append.h"
#include "planner.h"
#include "time_utils.h"
#include "uuid.h"

typedef struct CollectQualCtx
{
	PlannerInfo *root;
	RelOptInfo *rel;
	List *restrictions;
	List *propagate_conditions;
	List *all_quals;
	int join_level;
} CollectQualCtx;

static void propagate_join_quals(PlannerInfo *root, RelOptInfo *rel, CollectQualCtx *ctx);

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

static Const *
integral_timeval_to_const(int64 value, Oid type)
{
	bool typbyval = get_typbyval(type);

	switch (type)
	{
		case INT2OID:
			return makeConst(type, -1, InvalidOid, 2, Int16GetDatum(value), false, typbyval);
		case INT4OID:
			return makeConst(type, -1, InvalidOid, 4, Int32GetDatum(value), false, typbyval);
		case INT8OID:
			return makeConst(type, -1, InvalidOid, 8, Int64GetDatum(value), false, typbyval);
		case DATEOID:
			return makeConst(type,
							 -1,
							 InvalidOid,
							 sizeof(DateADT),
							 DateADTGetDatum(value),
							 false,
							 typbyval);
		case TIMESTAMPOID:
			return makeConst(type,
							 -1,
							 InvalidOid,
							 sizeof(Timestamp),
							 TimestampGetDatum(value),
							 false,
							 typbyval);
		case TIMESTAMPTZOID:
			return makeConst(type,
							 -1,
							 InvalidOid,
							 sizeof(TimestampTz),
							 TimestampTzGetDatum(value),
							 false,
							 typbyval);
		case UUIDOID:
		{
			/*
			 * UUIDv7 doesn't support timestamps smaller than the UNIX epoch. However, caggs often
			 * refresh from "beginning of time" so we need to restrict the lower boundary value to
			 * the UNIX epoch.
			 */
			if (value <= UNIX_EPOCH_AS_TIMESTAMP)
				value = UNIX_EPOCH_AS_TIMESTAMP;

			pg_uuid_t *uuid = ts_create_uuid_v7_from_timestamptz((TimestampTz) value, true);
			return makeConst(type, -1, InvalidOid, UUID_LEN, UUIDPGetDatum(uuid), false, typbyval);
		}
		default:
			elog(ERROR, "unsupported datatype in %s: %s", __func__, format_type_be(type));
			pg_unreachable();
	}
}

static int64
const_to_integral_timeval(const Const *cnst)
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
		case UUIDOID:
		{
			/*
			 * While it is possible to extract the timestamp from a UUID, there is currently no use
			 * case where this function is used since the UUID-based time_bucket() function returns
			 * a timestamptz. Thus, any value compared to is also a timestamptz and not a UUID.
			 * */
			TS_FALLTHROUGH;
		}
		default:
			elog(ERROR,
				 "unsupported datatype in %s: %s",
				 __func__,
				 format_type_be(cnst->consttype));
			pg_unreachable();
	}
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

typedef struct TimeBucketInfo
{
	Oid rettype;	/* Type of the return value */
	Const *width;	/* Bucket width */
	Node *timeval;	/* Bucket "time" value */
	Oid timetype;	/* Type of the time value */
	uint16 numargs; /* Number of bucket function arguments */
} TimeBucketInfo;

/*
 * Representation of a parse time bucket Qual:
 *
 *  <time_bucket() OP value>
 */
typedef struct TimeBucketQual
{
	TimeBucketInfo tb;
	int strategy;
	Const *value;
} TimeBucketQual;

/*
 * Parse an expression of form <time_bucket(width, column) OP value> and extract the important
 * components into a TimeBucketQual struct.
 *
 * Returns false if the expression does not fit the expected format.
 */
static bool
extract_time_bucket_qual(Expr *node, TimeBucketQual *tbqual)
{
	if (!IsA(node, OpExpr))
		return false;

	OpExpr *op = castNode(OpExpr, node);

	if (list_length((op)->args) != 2)
		return false;

	Expr *left = linitial((op)->args);
	Expr *right = lsecond((op)->args);
	FuncExpr *time_bucket;

	MemSet(tbqual, 0, sizeof(TimeBucketQual));

	Oid opno = InvalidOid;

	if (IsA(left, FuncExpr) && IsA(right, Const))
	{
		time_bucket = castNode(FuncExpr, left);
		tbqual->value = castNode(Const, right);
		opno = op->opno;
	}
	else if (IsA(right, FuncExpr) && IsA(left, Const))
	{
		time_bucket = castNode(FuncExpr, right);
		tbqual->value = castNode(Const, left);
		opno = get_commutator(op->opno);
	}
	else
	{
		return false;
	}

	if (!is_time_bucket_function((Expr *) time_bucket) || tbqual->value->constisnull)
		return false;

	Const *width = linitial(time_bucket->args);
	/* Get the time/partitioning column argument */
	Node *timearg = lsecond(time_bucket->args);

	if (!IsA(width, Const) || width->constisnull)
		return false;

	tbqual->tb.numargs = list_length(time_bucket->args);
	tbqual->tb.width = width;
	tbqual->tb.timeval = timearg;
	tbqual->tb.timetype = exprType(timearg);
	tbqual->tb.rettype = exprType((Node *) time_bucket);

	/* 3 or more args should have Const 3rd arg */
	if (list_length(time_bucket->args) > 2 && !IsA(lthird(time_bucket->args), Const))
		return false;

	/* 5 args variants should have Const 4th and 5th arg */
	if (list_length(time_bucket->args) == 5 &&
		(!IsA(lfourth(time_bucket->args), Const) || !IsA(lfifth(time_bucket->args), Const)))
		return false;

	Assert(list_length(time_bucket->args) == 2 || list_length(time_bucket->args) == 3 ||
		   list_length(time_bucket->args) == 5);

	TypeCacheEntry *tce = lookup_type_cache(tbqual->tb.rettype, TYPECACHE_BTREE_OPFAMILY);
	tbqual->strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

	return true;
}

/*
 * Convert at time_bucket() width argument (typically Interval or integer) to a microseconds
 * integer. Also check that the width (interval) doesn't overflow the time value.
 */
static bool
time_bucket_width_to_integral(const Const *width, Oid bucket_type, int64 integral_value,
							  int64 *integral_width)
{
	switch (width->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:

			/* We can support the offset variants of time_bucket as the
			 * amount of shifting they do is never bigger than the bucketing
			 * width.
			 */
			*integral_width = const_to_integral_timeval(width);

			if (integral_value >= ts_time_get_max(bucket_type) - *integral_width)
				return false;
			break;
		case INTERVALOID:
		{
			Interval *interval = DatumGetIntervalP(width->constvalue);
			/*
			 * Optimization can't be applied when interval has month component.
			 */
			if (interval->month != 0)
				return false;

			if (bucket_type == DATEOID)
			{
				/* bail out if interval->time can't be exactly represented as a double */
				if (interval->time >= 0x3FFFFFFFFFFFFFLL)
					return false;

				*integral_width =
					interval->day + ceil((double) interval->time / (double) USECS_PER_DAY);

				if (integral_value >= (TS_DATE_END - *integral_width))
					return false;
			}
			else if (bucket_type == TIMESTAMPOID || bucket_type == TIMESTAMPTZOID)
			{
				/*
				 * If width interval has day component we merge it with time component
				 */
				*integral_width = interval->time;

				if (interval->day != 0)
				{
					/*
					 * if our transformed restriction would overflow we skip adding it
					 */
					if (interval->time >= TS_TIMESTAMP_END - interval->day * USECS_PER_DAY)
						return false;

					*integral_width += interval->day * USECS_PER_DAY;
				}

				if (integral_value >= (TS_TIMESTAMP_END - *integral_width))
					return false;
			}
			else
			{
				return false;
			}
			break;
		}
		default:
			return false;
	}
	return true;
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
 * If the transformation cannot be applied, returns NULL.
 */
Expr *
ts_transform_time_bucket_comparison(Expr *node)
{
	TimeBucketQual tbqual;

	if (!extract_time_bucket_qual(node, &tbqual))
		return NULL;

	/*
	 * The qual is an expression <time_bucket OP value> or <value OP time_bucket>. Convert the value
	 * to integral time format.
	 */
	int64 integral_value = const_to_integral_timeval(tbqual.value);
	Const *newvalue = NULL;

	/*
	 * We strip the time_bucket() from the expression, leaving the input "time" argument. Depending
	 * on the comparison OP, the value might need adjustment. Then the value is converted to the
	 * input/column type for time_bucket(). In most cases, the value's original type and the bucket
	 * input type is the same (e.g. TIMESTAMPTZ), but in some cases they differ. For example, it is
	 * possible to compare an int8 bucket function with an int4 value. In the case of UUID bucket,
	 * the bucket function's input type (UUID) is different from the output type (TIMESTAMPTZ), so
	 * the timestamp value needs to be converted to a boundary UUID.
	 */
	switch (tbqual.strategy)
	{
		case BTGreaterStrategyNumber:
		case BTGreaterEqualStrategyNumber:
			/*
			 * Since time_bucket will always shift the input to the left this
			 * transformation is always safe even in the presence of offset variants.
			 *
			 * Handle expressions of form:
			 *
			 *  - column > value
			 *  - column >= value
			 */
			newvalue = integral_timeval_to_const(integral_value, tbqual.tb.timetype);
			break;
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
		{
			/*
			 * Handle expressions of form:
			 *
			 *  - column < value + width
			 *  - column <= value + width
			 *  */
			int64 integral_width = 0;

			if (!time_bucket_width_to_integral(tbqual.tb.width,
											   tbqual.tb.rettype,
											   integral_value,
											   &integral_width))
				return NULL;

			if (tbqual.strategy == BTLessStrategyNumber && tbqual.tb.numargs == 2 &&
				integral_value % integral_width == 0)
				newvalue = integral_timeval_to_const(integral_value, tbqual.tb.timetype);
			else
				newvalue =
					integral_timeval_to_const(integral_value + integral_width, tbqual.tb.timetype);

			break;
		}
		default:
			return NULL;
	}

	Assert(newvalue != NULL);

	/* Create a new "unwrapped" OpExpr using the time_bucket() input/column type */
	TypeCacheEntry *tce = lookup_type_cache(tbqual.tb.timetype, TYPECACHE_BTREE_OPFAMILY);
	Oid opno = get_opfamily_member(tce->btree_opf,
								   tce->btree_opintype,
								   tce->btree_opintype,
								   tbqual.strategy);

	OpExpr *op = (OpExpr *) copyObject(node);
	op->args = list_make2(tbqual.tb.timeval, newvalue);
	op->opno = opno;

	/* The operator might have changed, so reset the function ID */
	op->opfuncid = InvalidOid;

	return &op->xpr;
}

/*
 * Since baserestrictinfo is not yet set by the planner, we have to derive
 * it ourselves. It's safe for us to miss some restrict info clauses (this
 * will just result in more chunks being included) so this does not need
 * to be as comprehensive as the PG native derivation. This is inspired
 * by the derivation in `deconstruct_recurse` in PG
 *
 * TODO: as of 2025, the baserestrictinfo and joininfo is already set when the
 * hypertable expansion code is called, so this does duplicate work. If any bugs
 * are found in this code, it should be switched to use the RelOptInfos and
 * equivalence classes instead of the parse tree. The chunk exclusion code for
 * the non-join clauses was already changed to use the former.
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
		Relids relids = pull_varnos(ctx->root, (Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			if ((IsA(left, Var) && is_timestamptz_op_interval(right)) ||
				(IsA(right, Var) && is_timestamptz_op_interval(left)))
			{
				/*
				 * check for constraints with TIMESTAMPTZ OP INTERVAL calculations
				 */
				qual = (Expr *) constify_timestamptz_op_interval(ctx->root, op);
			}
			else
			{
				/*
				 * check for time_bucket comparisons
				 * time_bucket(Const, time_colum) > Const
				 */
				Expr *transformed = ts_transform_time_bucket_comparison(qual);
				if (transformed != NULL)
				{
					/*
					 * if we could transform the expression we add it to the list of
					 * quals so it can be used as an index condition
					 */
					additional_quals = lappend(additional_quals, transformed);

					/*
					 * Also use the transformed qual for chunk exclusion.
					 */
					qual = transformed;
				}
			}
		}

		/* Do not include this restriction if this is an outer join. Including
		 * the restriction would exclude chunks and thus rows of the outer
		 * relation when it should show all rows */
		if (!is_outer_join)
			ctx->restrictions =
				lappend(ctx->restrictions, make_simple_restrictinfo(ctx->root, qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

static Node *
timebucket_annotate(Node *quals, CollectQualCtx *ctx)
{
	ListCell *lc;
	List *additional_quals = NIL;

	foreach (lc, castNode(List, quals))
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos(ctx->root, (Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		/*
		 * check for time_bucket comparisons
		 * time_bucket(Const, time_colum) > Const
		 */
		Expr *transformed = ts_transform_time_bucket_comparison(qual);
		if (transformed != NULL)
		{
			/*
			 * if we could transform the expression we add it to the list of
			 * quals so it can be used as an index condition
			 */
			additional_quals = lappend(additional_quals, transformed);

			/*
			 * Also use the transformed qual for chunk exclusion.
			 */
			qual = transformed;
		}

		ctx->restrictions = lappend(ctx->restrictions, make_simple_restrictinfo(ctx->root, qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

/*
 * collect JOIN information
 *
 * This function adds information to the CollectQualCtx
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
		Relids relids = pull_varnos(ctx->root, (Node *) qual);
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

	return expression_tree_walker(node, collect_quals_walker, ctx);
}

static int
chunk_cmp_chunk_reloid(const void *c1, const void *c2)
{
	Oid lhs = (*(Chunk **) c1)->table_id;
	Oid rhs = (*(Chunk **) c2)->table_id;

	if (lhs < rhs)
		return -1;
	if (lhs > rhs)
		return 1;
	return 0;
}

static Chunk **
find_children_chunks(HypertableRestrictInfo *hri, Hypertable *ht, bool include_osm,
					 unsigned int *num_chunks)
{
	/*
	 * Unlike find_all_inheritors we do not include parent because if there
	 * are restrictions the parent table cannot fulfill them and since we do
	 * have a trigger blocking inserts on the parent table it cannot contain
	 * any rows.
	 */
	Chunk **chunks = ts_hypertable_restrict_info_get_chunks(hri, ht, include_osm, num_chunks);

	/*
	 * Sort the chunks by oid ascending to roughly match the order provided
	 * by find_inheritance_children. This is mostly needed to avoid test
	 * reference changes.
	 */
	qsort((void *) chunks, *num_chunks, sizeof(Chunk *), chunk_cmp_chunk_reloid);

	return chunks;
}

static bool
should_order_append(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, int *order_attno,
					bool *reverse)
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

	return ts_ordered_append_should_optimize(root, rel, ht, order_attno, reverse);
}

/*
 * Some time conditions are not directly applicable for the chunk exclusion, but
 * imply a simpler time comparison condition which can be used for hypertable
 * expansion. Return a list of any simplified restrictions we could build for
 * the restrictions in the given list.
 */
static List *
get_simplified_restrictions(PlannerInfo *root, List *restrictions)
{
	List *simplified_restrictions = NIL;
	ListCell *lc;
	foreach (lc, restrictions)
	{
		RestrictInfo *ri = castNode(RestrictInfo, lfirst(lc));
		Expr *qual = ri->clause;
		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			if ((IsA(left, Var) && is_timestamptz_op_interval(right)) ||
				(IsA(right, Var) && is_timestamptz_op_interval(left)))
			{
				/*
				 * Check for constraints with TIMESTAMPTZ OP INTERVAL calculations.
				 */
				Expr *transformed = (Expr *) constify_timestamptz_op_interval(root, op);
				if (transformed != (Expr *) op)
				{
					RestrictInfo *ri_copy = copyObject(ri);
					ri_copy->clause = transformed;
					simplified_restrictions = lappend(simplified_restrictions, ri_copy);
				}
			}
			else
			{
				/*
				 * check for time_bucket comparisons
				 * time_bucket(Const, time_colum) > Const
				 */
				Expr *transformed = ts_transform_time_bucket_comparison(qual);
				if (transformed != NULL)
				{
					/*
					 * Also use the transformed qual for chunk exclusion.
					 */
					RestrictInfo *ri_copy = copyObject(ri);
					ri_copy->clause = transformed;
					simplified_restrictions = lappend(simplified_restrictions, ri_copy);
				}
			}
		}
	}

	return simplified_restrictions;
}

/**
 * Get chunks from restrict info.
 *
 * If appends are returned in order appends_ordered on rel->fdw_private is set to true.
 * To make verifying pathkeys easier in set_rel_pathlist the hypertable attno of the column
 * ordered by is stored in rel->fdw_private.
 * If the hypertable uses space partitioning the nested oids are stored in nested_oids
 * on rel->fdw_private when appends are ordered.
 */
static Chunk **
get_chunks(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, bool include_osm,
		   unsigned int *num_chunks, HypertableRestrictInfo **hri_out)
{
	bool reverse;
	int order_attno;

	HypertableRestrictInfo *hri = ts_hypertable_restrict_info_create(rel, ht);

	/*
	 * This is where the magic happens: use our HypertableRestrictInfo
	 * infrastructure to deduce the appropriate chunks using our range
	 * exclusion.
	 */
	ts_hypertable_restrict_info_add(hri, root, rel->baserestrictinfo);

	List *simplified_restrictions = get_simplified_restrictions(root, rel->baserestrictinfo);
	ts_hypertable_restrict_info_add(hri, root, simplified_restrictions);

	/* Limit to hypertables without multiple dimensions for now */
	if (hri->num_base_restrictions >= 1 && hri->num_dimensions == 1 &&
		ht->space->num_dimensions == 1)
	{
		*hri_out = hri;
	}

	/*
	 * If fdw_private has not been setup by caller there is no point checking
	 * for ordered append as we can't pass the required metadata in fdw_private
	 * to signal that this is safe to transform in ordered append plan in
	 * set_rel_pathlist.
	 */
	if (rel->fdw_private != NULL && should_order_append(root, rel, ht, &order_attno, &reverse))
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
															  include_osm,
															  NULL,
															  reverse,
															  nested_oids,
															  num_chunks);
	}

	return find_children_chunks(hri, ht, include_osm, num_chunks);
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

	return expression_tree_walker(node, timebucket_annotate_walker, ctx);
}

void
ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel)
{
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.all_quals = NIL,
		.propagate_conditions = NIL,
	};

	/* Walk the tree and find restrictions or chunk exclusion functions */
	timebucket_annotate_walker((Node *) root->parse->jointree, &ctx);

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);
}

/*
 * Build a list of baserestrictinfo with any Var OP Const constraints on the primary
 * dimension removed.
 */
static List *
filter_baserestrictions(Hypertable *ht, List *base_restrictions)
{
	AttrNumber dim_attno = ht->space->dimensions[0].column_attno;
	List *filtered_restrictions = NIL;
	ListCell *lc;
	foreach (lc, base_restrictions)
	{
		RestrictInfo *ri = castNode(RestrictInfo, lfirst(lc));
		Expr *qual = ri->clause;
		if (IsA(qual, OpExpr))
		{
			OpExpr *op = castNode(OpExpr, qual);
			Node *left = strip_implicit_coercions(linitial(op->args));
			Node *right = strip_implicit_coercions(lsecond(op->args));
			if ((IsA(left, Var) && IsA(right, Const) &&
				 castNode(Var, left)->varattno == dim_attno) ||
				(IsA(right, Var) && IsA(left, Const) &&
				 castNode(Var, right)->varattno == dim_attno))
			{
				/* only consider simple column to constant comparisons */
				continue;
			}
		}

		filtered_restrictions = lappend(filtered_restrictions, ri);
	}
	return filtered_restrictions;
}

/*
 * Returns true if the given chunk is fully included by the restrictions
 * on the primary dimension.
 */
static bool
chunk_fully_covered(HypertableRestrictInfo *hri, Chunk *chunk)
{
	DimensionRestrictInfoOpen *dri = (DimensionRestrictInfoOpen *) hri->dimension_restriction[0];
	Ensure(dri->base.dimension->type == DIMENSION_TYPE_OPEN, "primary dimension must be open");
	Ensure(hri->num_base_restrictions > 0, "must have base restrictions");

	if (IS_OSM_CHUNK(chunk) ||
		(dri->lower_strategy == InvalidStrategy && dri->upper_strategy == InvalidStrategy) ||
		(chunk->cube->slices[0]->fd.range_start == TS_TIME_NOBEGIN ||
		 chunk->cube->slices[0]->fd.range_end == TS_TIME_NOEND))
		return false;

	/*
	 * DimensionRetrictInfo strategy should only be one BTGreaterStrategyNumber
	 * or BTGreaterEqualStrategyNumber on the lower boundary and
	 * BTLessStrategyNumber or BTLessEqualStrategyNumber on the upper boundary.
	 *
	 * BTEqualStrategyNumber gets changed to BTGreaterEqualStrategyNumber
	 * on lower boundary and BTLessEqualStrategyNumber on upper boundary.
	 */
	if (dri->lower_strategy != InvalidStrategy)
	{
		switch (dri->lower_strategy)
		{
			case BTGreaterStrategyNumber:
				if (chunk->cube->slices[0]->fd.range_start <= dri->lower_bound)
					return false;
				break;
			case BTGreaterEqualStrategyNumber:
				if (chunk->cube->slices[0]->fd.range_start < dri->lower_bound)
					return false;
				break;
			default:
				/* Should never happen */
				elog(ERROR, "unexpected dimension restrictinfo strategy: %d", dri->upper_strategy);
		}
	}
	if (dri->upper_strategy != InvalidStrategy)
	{
		switch (dri->upper_strategy)
		{
			case BTLessStrategyNumber:
				if (chunk->cube->slices[0]->fd.range_end > dri->upper_bound)
					return false;
				break;
			case BTLessEqualStrategyNumber:
				if (chunk->cube->slices[0]->fd.range_end - 1 > dri->upper_bound)
					return false;
				break;
			default:
				/* Should never happen */
				elog(ERROR, "unexpected dimension restrictinfo strategy: %d", dri->upper_strategy);
		}
	}
	return true;
}

/* Inspired by expand_inherited_rtentry but expands
 * a hypertable chunks into an append relation. */
void
ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel,
								 bool include_osm)
{
	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
	Oid parent_oid = rte->relid;
	Relation oldrelation;
	Query *parse = root->parse;
	Index rti = rel->relid;
	List *appinfos = NIL;
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.all_quals = NIL,
		.propagate_conditions = NIL,
		.join_level = 0,
	};
	Index first_chunk_index = 0;

	/* double check our permissions are valid */
	Assert(rti != (Index) parse->resultRelation);

	/* Walk the tree and find restrictions */
	collect_quals_walker((Node *) root->parse->jointree, &ctx);
	/* check join_level bookkeeping is balanced */
	Assert(ctx.join_level == 0);

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);

	Chunk **chunks = NULL;
	unsigned int num_chunks = 0;

	HypertableRestrictInfo *hri = NULL;
	chunks = get_chunks(root, rel, ht, include_osm, &num_chunks, &hri);
	/* Can have zero chunks. */
	Assert(num_chunks == 0 || chunks != NULL);

	/* nothing to do here if we have no chunks */
	if (!num_chunks)
		return;

	/*
	 * Handle PlanRowMark for FOR UPDATE/SHARE and FK constraint enforcement.
	 * This replicates expand_inherited_rtentry() in inherit.c.
	 */
	PlanRowMark *oldrc = get_plan_rowmark(root->rowMarks, rti);
	bool old_isParent = false;
	int old_allMarkTypes = 0;
	if (oldrc)
	{
		old_isParent = oldrc->isParent;
		oldrc->isParent = true;
		old_allMarkTypes = oldrc->allMarkTypes;
	}

	for (unsigned int i = 0; i < num_chunks; i++)
	{
		/*
		 * Add the information about chunks to the baserel info cache for
		 * classify_relation().
		 */
		ts_add_baserel_cache_entry_for_chunk(chunks[i]->table_id, ht);
	}

	oldrelation = table_open(parent_oid, NoLock);

	/*
	 * the simple_*_array structures have already been set, we need to add the
	 * children to them.
	 */
	expand_planner_arrays(root, num_chunks);

	for (unsigned int i = 0; i < num_chunks; i++)
	{
		Chunk *chunk = chunks[i];
		Oid child_oid = chunk->table_id;
		Relation newrelation;
		RangeTblEntry *childrte;
		Index child_rtindex;
		AppendRelInfo *appinfo;
		LOCKMODE chunk_lock = rte->rellockmode;

		/* Open rel if needed */

		Assert(child_oid != parent_oid);
		newrelation = table_open(child_oid, chunk_lock);

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
		/* Since PG16, the permission info is maintained separately. Unlink
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
		ts_make_inh_translation_list(oldrelation, newrelation, child_rtindex, appinfo);
		appinfo->parent_reloid = parent_oid;
		appinfos = lappend(appinfos, appinfo);

		/*
		 * Create child PlanRowMark if parent has one. This replicates
		 * expand_single_inheritance_child() in inherit.c.
		 */
		if (oldrc)
		{
			PlanRowMark *childrc = makeNode(PlanRowMark);

			childrc->rti = child_rtindex;
			childrc->prti = oldrc->rti;
			childrc->rowmarkId = oldrc->rowmarkId;
			childrc->markType = select_rowmark_type(childrte, oldrc->strength);
			childrc->allMarkTypes = (1 << childrc->markType);
			childrc->strength = oldrc->strength;
			childrc->waitPolicy = oldrc->waitPolicy;
			childrc->isParent = false; /* chunks are never partitioned */

			oldrc->allMarkTypes |= childrc->allMarkTypes;

			root->rowMarks = lappend(root->rowMarks, childrc);
		}

		/* Close child relations, but keep locks */
		if (child_oid != parent_oid)
			table_close(newrelation, NoLock);
	}

	table_close(oldrelation, NoLock);

	/*
	 * Add required junk columns for row marks. This replicates the logic
	 * after the expansion loop in expand_inherited_rtentry() in inherit.c.
	 */
	if (oldrc)
	{
		int new_allMarkTypes = oldrc->allMarkTypes;
		Var *var;
		TargetEntry *tle;
		char resname[32];
		List *newvars = NIL;

		/*
		 * TID junk var: only needed if parent had only ROW_MARK_COPY but children
		 * added non-COPY marks. This can only happen if the parent is a foreign
		 * table with regular table children. Since hypertable parents are always
		 * regular tables, preprocess_targetlist() (preptlist.c) already adds TID
		 * for the parent before expansion, so this path is unreachable.
		 */
		Ensure(!(new_allMarkTypes & ~(1 << ROW_MARK_COPY) &&
				 !(old_allMarkTypes & ~(1 << ROW_MARK_COPY))),
			   "unexpected: TID junk var needed for hypertable (parent should always be regular "
			   "table)");

		/* Add whole-row junk Var if needed, unless we had it already */
		if ((new_allMarkTypes & (1 << ROW_MARK_COPY)) && !(old_allMarkTypes & (1 << ROW_MARK_COPY)))
		{
			var = makeWholeRowVar(planner_rt_fetch(oldrc->rti, root), oldrc->rti, 0, false);
			snprintf(resname, sizeof(resname), "wholerow%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/* Add tableoid junk Var, unless we had it already */
		if (!old_isParent)
		{
			var = makeVar(oldrc->rti, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
			snprintf(resname, sizeof(resname), "tableoid%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/*
		 * Add the newly added Vars to parent's reltarget.  We needn't worry
		 * about the children's reltargets, they'll be made later.
		 */
		add_vars_to_targetlist_compat(root, newvars, bms_make_singleton(0));
	}

	ts_add_append_rel_infos(root, appinfos);

	/* PostgreSQL will not set up the child rels for use, due to the games
	 * we're playing with inheritance, so we must do it ourselves.
	 * build_simple_rel will look things up in the append_rel_array, so we can
	 * only use it after that array has been set up.
	 */
	List *base_restrictions = rel->baserestrictinfo;
	List *filtered_restrictions = NIL;
	bool try_restriction_filtering =
		ts_guc_enable_qual_filtering && hri && ht->space->num_dimensions == 1;

	if (try_restriction_filtering)
	{
		filtered_restrictions = filter_baserestrictions(ht, base_restrictions);
		/* Dont try filtering if all restrictions remain after filtering */
		if (list_length(base_restrictions) == list_length(filtered_restrictions))
			try_restriction_filtering = false;
	}

	for (unsigned int i = 0; i < num_chunks; i++)
	{
		bool can_clear_restrictinfo = false;
		Index child_rtindex = first_chunk_index + i;
		Chunk *chunk = chunks[i];
		if (try_restriction_filtering)
		{
			can_clear_restrictinfo = chunk_fully_covered(hri, chunk);
		}

		/* build_simple_rel will copy baserestrictinfo to the child rel and
		 * do the necessary attribute mapping. If we can determine that the chunk
		 * is fully covered by the primary dimension restriction we can remove
		 * primary dimension restrictions from baserestrictinfo.
		 */
		if (can_clear_restrictinfo)
			rel->baserestrictinfo = filtered_restrictions;

		/* build_simple_rel will add the child to the relarray */
		RelOptInfo *child_rel = build_simple_rel(root, child_rtindex, rel);

		if (can_clear_restrictinfo)
			rel->baserestrictinfo = base_restrictions;

		/*
		 * Can't touch fdw_private for OSM chunks, it might be managed by the
		 * OSM extension, or, in the tests, by postgres_fdw.
		 */
		if (!IS_OSM_CHUNK(chunk))
		{
			Assert(chunk->table_id == root->simple_rte_array[child_rtindex]->relid);
			ts_get_private_reloptinfo(child_rel)->cached_chunk_struct = chunk;
		}
	}
}

static bool
restrictinfo_has_qual(List *restrictions, OpExpr *qual)
{
	ListCell *lc_ri;
	foreach (lc_ri, restrictions)
	{
		if (equal(castNode(RestrictInfo, lfirst(lc_ri))->clause, (Expr *) qual))
			return true;
	}
	return false;
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
		 * propagate_conditions only has OpExpr with 2 Var as arguments
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
			if (restrictinfo_has_qual(ctx->restrictions, propagated))
				continue;

			Relids relids = pull_varnos(ctx->root, (Node *) propagated);
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
				if (!restrictinfo_has_qual(rel->baserestrictinfo, propagated))
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
