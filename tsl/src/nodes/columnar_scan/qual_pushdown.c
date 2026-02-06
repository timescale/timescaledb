/* * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/restrictinfo.h>
#include <parser/parse_func.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "columnar_scan.h"
#include "compression/create.h"
#include "compression/sparse_index_bloom1.h"
#include "custom_type_cache.h"
#include "guc.h"
#include "ts_catalog/array_utils.h"

#include "qual_pushdown.h"

typedef struct BloomCandidate
{
	Expr *predicate;
	Bitmapset *col_attnos;
} BloomCandidate;

typedef struct BloomCandidates
{
	List *candidates;
} BloomCandidates;
typedef struct QualPushdownContext
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	CompressionSettings *settings;

	/* Bloom candidates list to push down. This will be
	 * merged into the baserestrictinfo in the end. But before
	 * mergeing we will want to sort them and remove redundant ones.
	 */
	BloomCandidates *bloom_candidates;

	/*
	 * This is actually the result, not the static input context like above, but
	 * there's no way to separate this properly using the expression tree mutator
	 * interface.
	 */
	bool can_pushdown;
	bool needs_recheck;
} QualPushdownContext;

static QualPushdownContext
copy_context(const QualPushdownContext *source)
{
	QualPushdownContext copy;
	copy = *source;
	copy.can_pushdown = true;
	copy.needs_recheck = false;
	return copy;
}

static Node *qual_pushdown_mutator(Node *node, QualPushdownContext *context);

/*
 * Result of validating an OpExpr as a potential bloom filter candidate.
 * Does NOT make decisions about which operand to use.
 */
typedef struct HashableEqualityInfo
{
	Var *left_var;		 /* NULL if left is not a Var on chunk_rel */
	Var *right_var;		 /* NULL if right is not a Var on chunk_rel */
	Expr *left_expr;	 /* Original left operand (after unwrapping RelabelType) */
	Expr *right_expr;	 /* Original right operand (after unwrapping RelabelType) */
	Oid opno;			 /* Original operator OID */
	bool left_hashable;	 /* Is operator in left_var's hash opfamily? */
	bool right_hashable; /* Is operator in right_var's hash opfamily? */
	bool valid;			 /* Is this a valid hashable equality? */
} HashableEqualityInfo;

static HashableEqualityInfo validate_hashable_equality(OpExpr *opexpr,
													   QualPushdownContext *context);
static Var *extract_var_for_bloom1(OpExpr *opexpr, QualPushdownContext *context, Expr **value_out,
								   Oid *op_oid_out);

static Var *extract_var_for_composite_bloom(OpExpr *opexpr, QualPushdownContext *context,
											Expr **value_out, Oid *op_oid_out);
static void pushdown_composite_blooms(PlannerInfo *root, QualPushdownContext *context);
static void add_composite_bloom_candidate(BloomCandidates *bloom_candidates, Expr *predicate,
										  const Bitmapset *col_attnos);

void
pushdown_quals(PlannerInfo *root, CompressionSettings *settings, RelOptInfo *chunk_rel,
			   RelOptInfo *compressed_rel, bool chunk_partial)
{
	ListCell *lc;
	List *decompress_clauses = NIL;
	BloomCandidates *bloom_candidates = palloc0(sizeof(BloomCandidates));
	QualPushdownContext base_context = {
		.chunk_rel = chunk_rel,
		.compressed_rel = compressed_rel,
		.chunk_rte = planner_rt_fetch(chunk_rel->relid, root),
		.compressed_rte = planner_rt_fetch(compressed_rel->relid, root),
		.settings = settings,
		.bloom_candidates = bloom_candidates,
	};

	/*
	 * Collect composite bloom candidates first.
	 * This looks at ALL equality predicates together to find composite bloom matches
	 * and push down the composite bloom filters.
	 */
	if (ts_guc_enable_sparse_index_bloom && settings != NULL && settings->fd.index != NULL &&
		ts_guc_enable_composite_bloom_indexes)
	{
		pushdown_composite_blooms(root, &base_context);
	}

	foreach (lc, chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);

		QualPushdownContext clause_context = copy_context(&base_context);
		Node *pushed_down = qual_pushdown_mutator((Node *) ri->clause, &clause_context);

		if (clause_context.can_pushdown)
		{
			/*
			 * We have to call eval_const_expressions after pushing down
			 * the quals, to normalize the bool expressions. Namely, we might add an
			 * AND boolexpr on minmax metadata columns, but the normal form is not
			 * allowed to have nested AND boolexprs. They break some functions like
			 * generate_bitmap_or_paths().
			 */
			pushed_down = eval_const_expressions(root, pushed_down);

			if (IsA(pushed_down, BoolExpr) && castNode(BoolExpr, pushed_down)->boolop == AND_EXPR)
			{
				/* have to separate out and expr into different restrict infos */
				ListCell *lc_and;
				BoolExpr *bool_expr = castNode(BoolExpr, pushed_down);
				foreach (lc_and, bool_expr->args)
				{
					compressed_rel->baserestrictinfo =
						lappend(compressed_rel->baserestrictinfo,
								make_simple_restrictinfo(root, lfirst(lc_and)));
				}
			}
			else
				compressed_rel->baserestrictinfo =
					lappend(compressed_rel->baserestrictinfo,
							make_simple_restrictinfo(root, (Expr *) pushed_down));
		}

		/*
		 * We need to check the restriction clause on the decompress node if the clause can't be
		 * pushed down or needs re-checking.
		 */
		if (!clause_context.can_pushdown || clause_context.needs_recheck || chunk_partial)
		{
			decompress_clauses = lappend(decompress_clauses, ri);
		}
	}
	chunk_rel->baserestrictinfo = decompress_clauses;
	if (ts_guc_enable_composite_bloom_indexes)
	{
		/* Merge the composite bloom candidates into the baserestrictinfo as last quals. */
		ListCell *lc;
		foreach (lc, base_context.bloom_candidates->candidates)
		{
			BloomCandidate *cand = lfirst(lc);
			base_context.compressed_rel->baserestrictinfo =
				lappend(base_context.compressed_rel->baserestrictinfo,
						make_simple_restrictinfo(root, cand->predicate));
		}
	}
}

static OpExpr *
make_segment_meta_opexpr(QualPushdownContext *context, Oid opno, AttrNumber meta_column_attno,
						 Var *uncompressed_var, Expr *compare_to_expr, StrategyNumber strategy)
{
	Var *meta_var = makeVar(context->compressed_rel->relid,
							meta_column_attno,
							uncompressed_var->vartype,
							-1,
							InvalidOid,
							0);

	return (OpExpr *) make_opclause(opno,
									BOOLOID,
									false,
									(Expr *) meta_var,
									copyObject(compare_to_expr),
									InvalidOid,
									uncompressed_var->varcollid);
}

static void
expr_fetch_minmax_metadata(QualPushdownContext *context, Expr *expr, AttrNumber *min_attno,
						   AttrNumber *max_attno)
{
	*min_attno = InvalidAttrNumber;
	*max_attno = InvalidAttrNumber;

	if (!IsA(expr, Var))
		return;

	Var *var = castNode(Var, expr);

	/*
	 * Not on the chunk we expect. This doesn't really happen because we don't
	 * push down the join quals, only the baserestrictinfo.
	 */
	if ((Index) var->varno != context->chunk_rel->relid)
		return;

	/* ignore system attributes or whole row references */
	if (var->varattno <= 0)
		return;

	*min_attno = compressed_column_metadata_attno(context->settings,
												  context->chunk_rte->relid,
												  var->varattno,
												  context->compressed_rte->relid,
												  "min");
	*max_attno = compressed_column_metadata_attno(context->settings,
												  context->chunk_rte->relid,
												  var->varattno,
												  context->compressed_rte->relid,
												  "max");
}

static void *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, OpExpr *orig_opexpr)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	List *expr_args = orig_opexpr->args;
	Assert(list_length(expr_args) == 2);
	Expr *orig_leftop = linitial(expr_args);
	Expr *orig_rightop = lsecond(expr_args);

	if (IsA(orig_leftop, RelabelType))
		orig_leftop = ((RelabelType *) orig_leftop)->arg;
	if (IsA(orig_rightop, RelabelType))
		orig_rightop = ((RelabelType *) orig_rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	Oid op_oid = orig_opexpr->opno;
	AttrNumber min_attno;
	AttrNumber max_attno;
	expr_fetch_minmax_metadata(context, orig_leftop, &min_attno, &max_attno);
	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for the left operand, try to commute the operator. */
		op_oid = get_commutator(op_oid);
		Expr *tmp = orig_leftop;
		orig_leftop = orig_rightop;
		orig_rightop = tmp;

		expr_fetch_minmax_metadata(context, orig_leftop, &min_attno, &max_attno);
	}

	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for either operand. */
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Var *var_with_segment_meta = castNode(Var, orig_leftop);

	/* May be able to allow non-strict operations as well.
	 * Next steps: Think through edge cases, either allow and write tests or figure out why we must
	 * block strict operations
	 */
	if (!OidIsValid(op_oid) || !op_strict(op_oid))
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/* If the collation to be used by the OP doesn't match the column's collation do not push down
	 * as the materialized min/max value do not match the semantics of what we need here */
	Oid op_collation = orig_opexpr->inputcollid;
	if (var_with_segment_meta->varcollid != op_collation)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	TypeCacheEntry *tce =
		lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_BTREE_OPFAMILY);

	const int strategy = get_op_opfamily_strategy(op_oid, tce->btree_opf);
	if (strategy == InvalidStrategy)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/*
	 * Check if the righthand expression is safe to push down. We cannot combine
	 * it with the original operator if there can be false negatives.
	 */
	QualPushdownContext tmp_context = copy_context(context);
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) orig_rightop, &tmp_context);
	if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}
	Assert(pushed_down_rightop != NULL);

	const Oid expr_type_id = exprType((Node *) pushed_down_rightop);

	switch (strategy)
	{
		case BTEqualStrategyNumber:
		{
			/* var = expr implies min < expr and max > expr */
			Oid opno_le = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  expr_type_id,
											  BTLessEqualStrategyNumber);
			Oid opno_ge = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  expr_type_id,
											  BTGreaterEqualStrategyNumber);

			if (!OidIsValid(opno_le) || !OidIsValid(opno_ge))
			{
				/*
				 * Shouldn't be possible if we managed to create the min/max
				 * sparse index, but defend against catalog corruption.
				 */
				context->can_pushdown = false;
				return orig_opexpr;
			}

			return make_andclause(
				list_make2(make_segment_meta_opexpr(context,
													opno_le,
													min_attno,
													var_with_segment_meta,
													pushed_down_rightop,
													BTLessEqualStrategyNumber),
						   make_segment_meta_opexpr(context,
													opno_ge,
													max_attno,
													var_with_segment_meta,
													pushed_down_rightop,
													BTGreaterEqualStrategyNumber)));
		}
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			/* var < expr  implies min < expr */
			{
				Oid opno =
					get_opfamily_member(tce->btree_opf, tce->type_id, expr_type_id, strategy);

				if (!OidIsValid(opno))
				{
					/*
					 * Shouldn't be possible if we managed to create the min/max
					 * sparse index, but defend against catalog corruption.
					 */
					context->can_pushdown = false;
					return orig_opexpr;
				}

				return (Expr *) make_segment_meta_opexpr(context,
														 opno,
														 min_attno,
														 var_with_segment_meta,
														 pushed_down_rightop,
														 strategy);
			}

		case BTGreaterStrategyNumber:
		case BTGreaterEqualStrategyNumber:
			/* var > expr  implies max > expr */
			{
				Oid opno =
					get_opfamily_member(tce->btree_opf, tce->type_id, expr_type_id, strategy);

				if (!OidIsValid(opno))
				{
					/*
					 * Shouldn't be possible if we managed to create the min/max
					 * sparse index, but defend against catalog corruption.
					 */
					context->can_pushdown = false;
					return orig_opexpr;
				}

				return (Expr *) make_segment_meta_opexpr(context,
														 opno,
														 max_attno,
														 var_with_segment_meta,
														 pushed_down_rightop,
														 strategy);
			}
		default:
			context->can_pushdown = false;
			return orig_opexpr;
	}
}

static void
expr_fetch_bloom1_metadata(QualPushdownContext *context, Expr *expr, AttrNumber *bloom1_attno)
{
	*bloom1_attno = InvalidAttrNumber;

	if (!IsA(expr, Var))
		return;

	Var *var = castNode(Var, expr);

	/*
	 * Not on the chunk we expect. This doesn't really happen because we don't
	 * push down the join quals, only the baserestrictinfo.
	 */
	if ((Index) var->varno != context->chunk_rel->relid)
		return;

	/* ignore system attributes or whole row references */
	if (var->varattno <= 0)
		return;

	*bloom1_attno = compressed_column_metadata_attno(context->settings,
													 context->chunk_rte->relid,
													 var->varattno,
													 context->compressed_rte->relid,
													 bloom1_column_prefix);

	if (*bloom1_attno == InvalidAttrNumber && ts_guc_read_legacy_bloom1_v1)
	{
		/*
		 * The version 1 of bloom1 indexes is disabled by default because its
		 * hashing was dependent on build options leading to corrupt indexes,
		 * but can be enabled manually.
		 */
		*bloom1_attno = compressed_column_metadata_attno(context->settings,
														 context->chunk_rte->relid,
														 var->varattno,
														 context->compressed_rte->relid,
														 "bloom1");
	}
}

/*
 * Validate an OpExpr as a hashable equality predicate.
 *
 * Does NOT:
 * - Decide which operand is "column" vs "value"
 * - Check bloom metadata
 * - Check collation (Caller's responsibility - depends on which Var is chosen)
 * - Commute the operator
 *
 * DOES validate:
 * - OpExpr structure
 * - Var identification on chunk_rel
 * - Hash operator validity for each Var
 *
 * Returns info about both operands with validation flags.
 * Caller decides which Var to use and validates collation.
 */
static HashableEqualityInfo
validate_hashable_equality(OpExpr *opexpr, QualPushdownContext *context)
{
	Assert(opexpr != NULL);
	Assert(context != NULL);

	HashableEqualityInfo info = { 0 };
	info.valid = false;
	info.left_hashable = false;
	info.right_hashable = false;

	if (list_length(opexpr->args) != 2)
		return info;

	Expr *left = linitial(opexpr->args);
	Expr *right = lsecond(opexpr->args);

	/* Unwrap RelabelType */
	if (IsA(left, RelabelType))
		left = ((RelabelType *) left)->arg;
	if (IsA(right, RelabelType))
		right = ((RelabelType *) right)->arg;

	info.left_expr = left;
	info.right_expr = right;
	info.opno = opexpr->opno;

	/* Must have valid operator OID */
	if (!OidIsValid(info.opno))
		return info;

	/* Identify Vars on our relation and validate hash operator for each */
	if (IsA(left, Var))
	{
		Var *left_var = (Var *) left;
		if ((Index) left_var->varno == context->chunk_rel->relid && left_var->varattno > 0)
		{
			info.left_var = left_var;

			/* Check if operator is hashable equality for this type */
			TypeCacheEntry *tce = lookup_type_cache(left_var->vartype, TYPECACHE_HASH_OPFAMILY);
			if (OidIsValid(tce->hash_opf))
			{
				int strategy = get_op_opfamily_strategy(info.opno, tce->hash_opf);
				if (strategy == HTEqualStrategyNumber)
					info.left_hashable = true;
			}
		}
	}

	if (IsA(right, Var))
	{
		Var *right_var = (Var *) right;
		if ((Index) right_var->varno == context->chunk_rel->relid && right_var->varattno > 0)
		{
			info.right_var = right_var;

			/* Check if operator is hashable equality for this type */
			TypeCacheEntry *tce = lookup_type_cache(right_var->vartype, TYPECACHE_HASH_OPFAMILY);
			if (OidIsValid(tce->hash_opf))
			{
				int strategy = get_op_opfamily_strategy(info.opno, tce->hash_opf);
				if (strategy == HTEqualStrategyNumber)
					info.right_hashable = true;
			}
		}
	}

	/* Must have at least one Var on our relation */
	if (info.left_var == NULL && info.right_var == NULL)
		return info;

	/* Must have at least one Var that passes hashable equality check */
	if (!info.left_hashable && !info.right_hashable)
		return info;

	info.valid = true;
	return info;
}

/*
 * Extract Var for single-column bloom filter pushdown.
 * Uses bloom metadata presence to decide which operand to use.
 *
 * This handles cases like:
 * - bloom_col = 5              (left has bloom)
 * - 5 = bloom_col              (right has bloom, commute)
 * - bloom_col = segmentby_col  (left has bloom, caller validates segmentby)
 * - segmentby_col = bloom_col  (right has bloom, commute, caller validates)
 * - bloom_col1 = bloom_col2    (left has bloom, caller validates bloom_col2 → FAILS)
 *
 * Returns the Var that has single-column bloom metadata, along with
 * the value expression and (possibly commuted) operator.
 */
static Var *
extract_var_for_bloom1(OpExpr *opexpr, QualPushdownContext *context, Expr **value_out,
					   Oid *op_oid_out)
{
	Assert(value_out != NULL);
	Assert(op_oid_out != NULL);
	Assert(opexpr != NULL);
	Assert(context != NULL);

	*value_out = NULL;
	*op_oid_out = InvalidOid;

	/* Validate the expression */
	HashableEqualityInfo info = validate_hashable_equality(opexpr, context);
	if (!info.valid)
		return NULL;

	/* Try to find a Var with bloom metadata that passes hash operator validation. */
	Var *chosen_var = NULL;
	Expr *value_expr_tmp = NULL;
	Oid op_oid_tmp;
	AttrNumber bloom1_attno = InvalidAttrNumber;

	if (info.left_var != NULL && info.left_hashable)
	{
		expr_fetch_bloom1_metadata(context, (Expr *) info.left_var, &bloom1_attno);
		if (bloom1_attno != InvalidAttrNumber)
		{
			/* Left has bloom metadata and valid hash operator. */
			chosen_var = info.left_var;
			value_expr_tmp = info.right_expr;
			op_oid_tmp = info.opno;
		}
	}

	/* If left didn't qualify, try right. */
	if (chosen_var == NULL && info.right_var != NULL && info.right_hashable)
	{
		expr_fetch_bloom1_metadata(context, (Expr *) info.right_var, &bloom1_attno);
		if (bloom1_attno != InvalidAttrNumber)
		{
			/* Right has bloom metadata and valid hash operator. Need commutation. */
			chosen_var = info.right_var;
			value_expr_tmp = info.left_expr;
			op_oid_tmp = get_commutator(info.opno);
		}
	}

	if (chosen_var == NULL)
	{
		/* No Var with both bloom metadata and valid hash operator */
		return NULL;
	}

	/* Validate collation for the chosen Var */
	Oid op_collation = opexpr->inputcollid;
	if (chosen_var->varcollid != op_collation)
	{
		/* Collation mismatch - bloom filter hash won't match operator hash */
		return NULL;
	}

	/* Cannot use non-deterministic collations */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
		return NULL;

	*value_out = value_expr_tmp;
	*op_oid_out = op_oid_tmp;
	return chosen_var;
}

static void *
pushdown_op_to_segment_meta_bloom1(QualPushdownContext *context, OpExpr *orig_opexpr)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	/*
	 * Use single-column bloom helper to find Var with bloom metadata.
	 * Helper returns first Var with bloom metadata.
	 */
	Expr *orig_rightop = NULL;
	Oid op_oid;
	Var *var = extract_var_for_bloom1(orig_opexpr, context, &orig_rightop, &op_oid);

	if (var == NULL)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/* Get bloom metadata. */
	AttrNumber bloom1_attno = InvalidAttrNumber;
	expr_fetch_bloom1_metadata(context, (Expr *) var, &bloom1_attno);
	Assert(bloom1_attno != InvalidAttrNumber);

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down. We cannot combine
	 * it with the original operator if there can be false negatives.
	 */
	QualPushdownContext tmp_context = copy_context(context);
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) orig_rightop, &tmp_context);
	if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}
	Assert(pushed_down_rightop != NULL);

	if (ts_guc_enable_bloom_index_pruning && context->bloom_candidates != NULL)
	{
		Bitmapset *single_col = bms_make_singleton(var->varattno);
		ListCell *lc;
		foreach (lc, context->bloom_candidates->candidates)
		{
			BloomCandidate *composite = lfirst(lc);
			if (bms_is_subset(single_col, composite->col_attnos))
			{
				/* Single bloom is covered by a composite - skip it */
				bms_free(single_col);
				context->can_pushdown = false;
				return orig_opexpr;
			}
		}
		bms_free(single_col);
	}

	/*
	 * var = expr implies bloom1_contains(var_bloom, expr).
	 */
	Var *bloom_var = makeVar(context->compressed_rel->relid,
							 bloom1_attno,
							 ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
							 -1,
							 InvalidOid,
							 0);

	Oid func = LookupFuncName(list_make2(makeString("_timescaledb_functions"),
										 makeString("bloom1_contains")),
							  /* nargs = */ -1,
							  /* argtypes = */ (void *) -1,
							  /* missing_ok = */ false);

	return (Expr *) makeFuncExpr(func,
								 BOOLOID,
								 list_make2(bloom_var, pushed_down_rightop),
								 /* funccollid = */ InvalidOid,
								 /* inputcollid = */ InvalidOid,
								 COERCE_EXPLICIT_CALL);
}

/*
 * Try to transform x = any(array[]) into bloom1_contains_any(bloom_x, array[]).
 */
static void *
pushdown_saop_bloom1(QualPushdownContext *context, ScalarArrayOpExpr *orig_saop)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	if (!orig_saop->useOr)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	List *expr_args = orig_saop->args;
	Assert(list_length(expr_args) == 2);
	Expr *orig_leftop = linitial(expr_args);
	Expr *orig_rightop = lsecond(expr_args);

	if (IsA(orig_leftop, RelabelType))
		orig_leftop = ((RelabelType *) orig_leftop)->arg;
	if (IsA(orig_rightop, RelabelType))
		orig_rightop = ((RelabelType *) orig_rightop)->arg;

	/*
	 * For scalar array operation, we expect a var on the left side.
	 */
	AttrNumber bloom1_attno = InvalidAttrNumber;
	expr_fetch_bloom1_metadata(context, orig_leftop, &bloom1_attno);
	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for left operand. */
		context->can_pushdown = false;
		return orig_saop;
	}

	Var *var_with_segment_meta = castNode(Var, orig_leftop);

	/*
	 * Play it safe and don't push down if the operator collation doesn't match
	 * the column collation.
	 */
	Oid op_collation = orig_saop->inputcollid;
	if (var_with_segment_meta->varcollid != op_collation)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * We cannot use bloom filters for non-deterministic collations.
	 */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * We only support hashable equality operators.
	 */
	const Oid op_oid = orig_saop->opno;
	TypeCacheEntry *tce =
		lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	const int strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
	if (strategy != HTEqualStrategyNumber)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down. We cannot combine
	 * it with the original operator if there can be false negatives.
	 */
	QualPushdownContext tmp_context = copy_context(context);
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) orig_rightop, &tmp_context);
	if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
	{
		context->can_pushdown = false;
		return orig_saop;
	}
	Assert(pushed_down_rightop != NULL);

	/*
	 * var = any(array) implies bloom1_contains_any(var_bloom, array).
	 */
	Var *bloom_var = makeVar(context->compressed_rel->relid,
							 bloom1_attno,
							 ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
							 -1,
							 InvalidOid,
							 0);

	Oid func = LookupFuncName(list_make2(makeString("_timescaledb_functions"),
										 makeString("bloom1_contains_any")),
							  /* nargs = */ -1,
							  /* argtypes = */ (void *) -1,
							  /* missing_ok = */ false);

	return makeFuncExpr(func,
						BOOLOID,
						list_make2(bloom_var, pushed_down_rightop),
						/* funccollid = */ InvalidOid,
						/* inputcollid = */ InvalidOid,
						COERCE_EXPLICIT_CALL);
}

/*
 * Extract Var for composite bloom filter pushdown.
 * Uses segmentby membership as a heuristic for Var-to-Var cases.
 * Does NOT check bloom metadata (composite bloom is checked later by name matching).
 *
 * This handles cases like:
 * - col = 5                (obvious)
 * - 5 = col                (commute)
 * - col = segmentby_col    (prefer non-segmentby col)
 * - col1 = col2            (prefer non-segmentby, but caller validates value)
 *
 * IMPORTANT: For col1 = col2 where both are non-segmentby, returns col1 but
 * the caller (pushdown_composite_blooms) will reject col2 during value validation.
 * Composite bloom requires value expressions to be constants, params, or segmentby Vars.
 *
 * Returns a Var along with the value expression and (possibly commuted) operator.
 */
static Var *
extract_var_for_composite_bloom(OpExpr *opexpr, QualPushdownContext *context, Expr **value_out,
								Oid *op_oid_out)
{
	Assert(opexpr != NULL);
	Assert(context != NULL);
	Assert(value_out != NULL);
	Assert(op_oid_out != NULL);

	*value_out = NULL;
	*op_oid_out = InvalidOid;

	/* Validate the expression */
	HashableEqualityInfo info = validate_hashable_equality(opexpr, context);
	if (!info.valid)
		return NULL;

	/* Only one side is a Var. */
	Var *chosen_var = NULL;
	Expr *value_expr_tmp = NULL;
	Oid op_oid_tmp;
	bool value_is_segmentby = false;

	if (info.left_var != NULL && info.right_var == NULL)
	{
		chosen_var = info.left_var;
		value_expr_tmp = info.right_expr;
		op_oid_tmp = info.opno;
	}
	else if (info.right_var != NULL && info.left_var == NULL)
	{
		chosen_var = info.right_var;
		value_expr_tmp = info.left_expr;
		op_oid_tmp = get_commutator(info.opno);
	}
	else if (info.left_var != NULL && info.right_var != NULL)
	{
		/*
		 * Both are Vars. Prefer non-segmentby Var (segmentby columns cannot
		 * be in bloom filters), but also require valid hash operator.
		 */
		bool left_is_segmentby = false;
		bool right_is_segmentby = false;

		if (context->settings && context->settings->fd.segmentby)
		{
			char *left_attname =
				get_attname(context->chunk_rte->relid, info.left_var->varattno, false);
			left_is_segmentby = ts_array_is_member(context->settings->fd.segmentby, left_attname);

			char *right_attname =
				get_attname(context->chunk_rte->relid, info.right_var->varattno, false);
			right_is_segmentby = ts_array_is_member(context->settings->fd.segmentby, right_attname);
		}

		/* Try candidates in preference order: non-segmentby+hashable, then segmentby+hashable */
		if (!right_is_segmentby && info.right_hashable)
		{
			/* Right is non-segmentby and hashable - prefer it */
			chosen_var = info.right_var;
			value_expr_tmp = info.left_expr;
			op_oid_tmp = get_commutator(info.opno);
			value_is_segmentby = left_is_segmentby;
		}
		else if (!left_is_segmentby && info.left_hashable)
		{
			/* Left is non-segmentby and hashable - use it */
			chosen_var = info.left_var;
			value_expr_tmp = info.right_expr;
			op_oid_tmp = info.opno;
			value_is_segmentby = right_is_segmentby;
		}
		else if (info.left_hashable)
		{
			/* Left is hashable (may be segmentby) - use it as fallback */
			chosen_var = info.left_var;
			value_expr_tmp = info.right_expr;
			op_oid_tmp = info.opno;
			value_is_segmentby = right_is_segmentby;
		}
		else if (info.right_hashable)
		{
			/* Right is hashable (may be segmentby) - use it as last resort */
			chosen_var = info.right_var;
			value_expr_tmp = info.left_expr;
			op_oid_tmp = get_commutator(info.opno);
			value_is_segmentby = left_is_segmentby;
		}
		else
		{
			/* Neither Var has valid hash operator */
			return NULL;
		}
	}

	/* Validate the chosen Var and value expression */
	if (chosen_var != NULL)
	{
		/* Check collation */
		Oid op_collation = opexpr->inputcollid;
		if (chosen_var->varcollid != op_collation)
		{
			/* Collation mismatch. */
			return NULL;
		}

		/* Cannot use non-deterministic collations */
		if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
			return NULL;

		/*
		 * Reject non-segmentby Vars in value expression.
		 * For composite bloom, value expressions must be pushable (const, param, or segmentby Var).
		 * Non-segmentby Vars need decompression and cannot be used in bloom checks.
		 *
		 * Note: value_is_segmentby is only set when both sides are Vars (both-Vars case).
		 * In that case, value_expr_tmp is always a Var on chunk_rel. If value_is_segmentby
		 * is false in the both-Vars case, the value Var is non-segmentby and must be rejected.
		 * In the single-Var case, value_expr_tmp is not a Var on chunk_rel, so this check
		 * doesn't apply (value_is_segmentby remains false but value_expr_tmp is not a Var).
		 */
		if (IsA(value_expr_tmp, Var))
		{
			Var *value_var = (Var *) value_expr_tmp;
			/* Only check segmentby for Vars on our relation */
			if ((Index) value_var->varno == context->chunk_rel->relid && value_var->varattno > 0 &&
				!value_is_segmentby)
			{
				/* Value is a non-segmentby Var on our relation - cannot be pushed */
				return NULL;
			}
		}

		*value_out = value_expr_tmp;
		*op_oid_out = op_oid_tmp;
		return chosen_var;
	}

	return NULL;
}

/* When adding a composite bloom candidate */
static void
add_composite_bloom_candidate(BloomCandidates *bloom_candidates, Expr *predicate,
							  const Bitmapset *col_attnos)
{
	ListCell *lc;
	foreach (lc, bloom_candidates->candidates)
	{
		BloomCandidate *existing = lfirst(lc);
		if (bms_is_subset(col_attnos, existing->col_attnos))
		{
			if (ts_guc_enable_bloom_index_pruning)
				return;
		}
	}

	/* Remove any existing composites that are subsets of this one */
	if (ts_guc_enable_bloom_index_pruning)
	{
		foreach (lc, bloom_candidates->candidates)
		{
			BloomCandidate *existing = lfirst(lc);
			if (bms_is_subset(existing->col_attnos, col_attnos))
			{
				bloom_candidates->candidates =
					foreach_delete_current(bloom_candidates->candidates, lc);
			}
		}
	}

	/* Add this composite */
	BloomCandidate *candidate = palloc(sizeof(BloomCandidate));
	candidate->predicate = predicate;
	candidate->col_attnos = bms_copy(col_attnos);
	bloom_candidates->candidates = lappend(bloom_candidates->candidates, candidate);
}

/*
 * Scan baserestrictinfo for composite bloom opportunities.
 * For each applicable composite bloom, generate bloom1_contains(bloom, ROW(...)).
 *
 * Scans predicates first, then parses settings only if needed.
 */
static void
pushdown_composite_blooms(PlannerInfo *root, QualPushdownContext *context)
{
	Assert(root != NULL);
	Assert(context != NULL);

	/* We need settings to generate composite blooms. */
	CompressionSettings *settings = context->settings;
	if (settings == NULL || settings->fd.index == NULL)
	{
		return;
	}

	/* We need at least two baserestrictinfo to have a chance to push down a composite bloom filter.
	 */
	if (list_length(context->chunk_rel->baserestrictinfo) < 2)
	{
		return;
	}

	ListCell *lc;
	Bitmapset *var_attnos = NULL;

	/* Build map: chunk_attno -> value_expr for equality predicates. */
	/* Note that we may have more than one predicate for the same attribute
	 * (e.g. col1 = 1 AND col1 = 2) which is a contradiction and we should
	 * be able to detect and optimize for this, meaning that this predicate
	 * will always be false. This is a TODO.
	 *
	 * For now, we will just use the last one, which will filter out some
	 * chunks which is better than nothing.
	 */
	AttrNumber max_attno = context->chunk_rel->max_attr;
	Expr **attno_to_value = palloc0((max_attno + 1) * sizeof(Expr *));

	/* Determine vars with equality predicates. */
	foreach (lc, context->chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		if (!IsA(ri->clause, OpExpr))
			continue;

		Expr *value = NULL;
		Oid op_oid = InvalidOid;
		Var *var =
			extract_var_for_composite_bloom(castNode(OpExpr, ri->clause), context, &value, &op_oid);
		if (var != NULL && value != NULL)
		{
			var_attnos = bms_add_member(var_attnos, var->varattno);
			attno_to_value[var->varattno] = value;
		}
	}

	/* Check if not enough vars with equality predicates. */
	if (bms_num_members(var_attnos) < 2)
		return;

	/* Parse settings to get per-column compression settings. */
	ParsedCompressionSettings *parsed =
		ts_convert_to_parsed_compression_settings(settings->fd.index);
	if (parsed == NULL)
	{
		bms_free(var_attnos);
		pfree(attno_to_value);
		return;
	}

	/* For each sparse index object, resolve the columns to attribute numbers. */
	TsBmsList per_column_attnos =
		ts_resolve_columns_to_attnos_from_parsed_settings(parsed, context->chunk_rte->relid);

	/* This bitmap tells which sparse index objects are candidates for composite bloom filters. */
	Bitmapset *composite_filter_candidates_ids = NULL;

	/* Iterate over the resolved columns and check if they match the vars with equality predicates.
	 */
	ListCell *attno_cell = NULL;
	int sparse_index_obj_id = -1;
	foreach (attno_cell, per_column_attnos)
	{
		sparse_index_obj_id++;
		Bitmapset *attnos = lfirst(attno_cell);
		/* Only care about sparse indices with at least 2 columns. */
		if (bms_num_members(attnos) < 2)
		{
			continue;
		}
		if (bms_is_subset(attnos, var_attnos))
		{
			/* This sparse index object matches the vars with equality predicates. */
			ParsedCompressionSettingsObject *obj = list_nth(parsed->objects, sparse_index_obj_id);
			Assert(obj != NULL);
			if (obj == NULL)
			{
				continue;
			}

			/* Get the index type from the parsed object. */
			List *index_type =
				ts_get_values_by_key_from_parsed_object(obj,
														ts_sparse_index_common_keys
															[SparseIndexKeyType] /* "type" */);
			Assert(index_type != NIL && list_length(index_type) == 1);
			if (index_type == NIL || list_length(index_type) != 1)
			{
				continue;
			}
			/* Check that it is a bloom index. */
			const char *index_type_str = lfirst(list_head(index_type));
			if (strcmp(index_type_str,
					   ts_sparse_index_type_names[_SparseIndexTypeEnumBloom] /* "bloom" */) != 0)
			{
				continue;
			}
			Assert(list_length(ts_get_column_names_from_parsed_object(obj)) >= 2);
			composite_filter_candidates_ids =
				bms_add_member(composite_filter_candidates_ids, sparse_index_obj_id);
		}
	}

	/* Early exit: no composite filter candidates. */
	if (bms_is_empty(composite_filter_candidates_ids))
	{
		pfree(attno_to_value);
		bms_free(var_attnos);
		ts_bmslist_free(per_column_attnos);
		ts_free_parsed_compression_settings(parsed);
		return;
	}

	/* For each composite filter candidate, build the composite bloom filter. */
	int candidate_filter_id = -1;
	while ((candidate_filter_id =
				bms_next_member(composite_filter_candidates_ids, candidate_filter_id)) >= 0)
	{
		ParsedCompressionSettingsObject *obj = list_nth(parsed->objects, candidate_filter_id);
		Assert(obj != NULL);
		/* The column attnos generated from the parsed object is a list indexed by the object id.*/
		Bitmapset *column_attnos = list_nth(per_column_attnos, candidate_filter_id);
		Assert(bms_num_members(column_attnos) >= 2);

		/* Iterate over the attnos for the current candidate filter an check if this is a valid
		 * predicate to push down. This is a safety check and hope that the composite bloom filter
		 * will also be valid to push down by induction.
		 */
		List *pushed_value_exprs = NIL;
		bool all_valid = true;
		int col_attno = -1;
		while ((col_attno = bms_next_member(column_attnos, col_attno)) >= 0)
		{
			Expr *value_expr = attno_to_value[col_attno];
			Assert(value_expr != NULL);

			/* Validate the value expression via qual_pushdown_mutator. */
			QualPushdownContext tmp_context = copy_context(context);
			Expr *pushed_value = (Expr *) qual_pushdown_mutator((Node *) value_expr, &tmp_context);

			if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
			{
				all_valid = false;
				break;
			}
			pushed_value_exprs = lappend(pushed_value_exprs, pushed_value);
		}
		if (!all_valid)
		{
			continue;
		}

		List *column_names = ts_get_column_names_from_parsed_object(obj);
		Assert(list_length(column_names) >= 2 &&
			   list_length(column_names) <= MAX_BLOOM_FILTER_COLUMNS);

		/* Check if this chunk has the composite bloom column */
		char *composite_col_name =
			compressed_column_metadata_name_list_v2(bloom1_column_prefix, column_names);
		AttrNumber composite_attno = get_attnum(context->compressed_rte->relid, composite_col_name);
		pfree(composite_col_name);
		if (!AttributeNumberIsValid(composite_attno))
		{
			continue;
		}

		/* Build ROW(val1, val2, ...) expression using pushed-down values */
		RowExpr *row_expr = makeNode(RowExpr);
		row_expr->args = pushed_value_exprs;
		row_expr->row_typeid = RECORDOID;
		row_expr->row_format = COERCE_IMPLICIT_CAST;
		row_expr->colnames = NIL;
		row_expr->location = -1;

		/* Build bloom1_contains(composite_bloom, ROW(...)) */
		Var *bloom_var = makeVar(context->compressed_rel->relid,
								 composite_attno,
								 ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
								 -1,
								 InvalidOid,
								 0);

		Oid func_oid = LookupFuncName(list_make2(makeString("_timescaledb_functions"),
												 makeString("bloom1_contains")),
									  -1,
									  (void *) -1,
									  false);

		FuncExpr *bloom_check = makeFuncExpr(func_oid,
											 BOOLOID,
											 list_make2(bloom_var, row_expr),
											 InvalidOid,
											 InvalidOid,
											 COERCE_EXPLICIT_CALL);

		/* Add to bloom candidates list. */
		Assert(context->bloom_candidates != NULL);
		add_composite_bloom_candidate(context->bloom_candidates, (Expr *) bloom_check, column_attnos);
	}

	/* Cleanup */
	bms_free(var_attnos);
	bms_free(composite_filter_candidates_ids);
	ts_free_parsed_compression_settings(parsed);
	ts_bmslist_free(per_column_attnos);
	pfree(attno_to_value);
}

static bool
contain_volatile_functions_checker(Oid func_id, void *context)
{
	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

/*
 * Push down the given expression node.
 *
 * This is used as a mutator for expression_tree_mutator().
 *
 * We return the original node if we cannot push it down, to be consistent with
 * the expression_tree_mutator behavior. The caller must check
 * context.can_pushdown.
 */
static Node *
qual_pushdown_mutator(Node *orig_node, QualPushdownContext *context)
{
	if (orig_node == NULL)
	{
		/*
		 * An expression node can have a NULL field and the mutator will be
		 * still called for it, so we have to handle this.
		 */
		return NULL;
	}

	if (!context->can_pushdown)
	{
		/*
		 * Stop early if we already know we can't push down this filter.
		 */
		return orig_node;
	}

	if (check_functions_in_node(orig_node,
								contain_volatile_functions_checker,
								/* context = */ NULL))
	{
		/* pushdown is not safe for volatile expressions */
		context->can_pushdown = false;
		return orig_node;
	}

	switch (nodeTag(orig_node))
	{
		case T_Var:
		{
			Var *var = castNode(Var, orig_node);
			Assert((Index) var->varno == context->chunk_rel->relid);

			if (var->varattno <= 0)
			{
				/* Can't do this for system columns such as whole-row var. */
				context->can_pushdown = false;
				return orig_node;
			}

			char *attname = get_attname(context->chunk_rte->relid, var->varattno, false);
			/* we can only push down quals for segmentby columns */
			if (!ts_array_is_member(context->settings->fd.segmentby, attname))
			{
				context->can_pushdown = false;
				return orig_node;
			}

			var = copyObject(var);
			var->varno = context->compressed_rel->relid;
			var->varattno = get_attnum(context->compressed_rte->relid, attname);

			return (Node *) var;
		}
		case T_OpExpr:
		{
			OpExpr *opexpr = (OpExpr *) orig_node;

			/*
			 * It might be possible to push down the OpExpr as is, if it
			 * references only the segmentby columns. Check this case first.
			 *
			 * Note that we can't push down the entire operator if we pushed
			 * down both sides inexactly, i.e. they require recheck. This means
			 * we can have false positives there, and combining false positives
			 * with the original operator could lead to false negatives, which
			 * would be a bug. Consider for example (x = 1) = (y = 1) in case
			 * where both sides are false, but there's a false posistive for the
			 * pushed down version of the left side but not the right side.
			 */
			QualPushdownContext tmp_context = copy_context(context);
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown && !tmp_context.needs_recheck)
			{
				return pushed_down;
			}

			if (opexpr->opresulttype != BOOLOID)
			{
				/*
				 * The following pushdown options only support operators that
				 * return bool.
				 */
				context->can_pushdown = false;
				return orig_node;
			}

			if (list_length(opexpr->args) != 2)
			{
				/*
				 * The following pushdown options only support operators with
				 * two operands.
				 */
				context->can_pushdown = false;
				return orig_node;
			}

			/*
			 * Try bloom1 sparse index.
			 */
			if (ts_guc_enable_sparse_index_bloom)
			{
				tmp_context = copy_context(context);
				pushed_down = pushdown_op_to_segment_meta_bloom1(&tmp_context, opexpr);
				if (tmp_context.can_pushdown)
				{
					context->needs_recheck |= tmp_context.needs_recheck;
					return pushed_down;
				}
			}

			/*
			 * Try minmax sparse index.
			 */
			tmp_context = copy_context(context);
			pushed_down = pushdown_op_to_segment_meta_min_max(&tmp_context, opexpr);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * No other options to push down the OpExpr.
			 */
			context->can_pushdown = false;
			return orig_node;
		}
		case T_ScalarArrayOpExpr:
		{
			/*
			 * It can be possible to push down the scalar array operation as is,
			 * if it references only the segmentby columns. Check this case
			 * first.
			 *
			 * See the comment for OpExpr about needs_recheck handling.
			 */
			QualPushdownContext tmp_context = copy_context(context);
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown && !tmp_context.needs_recheck)
			{
				return pushed_down;
			}

			ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, orig_node);

			/*
			 * Try to transform x = any(array[]) into
			 * bloom1_contains_any(bloom_x, array[]).
			 */
			if (ts_guc_enable_sparse_index_bloom)
			{
				tmp_context = *context;
				pushed_down = pushdown_saop_bloom1(&tmp_context, saop);
				if (tmp_context.can_pushdown)
				{
					context->needs_recheck |= tmp_context.needs_recheck;
					return pushed_down;
				}
			}

			/*
			 * No other ways to push it down, so consider it failed.
			 */
			context->can_pushdown = false;
			return orig_node;
		}
		/*
		 * These nodes do not influence the pushdown by themselves, so we
		 * recurse.
		 */
		case T_BoolExpr:
		case T_FuncExpr:
		case T_CoerceViaIO:
		case T_RelabelType:
		case T_List:
		case T_Const:
		case T_NullTest:
		case T_Param:
		case T_SQLValueFunction:
		case T_CaseExpr:
		case T_CaseWhen:
		case T_ArrayExpr:
		{
			Node *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, context);
			return pushed_down;
		}

		/*
		 * We don't know how to work with other nodes.
		 */
		default:
			context->can_pushdown = false;
			return orig_node;
	}
}
