/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "chunk.h"
#include "hypertable_cache.h"
#include <catalog/pg_operator.h>
#include <math.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <planner/planner.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>

#include <planner.h>

#include "compat/compat.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "cross_module_fn.h"
#include "custom_type_cache.h"
#include "debug_assert.h"
#include "import/allpaths.h"
#include "import/planner.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/columnar_scan/planner.h"
#include "nodes/columnar_scan/qual_pushdown.h"
#include "ts_catalog/array_utils.h"
#include "utils.h"

static CustomPathMethods columnar_scan_path_methods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = columnar_scan_plan_create,
};

typedef struct SortInfo
{
	List *required_compressed_pathkeys;
	List *required_eq_classes;
	bool needs_sequence_num;
	bool use_compressed_sort; /* sort can be pushed below ColumnarScan */
	bool use_batch_sorted_merge;
	bool reverse;

	List *decompressed_sort_pathkeys;
	QualCost decompressed_sort_pathkeys_cost;
} SortInfo;

static RangeTblEntry *columnar_scan_make_rte(Oid compressed_relid, LOCKMODE lockmode, Query *parse);
static void create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel,
										 const CompressionInfo *compression_info,
										 const SortInfo *sort_info);

static ColumnarScanPath *columnar_scan_path_create(PlannerInfo *root, const CompressionInfo *info,
												   Path *compressed_path);

static void columnar_scan_add_plannerinfo(PlannerInfo *root, CompressionInfo *info,
										  const Chunk *chunk, RelOptInfo *chunk_rel,
										  bool needs_sequence_num);

static SortInfo build_sortinfo(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
							   const CompressionInfo *info, List *pathkeys);

static Bitmapset *find_const_segmentby(RelOptInfo *chunk_rel, const CompressionInfo *info);

static EquivalenceClass *
append_ec_for_seqnum(PlannerInfo *root, const CompressionInfo *info, const SortInfo *sort_info,
					 Var *var, Oid sortop, bool nulls_first)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(root->planner_cxt);

	Oid opfamily, opcintype, equality_op;
	CompareType strategy;
	List *opfamilies;
	EquivalenceClass *newec = makeNode(EquivalenceClass);
	EquivalenceMember *em = makeNode(EquivalenceMember);

	/* Find the operator in pg_amop --- failure shouldn't happen */
	if (!get_ordering_op_properties(sortop, &opfamily, &opcintype, &strategy))
		elog(ERROR, "operator %u is not a valid ordering operator", sortop);

	/*
	 * EquivalenceClasses need to contain opfamily lists based on the family
	 * membership of mergejoinable equality operators, which could belong to
	 * more than one opfamily.  So we have to look up the opfamily's equality
	 * operator and get its membership.
	 */
	equality_op = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
	if (!OidIsValid(equality_op)) /* shouldn't happen */
		elog(ERROR,
			 "missing operator %d(%u,%u) in opfamily %u",
			 BTEqualStrategyNumber,
			 opcintype,
			 opcintype,
			 opfamily);
	opfamilies = get_mergejoin_opfamilies(equality_op);
	if (!opfamilies) /* certainly should find some */
		elog(ERROR, "could not find opfamilies for equality operator %u", equality_op);

	em->em_expr = (Expr *) var;
	em->em_relids = bms_make_singleton(info->compressed_rel->relid);
#if PG16_LT
	em->em_nullable_relids = NULL;
#endif
	em->em_is_const = false;
	em->em_is_child = false;
	em->em_datatype = INT4OID;

	newec->ec_opfamilies = list_copy(opfamilies);
	newec->ec_collation = 0;
	newec->ec_members = list_make1(em);
	newec->ec_sources = NIL;
	newec->ec_derives_list = NIL;
	newec->ec_relids = bms_make_singleton(info->compressed_rel->relid);
	newec->ec_has_const = false;
	newec->ec_has_volatile = false;
#if PG16_LT
	newec->ec_below_outer_join = false;
#endif
	newec->ec_broken = false;
	newec->ec_sortref = 0;
	newec->ec_min_security = UINT_MAX;
	newec->ec_max_security = 0;
	newec->ec_merged = NULL;

	info->compressed_rel->eclass_indexes =
		bms_add_member(info->compressed_rel->eclass_indexes, list_length(root->eq_classes));
	root->eq_classes = lappend(root->eq_classes, newec);

	MemoryContextSwitchTo(oldcontext);

	return newec;
}

static EquivalenceClass *
append_ec_for_metadata_col(PlannerInfo *root, const CompressionInfo *info, Var *var, PathKey *pk)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(root->planner_cxt);
	EquivalenceMember *em = makeNode(EquivalenceMember);

	em->em_expr = (Expr *) var;
	em->em_relids = bms_make_singleton(info->compressed_rel->relid);
	em->em_is_const = false;
	em->em_is_child = false;
	em->em_datatype = var->vartype;
	EquivalenceClass *ec = makeNode(EquivalenceClass);
	ec->ec_opfamilies = pk->pk_eclass->ec_opfamilies;
	ec->ec_collation = pk->pk_eclass->ec_collation;
	ec->ec_members = list_make1(em);
	ec->ec_sources = list_copy(pk->pk_eclass->ec_sources);
	ec->ec_derives_list = list_copy(pk->pk_eclass->ec_derives_list);
	ec->ec_relids = bms_make_singleton(info->compressed_rel->relid);
	ec->ec_has_const = pk->pk_eclass->ec_has_const;
	ec->ec_has_volatile = pk->pk_eclass->ec_has_volatile;
#if PG16_LT
	ec->ec_below_outer_join = pk->pk_eclass->ec_below_outer_join;
#endif
	ec->ec_broken = pk->pk_eclass->ec_broken;
	ec->ec_sortref = pk->pk_eclass->ec_sortref;
	ec->ec_min_security = pk->pk_eclass->ec_min_security;
	ec->ec_max_security = pk->pk_eclass->ec_max_security;
	ec->ec_merged = pk->pk_eclass->ec_merged;
	root->eq_classes = lappend(root->eq_classes, ec);
	MemoryContextSwitchTo(oldcontext);
	info->compressed_rel->eclass_indexes =
		bms_add_member(info->compressed_rel->eclass_indexes, root->eq_classes->length - 1);

	return ec;
}

static List *
build_compressed_scan_pathkeys(const SortInfo *sort_info, PlannerInfo *root, List *chunk_pathkeys,
							   const CompressionInfo *info)
{
	Var *var;
	int varattno;
	List *required_compressed_pathkeys = NIL;
	ListCell *lc = NULL;
	PathKey *pk;

	/*
	 * all segmentby columns need to be prefix of pathkeys
	 * except those with equality constraint in baserestrictinfo
	 */
	if (info->num_segmentby_columns > 0)
	{
		TimescaleDBPrivate *compressed_fdw_private =
			(TimescaleDBPrivate *) info->compressed_rel->fdw_private;
		/*
		 * We don't need any sorting for the segmentby columns that are equated
		 * to a constant. The respective constant ECs are excluded from
		 * canonical pathkeys, so we won't see these columns here. Count them as
		 * seen from the start, so that we arrive at the proper counts of seen
		 * segmentby columns in the end.
		 */
		for (lc = list_head(chunk_pathkeys); lc; lc = lnext(chunk_pathkeys, lc))
		{
			PathKey *pk = lfirst(lc);
			EquivalenceMember *compressed_em = NULL;
			ListCell *ec_em_pair_cell;
			foreach (ec_em_pair_cell, compressed_fdw_private->compressed_ec_em_pairs)
			{
				List *pair = lfirst(ec_em_pair_cell);
				if (linitial(pair) == pk->pk_eclass)
				{
					compressed_em = lsecond(pair);
					break;
				}
			}

			/*
			 * We should exit the loop after we've seen all required segmentby
			 * columns. If we haven't seen them all, but the next pathkey
			 * already refers a compressed column, it is a bug. See
			 * build_sortinfo().
			 */
			if (!compressed_em)
				break;

			required_compressed_pathkeys = lappend(required_compressed_pathkeys, pk);
		}
	}

	/*
	 * If pathkeys contains non-segmentby columns the rest of the ordering
	 * requirements will be satisfied by ordering by sequence_num.
	 */
	if (sort_info->needs_sequence_num)
	{
		/* TODO: split up legacy sequence number path and non-sequence number path into dedicated
		 * functions. */
		if (info->has_seq_num)
		{
			bool nulls_first;
			Oid sortop;
			varattno = get_attnum(info->compressed_rte->relid,
								  COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
			var = makeVar(info->compressed_rel->relid, varattno, INT4OID, -1, InvalidOid, 0);

			if (sort_info->reverse)
			{
				sortop = get_commutator(Int4LessOperator);
				nulls_first = true;
			}
			else
			{
				sortop = Int4LessOperator;
				nulls_first = false;
			}

			/*
			 * Create the EquivalenceClass for the sequence number column of this
			 * compressed chunk, so that we can build the PathKey that refers to it.
			 */
			EquivalenceClass *ec =
				append_ec_for_seqnum(root, info, sort_info, var, sortop, nulls_first);

			/* Find the operator in pg_amop --- failure shouldn't happen. */
			Oid opfamily, opcintype;
			CompareType strategy;
			if (!get_ordering_op_properties(sortop, &opfamily, &opcintype, &strategy))
				elog(ERROR, "operator %u is not a valid ordering operator", sortop);

			pk = make_canonical_pathkey(root, ec, opfamily, strategy, nulls_first);

			required_compressed_pathkeys = lappend(required_compressed_pathkeys, pk);
		}
		else
		{
			/* If there are no segmentby pathkeys, start from the beginning of the list */
			if (info->num_segmentby_columns == 0)
			{
				lc = list_head(chunk_pathkeys);
			}
			Assert(lc != NULL);
			Expr *expr;
			char *column_name;
			for (; lc != NULL; lc = lnext(chunk_pathkeys, lc))
			{
				pk = lfirst(lc);
				expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

				Assert(expr != NULL && IsA(expr, Var));
				var = castNode(Var, expr);
				Assert(var->varattno > 0);

				column_name = get_attname(info->chunk_rte->relid, var->varattno, false);
				int16 orderby_index = ts_array_position(info->settings->fd.orderby, column_name);
				varattno =
					get_attnum(info->compressed_rte->relid, column_segment_min_name(orderby_index));
				Assert(orderby_index != 0);
				bool orderby_desc =
					ts_array_get_element_bool(info->settings->fd.orderby_desc, orderby_index);
				bool orderby_nullsfirst =
					ts_array_get_element_bool(info->settings->fd.orderby_nullsfirst, orderby_index);

				bool nulls_first;
				CompareType strategy;

				if (sort_info->reverse)
				{
					strategy = orderby_desc ? BTLessStrategyNumber : BTGreaterStrategyNumber;
					nulls_first = !orderby_nullsfirst;
				}
				else
				{
					strategy = orderby_desc ? BTGreaterStrategyNumber : BTLessStrategyNumber;
					nulls_first = orderby_nullsfirst;
				}

				var = makeVar(info->compressed_rel->relid,
							  varattno,
							  var->vartype,
							  var->vartypmod,
							  var->varcollid,
							  var->varlevelsup);
				EquivalenceClass *min_ec = append_ec_for_metadata_col(root, info, var, pk);
				PathKey *min =
					make_canonical_pathkey(root, min_ec, pk->pk_opfamily, strategy, nulls_first);
				required_compressed_pathkeys = lappend(required_compressed_pathkeys, min);

				varattno =
					get_attnum(info->compressed_rte->relid, column_segment_max_name(orderby_index));
				var = makeVar(info->compressed_rel->relid,
							  varattno,
							  var->vartype,
							  var->vartypmod,
							  var->varcollid,
							  var->varlevelsup);
				EquivalenceClass *max_ec = append_ec_for_metadata_col(root, info, var, pk);
				PathKey *max =
					make_canonical_pathkey(root, max_ec, pk->pk_opfamily, strategy, nulls_first);

				required_compressed_pathkeys = lappend(required_compressed_pathkeys, max);
			}
		}
	}
	return required_compressed_pathkeys;
}

ColumnarScanPath *
copy_columnar_scan_path(ColumnarScanPath *src)
{
	Assert(ts_is_columnar_scan_path(&src->custom_path.path));

	ColumnarScanPath *dst = palloc(sizeof(ColumnarScanPath));
	memcpy(dst, src, sizeof(ColumnarScanPath));

	return dst;
}

/*
 * Maps the attno of the min metadata column in the compressed chunk to the
 * attno of the corresponding max metadata column. Zero if none or not applicable.
 */
typedef struct SelectivityEstimationContext
{
	AttrNumber *min_to_max;
	AttrNumber *max_to_min;

	List *vars;
} SelectivityEstimationContext;

/*
 * Collect the Vars referencing the "min" metadata columns into the context->vars.
 */
static bool
min_metadata_vars_collector(Node *orig_node, SelectivityEstimationContext *context)
{
	if (orig_node == NULL)
	{
		/*
		 * An expression node can have a NULL field and the mutator will be
		 * still called for it, so we have to handle this.
		 */
		return false;
	}

	if (!IsA(orig_node, Var))
	{
		/*
		 * Recurse.
		 */
		return expression_tree_walker(orig_node, min_metadata_vars_collector, context);
	}

	Var *orig_var = castNode(Var, orig_node);
	if (orig_var->varattno <= 0)
	{
		/*
		 * We don't handle special variables. Not sure how it could happen though.
		 */
		return false;
	}

	AttrNumber replaced_attno = context->min_to_max[orig_var->varattno];
	if (replaced_attno == InvalidAttrNumber)
	{
		/*
		 * No replacement for this column.
		 */
		return false;
	}

	context->vars = lappend(context->vars, orig_var);
	return false;
}

static void
set_compressed_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
									  CompressionInfo *compression_info)
{
	/*
	 * We need some custom selectivity estimation code for the compressed chunk
	 * table, because some pushed down filters require special handling.
	 *
	 * An equality condition can be pushed down to the minmax sparse index
	 * condition, and becomes x_min <= const and const <= x_max. Postgres
	 * treats the part of this condition as independent, which leads to
	 * significant overestimates when x has high cardinality, and therefore
	 * not using the Index Scan. This stems from the fact that Postgres doesn't
	 * know that x_max is always just very slightly more than x_min for the
	 * given compressed batch.

	 * To work around this, temporarily replace all conditions on x_min with
	 * conditions on x_max before feeding them to the Postgres clauselist
	 * selectivity functions. Since the range of x_min to x_max for a given
	 * batch is small relative to the range of x in the entire chunk, this
	 * should not introduce much error, but at the same time allow Postgres to
	 * see the correlation.
	 *
	 * We do this here for the entire baserestrictinfo and not per-rinfo as we
	 * add them during filter pushdown, because the Postgres clauselist
	 * selectivity estimator must see the entire clause list to detect the range
	 * conditions.
	 *
	 * First, build the correspondence of min metadata attno -> max metadata
	 * attno for all minmax metadata.
	 */
	AttrNumber *storage =
		palloc0(2 * sizeof(AttrNumber) * compression_info->compressed_rel->max_attr);
	SelectivityEstimationContext context = {
		.min_to_max = &storage[0],
		.max_to_min = &storage[compression_info->compressed_rel->max_attr],
	};

	for (int uncompressed_attno = 1; uncompressed_attno <= compression_info->chunk_rel->max_attr;
		 uncompressed_attno++)
	{
		if (get_rte_attribute_is_dropped(compression_info->chunk_rte, uncompressed_attno))
		{
			/* Skip the dropped column. */
			continue;
		}

		const char *attname = get_attname(compression_info->chunk_rte->relid,
										  uncompressed_attno,
										  /* missing_ok = */ false);
		const int16 orderby_pos =
			ts_array_position(compression_info->settings->fd.orderby, attname);

		if (orderby_pos == 0)
		{
			/*
			 * This reasoning is only applicable to orderby columns, where each
			 * batch is a thin slice of the entire range of the column. It also does
			 * not have many intersections, because the compressed batches mostly
			 * follow the total order of orderby columns, that is relaxed for the
			 * last orderby  columns or unordered chunks.This does not necessarily
			 * hold for non-orderby columns that can also have a sparse index.
			 */
			continue;
		}

		AttrNumber min_attno =
			compressed_column_metadata_attno(compression_info->settings,
											 compression_info->chunk_rte->relid,
											 uncompressed_attno,
											 compression_info->compressed_rte->relid,
											 "min");
		AttrNumber max_attno =
			compressed_column_metadata_attno(compression_info->settings,
											 compression_info->chunk_rte->relid,
											 uncompressed_attno,
											 compression_info->compressed_rte->relid,
											 "max");

		if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
		{
			continue;
		}

		context.min_to_max[min_attno] = max_attno;
		context.max_to_min[max_attno] = min_attno;
	}

	/*
	 * Then, replace all conditions on min metadata column with conditions on
	 * max metadata column.
	 */
	ListCell *lc;
	foreach (lc, rel->baserestrictinfo)
	{
		RestrictInfo *orig_restrictinfo = castNode(RestrictInfo, lfirst(lc));
		Node *orig_clause = (Node *) orig_restrictinfo->clause;
		expression_tree_walker(orig_clause, min_metadata_vars_collector, &context);
	}

	/*
	 * Temporarily replace "min" with "max" in-place to save on memory allocations.
	 */
	foreach (lc, context.vars)
	{
		Var *var = castNode(Var, lfirst(lc));

		Assert(var->varattno != InvalidAttrNumber);
		Assert(context.min_to_max[var->varattno] != InvalidAttrNumber);
		Assert(context.max_to_min[context.min_to_max[var->varattno]] == var->varattno);

		var->varattno = context.min_to_max[var->varattno];
	}

	/*
	 * Compute selectivity with the updated filters.
	 */
	set_baserel_size_estimates(root, rel);

	/*
	 * Replace the Vars back.
	 */
	foreach (lc, context.vars)
	{
		Var *var = castNode(Var, lfirst(lc));
		var->varattno = context.max_to_min[var->varattno];
	}

	pfree(storage);
}

static CompressionInfo *
build_compressioninfo(PlannerInfo *root, const Hypertable *ht, const Chunk *chunk,
					  RelOptInfo *chunk_rel)
{
	AppendRelInfo *appinfo;
	CompressionInfo *info = palloc0(sizeof(CompressionInfo));

	info->compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	info->chunk_rel = chunk_rel;
	info->chunk_rte = planner_rt_fetch(chunk_rel->relid, root);
	info->settings = ts_compression_settings_get(chunk->table_id);

	if (chunk_rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		appinfo = ts_get_appendrelinfo(root, chunk_rel->relid, false);
		RangeTblEntry *rte = planner_rt_fetch(appinfo->parent_relid, root);
		if (rte->rtekind == RTE_RELATION)
		{
			info->ht_rte = rte;
			info->ht_rel = root->simple_rel_array[appinfo->parent_relid];
		}
		else
		{
			/* In UNION queries referencing chunks directly, the parent rel can be a subquery */
			Assert(rte->rtekind == RTE_SUBQUERY);
			info->single_chunk = true;
			info->ht_rte = info->chunk_rte;
			info->ht_rel = info->chunk_rel;
		}
	}
	else
	{
		Assert(chunk_rel->reloptkind == RELOPT_BASEREL);
		info->single_chunk = true;
		info->ht_rte = info->chunk_rte;
		info->ht_rel = info->chunk_rel;
	}

	info->hypertable_id = ht->fd.id;

	info->num_orderby_columns = ts_array_length(info->settings->fd.orderby);
	info->num_segmentby_columns = ts_array_length(info->settings->fd.segmentby);

	if (info->num_segmentby_columns)
	{
		ArrayIterator it = array_create_iterator(info->settings->fd.segmentby, 0, NULL);
		Datum datum;
		bool isnull;
		while (array_iterate(it, &datum, &isnull))
		{
			Ensure(!isnull, "NULL element in catalog array");
			AttrNumber chunk_attno = get_attnum(info->chunk_rte->relid, TextDatumGetCString(datum));
			info->chunk_segmentby_attnos =
				bms_add_member(info->chunk_segmentby_attnos, chunk_attno);
		}
	}

	info->has_seq_num =
		get_attnum(info->settings->fd.compress_relid,
				   COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) != InvalidAttrNumber;

	info->chunk_const_segmentby = find_const_segmentby(chunk_rel, info);

	/*
	 * If the chunk is member of hypertable expansion or a UNION, find its
	 * parent relation ids. We will use it later to filter out some parameterized
	 * paths.
	 */
	if (chunk_rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		info->parent_relids = find_childrel_parents(root, chunk_rel);
	}

	return info;
}

/*
 * Estimate the average count of elements in the compressed batch based on the
 * Postgres statistics for _ts_meta_count column.
 * Returns TARGET_COMPRESSED_BATCH_SIZE when no pg_statistic entry exists.
 */
static double
estimate_compressed_batch_size(PlannerInfo *root, const CompressionInfo *compression_info)
{
	AttrNumber attnum = get_attnum(compression_info->compressed_rte->relid, "_ts_meta_count");
	if (attnum == InvalidAttrNumber)
		return TARGET_COMPRESSED_BATCH_SIZE;

	Var *var = makeVar(compression_info->compressed_rel->relid, attnum, INT4OID, -1, InvalidOid, 0);

	/* fetch statistics */
	VariableStatData vardata;
	examine_variable(root, (Node *) var, 0, &vardata);

	if (!HeapTupleIsValid(vardata.statsTuple))
	{
		ReleaseVariableStats(vardata);
		return TARGET_COMPRESSED_BATCH_SIZE;
	}

	double mcv_sum = 0.0;
	double mcv_freq = 0.0;

	/* exact MCV contribution */
	AttStatsSlot mcvslot;
	if (get_attstatsslot(&mcvslot,
						 vardata.statsTuple,
						 STATISTIC_KIND_MCV,
						 InvalidOid,
						 ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
	{
		for (int i = 0; i < mcvslot.nvalues; i++)
		{
			double val = (double) DatumGetInt32(mcvslot.values[i]);
			double freq = (double) mcvslot.numbers[i];
			mcv_sum += val * freq;
			mcv_freq += freq;
		}
		free_attstatsslot(&mcvslot);
	}

	double hist_sum = 0.0;

	/* histogram contribution */
	AttStatsSlot histslot;
	if (get_attstatsslot(&histslot,
						 vardata.statsTuple,
						 STATISTIC_KIND_HISTOGRAM,
						 InvalidOid,
						 ATTSTATSSLOT_VALUES))
	{
		int buckets = histslot.nvalues - 1;

		if (buckets > 0 && mcv_freq < 1.0)
		{
			for (int i = 0; i < buckets; i++)
			{
				double lo = (double) DatumGetInt32(histslot.values[i]);
				double hi = (double) DatumGetInt32(histslot.values[i + 1]);
				hist_sum += (lo + hi) / 2.0;
			}
			hist_sum *= (1.0 - mcv_freq) / buckets;
		}
		free_attstatsslot(&histslot);
	}

	ReleaseVariableStats(vardata);

	const double final_result = mcv_sum + hist_sum;
	if (final_result == 0)
	{
		/*
		 * For tables with few rows, the statistics tuple will contain all zero
		 * values. We shouldn't return zero in this case to avoid weird behavior.
		 */
		return TARGET_COMPRESSED_BATCH_SIZE;
	}

	return final_result;
}

/*
 * calculate cost for ColumnarScanPath
 *
 * since we have to read whole batch before producing tuple
 * we put cost of 1 tuple of compressed_scan as startup cost
 */
static void
cost_columnar_scan(PlannerInfo *root, const CompressionInfo *compression_info, Path *path,
				   Path *compressed_path)
{
	/* startup_cost is cost before fetching first tuple */
	const double compressed_rows = Max(1, compressed_path->rows);
	path->startup_cost =
		compressed_path->startup_cost +
		(compressed_path->total_cost - compressed_path->startup_cost) / compressed_rows;

	/* total_cost is cost for fetching all tuples */
	path->rows = compressed_path->rows * compression_info->compressed_batch_size;
	path->total_cost = compressed_path->total_cost + path->rows * cpu_tuple_cost;

#if PG18_GE
	/* PG18 changes the way we handle disabled nodes so we
	 * need to take those into account as well.
	 *
	 * https://github.com/postgres/postgres/commit/e2225346
	 */
	path->disabled_nodes = compressed_path->disabled_nodes;
#endif
}

/* Smoothstep function S1 (the h01 cubic Hermite spline). */
static double
smoothstep(double x, double start, double end)
{
	x = (x - start) / (end - start);

	if (x < 0)
	{
		x = 0;
	}
	else if (x > 1)
	{
		x = 1;
	}

	return x * x * (3.0F - 2.0F * x);
}

/*
 * If the query 'order by' is prefix of the compression 'order by' (or equal), we can exploit
 * the ordering of the individual batches to create a total ordered result without resorting
 * the tuples. This speeds up all queries that use this ordering (because no sort node is
 * needed). In particular, queries that use a LIMIT are speed-up because only the top elements
 * of the affected batches needs to be decompressed. Without the optimization, the entire batches
 * are decompressed, sorted, and then the top elements are taken from the result.
 *
 * The idea is to do something similar to the MergeAppend node; a BinaryHeap is used
 * to merge the per segment by column sorted individual batches into a sorted result. So, we end
 * up which a data flow which looks as follows:
 *
 * ColumnarScan
 *   * Decompress Batch 1
 *   * Decompress Batch 2
 *   * Decompress Batch 3
 *       [....]
 *   * Decompress Batch N
 *
 * Using the presorted batches, we are able to open these batches dynamically. If we don't presort
 * them, we would have to open all batches at the same time. This would be similar to the work the
 * MergeAppend does, but this is not needed in our case and we could reduce the size of the heap and
 * the amount of parallel open batches.
 *
 * The algorithm works as follows:
 *
 *   (1) A sort node is placed below the decompress scan node and on top of the scan
 *       on the compressed chunk. This sort node uses the min/max values of the 'order by'
 *       columns from the metadata of the batch to get them into an order which can be
 *       used to merge them.
 *
 *       [Scan on compressed chunk] -> [Sort on min/max values] -> [Decompress and merge]
 *
 *       For example, the batches are sorted on the min value of the 'order by' metadata
 *       column: [0, 3] [0, 5] [3, 7] [6, 10]
 *
 *   (2) The decompress chunk node initializes a binary heap, opens the first batch and
 *       decompresses the first tuple from the batch. The tuple is put on the heap. In addition
 *       the opened batch is marked as the most recent batch (MRB).
 *
 *   (3) As soon as a tuple is requested from the heap, the following steps are performed:
 *       (3a) If the heap is empty, we are done.
 *       (3b) The top tuple from the heap is taken. It is checked if this tuple is from the
 *            MRB. If this is the case, the next batch is opened, the first tuple is decompressed,
 *            placed on the heap and this batch is marked as MRB. This is repeated until the
 *            top tuple from the heap is not from the MRB. After the top tuple is not from the
 *            MRB, all batches (and one ahead) which might contain the most recent tuple are
 *            opened and placed on the heap.
 *
 *            In the example above, the first three batches are opened because the first two
 *            batches might contain tuples with a value of 0.
 *       (3c) The top element from the heap is removed, the next tuple from the batch is
 *            decompressed (if present) and placed on the heap.
 *       (3d) The former top tuple of the heap is returned.
 *
 * This function calculate the costs for retrieving the decompressed in-order
 * using a binary heap.
 */
static void
cost_batch_sorted_merge(PlannerInfo *root, const CompressionInfo *compression_info,
						ColumnarScanPath *dcpath, Path *compressed_path)
{
	Path sort_path; /* dummy for result of cost_sort */

	/*
	 * Don't disable the compressed batch sorted merge plan with the enable_sort
	 * GUC. We have a separate GUC for it, and this way you can try to force the
	 * batch sorted merge plan by disabling sort.
	 */
	const bool old_enable_sort = enable_sort;
	enable_sort = true;
	cost_sort(&sort_path,
			  root,
			  dcpath->required_compressed_pathkeys,
#if PG18_GE
			  compressed_path->disabled_nodes,
#endif
			  compressed_path->total_cost,
			  compressed_path->rows,
			  compressed_path->pathtarget->width,
			  0.0,
			  work_mem,
			  -1);
	enable_sort = old_enable_sort;

	/*
	 * In compressed batch sorted merge, for each distinct segmentby value we
	 * have to keep the corresponding latest batch open. Estimate the number of
	 * these batches with the usual Postgres estimator for grouping cardinality.
	 */
	List *segmentby_groupexprs = NIL;
	for (int segmentby_attno = bms_next_member(compression_info->chunk_segmentby_attnos, -1);
		 segmentby_attno > 0;
		 segmentby_attno =
			 bms_next_member(compression_info->chunk_segmentby_attnos, segmentby_attno))
	{
		char *colname = get_attname(compression_info->chunk_rte->relid,
									segmentby_attno,
									/* missing_ok = */ false);
		AttrNumber compressed_attno = get_attnum(compression_info->compressed_rte->relid, colname);
		Ensure(compressed_attno != InvalidAttrNumber,
			   "segmentby column %s not found in compressed chunk %d",
			   colname,
			   compression_info->compressed_rte->relid);
		Var *var = palloc(sizeof(Var));
		*var = (Var){ .xpr.type = T_Var,
					  .varno = compression_info->compressed_rel->relid,
					  .varattno = compressed_attno };
		segmentby_groupexprs = lappend(segmentby_groupexprs, var);
	}
	const double open_batches_estimated =
		estimate_num_groups(root, segmentby_groupexprs, dcpath->custom_path.path.rows, NULL, NULL);
	Assert(open_batches_estimated > 0);

	/*
	 * We can't have more open batches than the total number of compressed rows,
	 * so clamp it for sanity of the following calculations.
	 */
	const double open_batches_clamped = Min(open_batches_estimated, sort_path.rows);

	/*
	 * Keeping a lot of batches open might use a lot of memory. The batch sorted
	 * merge can't offload anything to disk, so we just penalize it heavily if
	 * we expect it to go over the work_mem. First, estimate the amount of
	 * memory we'll need. We do this on the basis of uncompressed chunk width,
	 * as if we had to materialize entire decompressed batches. This might
	 * be less precise when bulk decompression is not used, because we
	 * materialize only the compressed data which is smaller. But it accounts
	 * for projections, which is probably more important than precision, because
	 * we often read a small subset of columns in analytical queries. The
	 * compressed chunk is never projected so we can't use it for that.
	 */
	const double work_mem_bytes = work_mem * 1024.0;
	const double needed_memory_bytes = open_batches_clamped *
									   compression_info->compressed_batch_size *
									   dcpath->custom_path.path.pathtarget->width;

	/*
	 * Next, calculate the cost penalty. It is a smooth step, starting at 75% of
	 * work_mem, and ending at 125%. We want to effectively disable this plan
	 * if it doesn't fit into the available memory, so the penalty should be
	 * comparable to disable_cost but still less than it, so that the
	 * manual disables still have priority.
	 */
	const double work_mem_penalty =
		0.1 * disable_cost *
		smoothstep(needed_memory_bytes, 0.75 * work_mem_bytes, 1.25 * work_mem_bytes);
	Assert(work_mem_penalty >= 0);

	/*
	 * startup_cost is cost before fetching first tuple. Batch sorted merge has
	 * to load at least the number of batches we expect to be open
	 * simultaneously, before it can produce the first row.
	 */
	const double sort_path_cost_for_startup =
		sort_path.startup_cost +
		((sort_path.total_cost - sort_path.startup_cost) * (open_batches_clamped / sort_path.rows));
	Assert(sort_path_cost_for_startup >= 0);
	dcpath->custom_path.path.startup_cost = sort_path_cost_for_startup + work_mem_penalty;

	/*
	 * Finally, to run this path to completion, we have to complete the
	 * underlying sort path, and return all uncompressed rows. Getting one
	 * uncompressed row involves replacing the top row in the heap, which costs
	 * O(log(heap size)). The constant multiplier is found empirically by
	 * benchmarking the queries returning 1 - 1e9 tuples, with segmentby
	 * cardinality 1 to 1e4, and adjusting the cost so that the fastest plan is
	 * used. The "+ 1" under the logarithm is to avoid zero uncompressed row cost
	 * when we expect to have only 1 batch open.
	 */
	const double sort_path_cost_rest = sort_path.total_cost - sort_path_cost_for_startup;
	Assert(sort_path_cost_rest >= 0);
	const double uncompressed_row_cost = 1.5 * log(open_batches_clamped + 1) * cpu_tuple_cost;
	Assert(uncompressed_row_cost > 0);
	dcpath->custom_path.path.total_cost = dcpath->custom_path.path.startup_cost +
										  sort_path_cost_rest +
										  dcpath->custom_path.path.rows * uncompressed_row_cost;

#if PG18_GE
	/* PG18 changes the way we handle disabled nodes so we
	 * need to take those into account as well.
	 *
	 * https://github.com/postgres/postgres/commit/e2225346
	 */
	dcpath->custom_path.path.disabled_nodes = sort_path.disabled_nodes;
#endif
}

/*
 * This function adds per-chunk sorted paths for compressed chunks if beneficial. This has two
 * advantages:
 *
 *  (1) Make ChunkAppend possible. If at least one chunk of a hypertable is uncompressed, PostgreSQL
 * will generate a MergeAppend path in generate_orderedappend_paths() / create_merge_append_path()
 * due to the existing pathkeys of the index on the uncompressed chunk. If all chunks are
 * compressed, no path keys are present and no MergeAppend path is generated by PostgreSQL. In that
 * case, the ChunkAppend optimization cannot be used because MergeAppend path can be promoted in
 * ts_chunk_append_path_create(). Adding a sorted path with pathkeys makes ChunkAppend possible for
 * these queries.
 *
 *  (2) Sorting on a per-chunk basis and merging / appending these results could be faster than
 * sorting the whole input. Especially limit queries that use an ORDER BY that is compatible with
 * the partitioning of the hypertable could be inefficiently executed otherwise. For example, an
 * expensive query plan with a sort node on top of the append node could be chosen. Due to the sort
 * node at the high level in the query plan and the missing ChunkAppend node (see (1)), all chunks
 * are decompressed (instead of only the actually needed ones).
 *
 * If existing index pathkeys do not match query pathkeys and sort cannot be pushed down
 * into compressed index, for example "SELECT * FROM ... ORDER BY (segcol, time DESC, some_col)" if
 * compressed index is (segcol, time DESC), we should  allow SortPath over (ColumnarScan
 * <- IndexScan) for such cases, i.e. should consider IndexScan compressed paths along with SeqScan
 * compressed paths. IndexScans with useful index conditions can be cheaper than SeqScans.
 *
 * The logic is inspired by PostgreSQL's add_paths_with_pathkeys_for_rel() function.
 *
 * Note: This function adds only non-partial paths. In parallel plans PostgreSQL prefers sorting
 * directly under the gather (merge) node and the per-chunk sorting are not used in parallel plans.
 * To save planning time, we therefore refrain from adding them.
 */
static Path *
make_chunk_sorted_path(PlannerInfo *root, RelOptInfo *chunk_rel, Path *path, Path *compressed_path,
					   const SortInfo *sort_info)
{
	/*
	 * Don't have a useful sorting after decompression.
	 */
	if (sort_info->decompressed_sort_pathkeys == NIL)
	{
		return NULL;
	}

	Assert(ts_is_columnar_scan_path(path));

	/*
	 * We should be given an unsorted ColumnarScan path.
	 */
	Assert(path->pathkeys == NIL);

	/*
	 * Create the sorted path for these useful_pathkeys. Copy the decompress
	 * chunk path because the original can be recycled in add_path, and our
	 * sorted path must be independent.
	 */
	ColumnarScanPath *path_copy = copy_columnar_scan_path((ColumnarScanPath *) path);

	/*
	 * Create the Sort path.
	 */
	Path *sorted_path = (Path *) create_sort_path(root,
												  chunk_rel,
												  (Path *) path_copy,
												  sort_info->decompressed_sort_pathkeys,
												  root->limit_tuples);

	/* Set in "create_sort_path" in PG18GE, have to set separately for PG17LE.
	 * Need to preserve info for sort over parametrized index paths. */
	sorted_path->param_info = path->param_info;

	/*
	 * Now, we need another dumb workaround for Postgres problems. When creating
	 * a sort plan, it performs a linear search of equivalence member of a
	 * pathkey's equivalence class, that matches the sorted relation (see
	 * prepare_sort_from_pathkeys()). This is effectively quadratic in the
	 * number of chunks, and becomes a real CPU sink after we pass 1k chunks.
	 * Try to reflect this in the costs, because in some cases a chunk-wise sort
	 * might be avoided, e.g. Limit 1 over MergeAppend over chunk-wise Sort can
	 * be just as well replaced with a Limit 1 over Sort over Append of chunks,
	 * that is just marginally costlier.
	 *
	 * We can't easily know the number of chunks in the query here, so add some
	 * startup cost that is quadratic in the current chunk index, which
	 * hopefully should be a good enough replacement.
	 */
	const int parent_relindex = bms_next_member(chunk_rel->top_parent_relids, -1);
	if (parent_relindex)
	{
		const int chunk_index = chunk_rel->relid - parent_relindex;
		sorted_path->startup_cost += cpu_operator_cost * chunk_index * chunk_index;
		sorted_path->total_cost += cpu_operator_cost * chunk_index * chunk_index;
	}

	return sorted_path;
}

static List *build_on_single_compressed_path(PlannerInfo *root, const Chunk *chunk,
											 RelOptInfo *chunk_rel, Path *compressed_path,
											 bool add_uncompressed_part,
											 List *uncompressed_table_pathlist,
											 const SortInfo *sort_info,
											 const CompressionInfo *compression_info);

void
ts_columnar_scan_generate_paths(PlannerInfo *root, RelOptInfo *chunk_rel, const Hypertable *ht,
								const Chunk *chunk)
{
	/*
	 * For UPDATE/DELETE commands, the executor decompresses and brings the rows into
	 * the uncompressed chunk. Therefore, it's necessary to add the scan on the
	 * uncompressed portion.
	 */
	bool add_uncompressed_part = ts_chunk_is_partial(chunk);
	if (ts_chunk_is_compressed(chunk) && ts_cm_functions->decompress_target_segments &&
		!add_uncompressed_part)
	{
		for (PlannerInfo *proot = root->parent_root; proot != NULL && !add_uncompressed_part;
			 proot = proot->parent_root)
		{
			/*
			 * We could additionally check and compare that the relation involved in the subquery
			 * and the DML target relation are one and the same. But these kinds of queries
			 * should be rare.
			 */
			if (proot->parse->commandType == CMD_UPDATE || proot->parse->commandType == CMD_DELETE
#if PG15_GE
				|| proot->parse->commandType == CMD_MERGE
#endif
			)
			{
				add_uncompressed_part = true;
			}
		}
	}

	CompressionInfo *compression_info = build_compressioninfo(root, ht, chunk, chunk_rel);

	/* double check we don't end up here on single chunk queries with ONLY */
	Assert(compression_info->chunk_rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
		   (compression_info->chunk_rel->reloptkind == RELOPT_BASEREL &&
			ts_rte_is_marked_for_expansion(compression_info->chunk_rte)));

	SortInfo sort_info =
		build_sortinfo(root, chunk, chunk_rel, compression_info, root->query_pathkeys);

	Assert(chunk->fd.compressed_chunk_id > 0);

	List *uncompressed_table_pathlist = chunk_rel->pathlist;
	List *uncompressed_table_parallel_pathlist = chunk_rel->partial_pathlist;
	chunk_rel->pathlist = NIL;
	chunk_rel->partial_pathlist = NIL;

	/* add RangeTblEntry and RelOptInfo for compressed chunk */
	columnar_scan_add_plannerinfo(root,
								  compression_info,
								  chunk,
								  chunk_rel,
								  sort_info.needs_sequence_num);

	if (sort_info.use_compressed_sort)
	{
		sort_info.required_compressed_pathkeys =
			build_compressed_scan_pathkeys(&sort_info,
										   root,
										   root->query_pathkeys,
										   compression_info);
	}

	RelOptInfo *compressed_rel = compression_info->compressed_rel;

	// why is consider parallel = true when there is no uncompressed_table_parallel_pathlist
	// compressed_rel->consider_parallel = chunk_rel->consider_parallel;
	compressed_rel->consider_parallel = false;

	/* translate chunk_rel->baserestrictinfo */
	pushdown_quals(root,
				   compression_info->settings,
				   chunk_rel,
				   compressed_rel,
				   add_uncompressed_part);
	/*
	 * Estimate the size of the compressed chunk table.
	 */
	set_compressed_baserel_size_estimates(root, compressed_rel, compression_info);

	/*
	 * Estimate the size of the compressed batch from Postgres
	 * statistics.
	 */
	compression_info->compressed_batch_size =
		estimate_compressed_batch_size(root, compression_info);

	/*
	 * Estimate the size of decompressed chunk based on the compressed chunk.
	 *
	 * The tuple estimates derived from pg_class will be empty, so we have to
	 * compute that based on the compressed relation as well. Wrong estimates
	 * there lead to wrong join order choice and wrong low cost for Sort over
	 * Append, and also different MergeAppend costs on Postgres before 17 due to
	 * a bug there.
	 */
	const double new_row_estimate = compressed_rel->rows * compression_info->compressed_batch_size;
	const double new_tuples_estimate =
		compressed_rel->tuples * compression_info->compressed_batch_size;
	if (!compression_info->single_chunk)
	{
		/*
		 * Adjust the hypertable estimate by the diff of new and old chunk
		 * estimate.
		 */
		AppendRelInfo *chunk_info = ts_get_appendrelinfo(root, chunk_rel->relid, false);
		const Index ht_relid = chunk_info->parent_relid;
		RelOptInfo *hypertable_rel = root->simple_rel_array[ht_relid];
		const double delta = new_row_estimate - chunk_rel->rows;
		hypertable_rel->rows += delta;
		/*
		 * For appendrel, set tuples to the same value as rows,
		 * like set_append_rel_size() does.
		 */
		hypertable_rel->tuples += delta;
	}
	chunk_rel->rows = new_row_estimate;
	chunk_rel->tuples = new_tuples_estimate;

	/*
	 * Create the paths for the compressed chunk table.
	 */
	create_compressed_scan_paths(root, compressed_rel, compression_info, &sort_info);

	/* create non-parallel paths */
	ListCell *compressed_cell;
	foreach (compressed_cell, compressed_rel->pathlist)
	{
		Path *compressed_path = lfirst(compressed_cell);
		List *decompressed_paths = build_on_single_compressed_path(root,
																   chunk,
																   chunk_rel,
																   compressed_path,
																   add_uncompressed_part,
																   uncompressed_table_pathlist,
																   &sort_info,
																   compression_info);

		/*
		 * We want to consider startup costs so that IndexScan is preferred to
		 * sorted SeqScan when we may have a chance to use SkipScan. We consider
		 * startup costs for LIMIT queries, and SkipScan is basically a
		 * "LIMIT 1" query run "ndistinct" times. At this point we don't have
		 * all information to check if SkipScan can be used, but we can narrow
		 * it down.
		 */
		if (!chunk_rel->consider_startup && IsA(compressed_path, IndexPath))
		{
			/* Candidate for SELECT DISTINCT SkipScan */
			if (list_length(root->distinct_pathkeys) == 1
				/* Candidate for DISTINCT aggregate SkipScan */
				|| (root->numOrderedAggs >= 1 && list_length(root->group_pathkeys) == 1))
			{
				chunk_rel->consider_startup = true;
			}
		}

		/*
		 * Add the paths to the chunk relation.
		 */
		ListCell *decompressed_cell;
		foreach (decompressed_cell, decompressed_paths)
		{
			Path *path = lfirst(decompressed_cell);
			add_path(chunk_rel, path);
		}
	}

	/* create parallel paths */
	List *uncompressed_paths_with_parallel =
		list_concat(uncompressed_table_parallel_pathlist, uncompressed_table_pathlist); // why?
	foreach (compressed_cell, compressed_rel->partial_pathlist)
	{
		Path *compressed_path = lfirst(compressed_cell);
		List *decompressed_paths = build_on_single_compressed_path(root,
																   chunk,
																   chunk_rel,
																   compressed_path,
																   add_uncompressed_part,
																   uncompressed_paths_with_parallel,
																   &sort_info,
																   compression_info);
		/*
		 * Add the paths to the chunk relation.
		 */
		ListCell *decompressed_cell;
		foreach (decompressed_cell, decompressed_paths)
		{
			Path *path = lfirst(decompressed_cell);
			add_partial_path(chunk_rel, path);
		}
	}

	/* the chunk_rel now owns the paths, remove them from the compressed_rel so they can't be freed
	 * if it's planned */
	compressed_rel->pathlist = NIL;
	compressed_rel->partial_pathlist = NIL;

	/*
	 * Remove the compressed_rel from planner arrays to prevent it from being
	 * referenced again.
	 */
	root->simple_rel_array[compressed_rel->relid] = NULL;
	root->append_rel_array[compressed_rel->relid] = NULL;

	/* We should never get in the situation with no viable paths. */
	Ensure(chunk_rel->pathlist, "could not create decompression path");
}

/*
 * Add various decompression paths that are possible based on the given
 * compressed path.
 */
static List *
build_on_single_compressed_path(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
								Path *compressed_path, bool add_uncompressed_part,
								List *uncompressed_table_pathlist, const SortInfo *sort_info,
								const CompressionInfo *compression_info)
{
	/*
	 * We skip any BitmapScan parameterized paths here as supporting
	 * those would require fixing up the internal scan. Since we
	 * currently do not do this BitmapScans would be generated
	 * when we have a parameterized path on a compressed column
	 * that would have invalid references due to our
	 * EquivalenceClasses.
	 */
	if (IsA(compressed_path, BitmapHeapPath) && compressed_path->param_info)
		return NIL;

	/*
	 * Filter out all paths that try to JOIN the compressed chunk on the
	 * hypertable or the uncompressed chunk
	 * Ideally, we wouldn't create these paths in the first place.
	 * However, create_join_clause code is called by PG while generating paths for the
	 * compressed_rel via generate_implied_equalities_for_column.
	 * create_join_clause ends up creating rinfo's between compressed_rel and ht because
	 * PG does not know that compressed_rel is related to ht in anyway.
	 * The parent-child relationship between chunk_rel and ht is known
	 * to PG and so it does not try to create meaningless rinfos for that case.
	 */
	if (compressed_path->param_info != NULL)
	{
		if (bms_is_member(chunk_rel->relid, compressed_path->param_info->ppi_req_outer))
			return NIL;

		/* check if this is path made with references between
		 * compressed_rel + hypertable or a nesting subquery.
		 * The latter can happen in the case of UNION queries. see github 2917. This
		 * happens since PG is not aware that the nesting
		 * subquery that references the hypertable is a parent of compressed_rel as well.
		 */
		if (bms_overlap(compression_info->parent_relids,
						compressed_path->param_info->ppi_req_outer))
		{
			return NIL;
		}
	}

	Path *chunk_path_no_sort =
		(Path *) columnar_scan_path_create(root, compression_info, compressed_path);
	List *decompressed_paths = list_make1(chunk_path_no_sort);

	/*
	 * Create a path for the batch sorted merge optimization. This optimization
	 * performs a sorted merge of the involved batches by using a binary heap
	 * and preserving the compression order. This optimization is only
	 * considered if we can't push down the sort to the compressed chunk. If we
	 * can push down the sort, the batches can be directly consumed in this
	 * order and we don't need to use this optimization.
	 */
	if (sort_info->use_batch_sorted_merge && ts_guc_enable_decompression_sorted_merge)
	{
		Assert(!sort_info->use_compressed_sort);

		ColumnarScanPath *path_copy =
			copy_columnar_scan_path((ColumnarScanPath *) chunk_path_no_sort);

		path_copy->reverse = sort_info->reverse;
		path_copy->batch_sorted_merge = true;

		/*
		 * The segment by optimization is only enabled if it can deliver the tuples in the
		 * same order as the query requested it. So, we can just copy the pathkeys of the
		 * query here.
		 */
		path_copy->custom_path.path.pathkeys = sort_info->decompressed_sort_pathkeys;
		cost_batch_sorted_merge(root, compression_info, path_copy, compressed_path);

		if (ts_guc_debug_require_batch_sorted_merge == DRO_Force)
		{
			path_copy->custom_path.path.startup_cost = cpu_tuple_cost;
			path_copy->custom_path.path.total_cost = 2 * cpu_tuple_cost;
		}

		decompressed_paths = lappend(decompressed_paths, path_copy);
	}
	else if (ts_guc_debug_require_batch_sorted_merge == DRO_Require ||
			 ts_guc_debug_require_batch_sorted_merge == DRO_Force)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("debug: batch sorted merge is required but not possible at planning "
						"time")));
	}

	/*
	 * If we can push down the sort below the ColumnarScan node, we set the
	 * pathkeys of the decompress node to the decompressed_sort_pathkeys. We
	 * will determine whether to put an actual sort between the decompression
	 * node and the scan during plan creation.
	 */
	if (sort_info->use_compressed_sort)
	{
		if (pathkeys_contained_in(sort_info->required_compressed_pathkeys,
								  compressed_path->pathkeys))
		{
			/*
			 * The compressed path already has the required ordering. Modify
			 * in place the no-sorting path we just created above.
			 */
			ColumnarScanPath *path = (ColumnarScanPath *) chunk_path_no_sort;
			path->reverse = sort_info->reverse;
			path->needs_sequence_num = sort_info->needs_sequence_num;
			path->required_compressed_pathkeys = sort_info->required_compressed_pathkeys;
			path->custom_path.path.pathkeys = sort_info->decompressed_sort_pathkeys;
		}
		else
		{
			/*
			 * We must sort the underlying compressed path to get the
			 * required ordering. Make a copy of no-sorting path and modify
			 * it accordingly
			 */
			ColumnarScanPath *path_copy =
				copy_columnar_scan_path((ColumnarScanPath *) chunk_path_no_sort);
			path_copy->reverse = sort_info->reverse;
			path_copy->needs_sequence_num = sort_info->needs_sequence_num;
			path_copy->required_compressed_pathkeys = sort_info->required_compressed_pathkeys;
			path_copy->custom_path.path.pathkeys = sort_info->decompressed_sort_pathkeys;

			/*
			 * Add costing for a sort. The standard Postgres pattern is to add the cost during
			 * path creation, but not add the sort path itself, that's done during plan
			 * creation. Examples of this in: create_merge_append_path &
			 * create_merge_append_plan
			 */
			Path sort_path; /* dummy for result of cost_sort */

			cost_sort(&sort_path,
					  root,
					  sort_info->required_compressed_pathkeys,
#if PG18_GE
					  compressed_path->disabled_nodes,
#endif
					  compressed_path->total_cost,
					  compressed_path->rows,
					  compressed_path->pathtarget->width,
					  0.0,
					  work_mem,
					  -1);

			cost_columnar_scan(root, compression_info, &path_copy->custom_path.path, &sort_path);

			decompressed_paths = lappend(decompressed_paths, path_copy);
		}
	}

	/*
	 * Also try explicit sort after decompression, if we couldn't push down the
	 * sort. Don't do this for parallel plans, because in this case it is
	 * typically done with Sort under Gather node. This splits the Sort in
	 * per-worker buckets, so splitting the buckets further per-chunk is less
	 * important.
	 */
	if (!sort_info->use_compressed_sort && chunk_path_no_sort->parallel_workers == 0)
	{
		Path *sort_above_chunk =
			make_chunk_sorted_path(root, chunk_rel, chunk_path_no_sort, compressed_path, sort_info);
		if (sort_above_chunk != NULL)
		{
			decompressed_paths = lappend(decompressed_paths, sort_above_chunk);
		}
	}

	if (!add_uncompressed_part)
	{
		/*
		 * If the chunk has only the compressed part, we're done.
		 */
		return decompressed_paths;
	}

	/*
	 * This is a partially compressed chunk, we have to combine data from
	 * compressed and uncompressed chunk.
	 */
	List *combined_paths = NIL;

	/*
	 * All decompressed paths we've built have the same parameterization since
	 * we're building on a single compressed path. We only inherit the
	 * parameterization from it and don't add our own.
	 */
	Bitmapset *req_outer = PATH_REQ_OUTER(chunk_path_no_sort);

	/*
	 * Look up the uncompressed chunk paths. We might need an unordered path
	 * (SeqScan) and an ordered path (e.g. IndexScan).
	 */
	Path *unordered_uncompressed_path = get_cheapest_path_for_pathkeys(uncompressed_table_pathlist,
																	   NIL,
																	   req_outer,
																	   TOTAL_COST,
																	   false);
	Ensure(unordered_uncompressed_path != NULL,
		   "couldn't find a scan path for uncompressed chunk table");

	Path *ordered_uncompressed_path = NULL;
	if (sort_info->decompressed_sort_pathkeys != NIL)
	{
		ordered_uncompressed_path =
			get_cheapest_path_for_pathkeys(uncompressed_table_pathlist,
										   sort_info->decompressed_sort_pathkeys,
										   req_outer,
										   TOTAL_COST,
										   false);
	}

	/*
	 * All children of an append path are required to have the same parameterization
	 * so we reparameterize here when we couldn't get a path with the parameterization
	 * we need. Reparameterization should always succeed here since uncompressed_path
	 * should always be a scan.
	 */
	if (!bms_equal(req_outer, PATH_REQ_OUTER(unordered_uncompressed_path)))
	{
		unordered_uncompressed_path =
			reparameterize_path(root, unordered_uncompressed_path, req_outer, 1.0);
		Ensure(unordered_uncompressed_path != NULL,
			   "couldn't reparameterize a scan path for uncompressed chunk table");
	}
	if (ordered_uncompressed_path != NULL &&
		!bms_equal(req_outer, PATH_REQ_OUTER(ordered_uncompressed_path)))
	{
		ordered_uncompressed_path =
			reparameterize_path(root, ordered_uncompressed_path, req_outer, 1.0);
	}

	/*
	 * Create plain Append, potentially parallel. It only makes sense for the
	 * unsorted input paths.
	 */
	{
		const int workers = Max(chunk_path_no_sort->parallel_workers,
								unordered_uncompressed_path->parallel_workers);

		List *parallel_paths = NIL;
		List *sequential_paths = NIL;

		if (chunk_path_no_sort->parallel_workers > 0)
		{
			parallel_paths = lappend(parallel_paths, chunk_path_no_sort);
		}
		else
		{
			sequential_paths = lappend(sequential_paths, chunk_path_no_sort);
		}

		if (unordered_uncompressed_path->parallel_workers > 0)
		{
			parallel_paths = lappend(parallel_paths, unordered_uncompressed_path);
		}
		else
		{
			sequential_paths = lappend(sequential_paths, unordered_uncompressed_path);
		}

		Path *plain_append = (Path *) create_append_path(root,
														 chunk_rel,
														 sequential_paths,
														 parallel_paths,
														 /* pathkeys = */ NIL,
														 req_outer,
														 workers,
														 workers > 0,
														 chunk_path_no_sort->rows +
															 unordered_uncompressed_path->rows);

		combined_paths = lappend(combined_paths, plain_append);
	}

	if (sort_info->decompressed_sort_pathkeys == NIL)
	{
		/*
		 * No sorting requested, so we're done after creating the plain Append
		 * above.
		 */
		return combined_paths;
	}

	/*
	 * We require sorting, try MergeAppend.
	 */
	Path *uncompressed_path_for_merge = ordered_uncompressed_path;
	if (uncompressed_path_for_merge == NULL || IsA(uncompressed_path_for_merge, SortPath))
	{
		/*
		 * Don't use explicit Sort as MergeAppend child, because the
		 * MergeAppend adds the required sorting anyway. With the explicit
		 * Sort it still works but performs the pathkey lookups twice, which
		 * leads to planning performance regression.
		 */
		uncompressed_path_for_merge = unordered_uncompressed_path;
	}
	if (uncompressed_path_for_merge->parallel_workers > 0)
	{
		/*
		 * MergeAppend can't be parallel.
		 */
		return combined_paths;
	}

	/*
	 * For Merge Append, we consider:
	 * 1) explicit sorting over decompressed path,
	 * 2) compressed sort pushdown path,
	 * 3) batch sorted merge path.
	 * We have to make a cost-based decision between them (i.e. batch sorted
	 * merge might be more expensive due to memory requirements).
	 */
	ListCell *lc;
	foreach (lc, decompressed_paths)
	{
		Path *decompression_path = lfirst(lc);
		if (decompression_path->parallel_workers > 0)
		{
			/*
			 * MergeAppend can't be parallel.
			 */
			continue;
		}

		if (decompression_path == chunk_path_no_sort)
		{
			/*
			 * We can't use the unsorted decompression path directly because it
			 * doesn't have the sort projection cost workaround.
			 */
			continue;
		}

		if (!bms_is_empty(chunk_rel->lateral_relids) || !bms_is_empty(req_outer))
		{
			/*
			 * Parametrized MergeAppend paths are not supported.
			 */
			continue;
		}

		if (IsA(decompression_path, SortPath))
		{
			/*
			 * We have to remove the explicit Sort, otherwise it will lead to
			 * planning time regression because of double call of
			 * prepare_sort_from_pathkeys() in MergeAppend plan creation. Still,
			 * we have to use the copy of ColumnarScan path that we created
			 * for explicit sorting, because it has the sort projection cost
			 * workaround.
			 */
			decompression_path = castNode(SortPath, decompression_path)->subpath;
		}

		Path *merge_append =
			(Path *) create_merge_append_path(root,
											  chunk_rel,
											  list_make2(decompression_path,
														 uncompressed_path_for_merge),
											  sort_info->decompressed_sort_pathkeys,
											  req_outer);
		combined_paths = lappend(combined_paths, merge_append);
	}

	return combined_paths;
}

/*
 * Add a var for a particular column to the reltarget. attrs_used is a bitmap
 * of which columns we already have in reltarget. We do not add the columns that
 * are already there, and update it after adding something.
 */
static void
compressed_reltarget_add_var_for_column(RelOptInfo *compressed_rel, Oid compressed_relid,
										const char *column_name, Bitmapset **attrs_used)
{
	AttrNumber attnum = get_attnum(compressed_relid, column_name);
	Assert(attnum > 0);

	if (bms_is_member(attnum, *attrs_used))
	{
		/* This column is already in reltarget, we don't need duplicates. */
		return;
	}

	*attrs_used = bms_add_member(*attrs_used, attnum);

	Oid typid, collid;
	int32 typmod;
	get_atttypetypmodcoll(compressed_relid, attnum, &typid, &typmod, &collid);
	compressed_rel->reltarget->exprs =
		lappend(compressed_rel->reltarget->exprs,
				makeVar(compressed_rel->relid, attnum, typid, typmod, collid, 0));
}

/* copy over the vars from the chunk_rel->reltarget to the compressed_rel->reltarget
 * altering the fields that need it
 */
static void
compressed_rel_setup_reltarget(RelOptInfo *compressed_rel, CompressionInfo *info,
							   bool needs_sequence_num)
{
	bool have_whole_row_var = false;
	Bitmapset *attrs_used = NULL;

	Oid compressed_relid = info->compressed_rte->relid;

	/*
	 * We have to decompress three kinds of columns:
	 * 1) output targetlist of the relation,
	 * 2) columns required for the quals (WHERE),
	 * 3) columns required for joins.
	 */
	List *exprs = list_copy(info->chunk_rel->reltarget->exprs);
	ListCell *lc;
	foreach (lc, info->chunk_rel->baserestrictinfo)
	{
		exprs = lappend(exprs, ((RestrictInfo *) lfirst(lc))->clause);
	}
	foreach (lc, info->chunk_rel->joininfo)
	{
		exprs = lappend(exprs, ((RestrictInfo *) lfirst(lc))->clause);
	}

	/*
	 * Now go over the required expressions we prepared above, and add the
	 * required columns to the compressed reltarget.
	 */
	info->compressed_rel->reltarget->exprs = NIL;
	foreach (lc, exprs)
	{
		ListCell *lc2;
		List *chunk_vars = pull_var_clause(lfirst(lc), PVC_RECURSE_PLACEHOLDERS);
		foreach (lc2, chunk_vars)
		{
			char *column_name;
			Var *chunk_var = castNode(Var, lfirst(lc2));

			/* skip vars that aren't from the uncompressed chunk */
			if ((Index) chunk_var->varno != info->chunk_rel->relid)
			{
				continue;
			}

			/*
			 * If there's a system column or whole-row reference, add a whole-
			 * row reference, and we're done.
			 */
			if (chunk_var->varattno <= 0)
			{
				have_whole_row_var = true;
				continue;
			}

			column_name = get_attname(info->chunk_rte->relid, chunk_var->varattno, false);
			compressed_reltarget_add_var_for_column(compressed_rel,
													compressed_relid,
													column_name,
													&attrs_used);

			/* if the column is an orderby, add it's metadata columns too */
			int16 index = ts_array_position(info->settings->fd.orderby, column_name);
			if (index != 0)
			{
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														column_segment_min_name(index),
														&attrs_used);
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														column_segment_max_name(index),
														&attrs_used);
			}
		}
	}

	/* always add the count column */
	compressed_reltarget_add_var_for_column(compressed_rel,
											compressed_relid,
											COMPRESSION_COLUMN_METADATA_COUNT_NAME,
											&attrs_used);

	/* add the sequence number or orderby metadata columns if we try to order by them*/
	if (needs_sequence_num)
	{
		if (info->has_seq_num)
		{
			compressed_reltarget_add_var_for_column(compressed_rel,
													compressed_relid,
													COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
													&attrs_used);
		}
		else
		{
			for (int i = 1; i <= ts_array_length(info->settings->fd.orderby); i++)
			{
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														column_segment_min_name(i),
														&attrs_used);
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														column_segment_max_name(i),
														&attrs_used);
			}
		}
	}

	/*
	 * It doesn't make sense to request a whole-row var from the compressed
	 * chunk scan. If it is requested, just fetch the rest of columns. The
	 * whole-row var will be created by the projection of ColumnarScan node.
	 */
	if (have_whole_row_var)
	{
		for (int i = 1; i <= info->chunk_rel->max_attr; i++)
		{
			char *column_name = get_attname(info->chunk_rte->relid,
											i,
											/* missing_ok = */ false);
			AttrNumber chunk_attno = get_attnum(info->chunk_rte->relid, column_name);
			if (chunk_attno == InvalidAttrNumber)
			{
				/* Skip the dropped column. */
				continue;
			}

			AttrNumber compressed_attno = get_attnum(info->compressed_rte->relid, column_name);
			if (compressed_attno == InvalidAttrNumber)
			{
				elog(ERROR,
					 "column '%s' not found in the compressed chunk '%s'",
					 column_name,
					 get_rel_name(info->compressed_rte->relid));
			}

			if (bms_is_member(compressed_attno, attrs_used))
			{
				continue;
			}

			compressed_reltarget_add_var_for_column(compressed_rel,
													compressed_relid,
													column_name,
													&attrs_used);
		}
	}
}

static Bitmapset *
columnar_scan_adjust_child_relids(Bitmapset *src, int chunk_relid, int compressed_chunk_relid)
{
	Bitmapset *result = NULL;
	if (src != NULL)
	{
		result = bms_copy(src);
		result = bms_del_member(result, chunk_relid);
		result = bms_add_member(result, compressed_chunk_relid);
	}
	return result;
}

/* based on adjust_appendrel_attrs_mutator handling of RestrictInfo */
static Node *
chunk_joininfo_mutator(Node *node, CompressionInfo *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		Var *compress_var = copyObject(var);
		char *column_name;
		AttrNumber compressed_attno;
		if ((Index) var->varno != context->chunk_rel->relid)
			return (Node *) var;

		column_name = get_attname(context->chunk_rte->relid, var->varattno, false);
		compressed_attno = get_attnum(context->compressed_rte->relid, column_name);
		compress_var->varno = context->compressed_rel->relid;
		compress_var->varattno = compressed_attno;

		return (Node *) compress_var;
	}
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *oldinfo = (RestrictInfo *) node;
		RestrictInfo *newinfo = makeNode(RestrictInfo);

		/* Copy all flat-copiable fields */
		memcpy(newinfo, oldinfo, sizeof(RestrictInfo));

		/* Recursively fix the clause itself */
		newinfo->clause = (Expr *) chunk_joininfo_mutator((Node *) oldinfo->clause, context);

		/* and the modified version, if an OR clause */
		newinfo->orclause = (Expr *) chunk_joininfo_mutator((Node *) oldinfo->orclause, context);

		/* adjust relid sets too */
		newinfo->clause_relids = columnar_scan_adjust_child_relids(oldinfo->clause_relids,
																   context->chunk_rel->relid,
																   context->compressed_rel->relid);
		newinfo->required_relids =
			columnar_scan_adjust_child_relids(oldinfo->required_relids,
											  context->chunk_rel->relid,
											  context->compressed_rel->relid);
		newinfo->outer_relids = columnar_scan_adjust_child_relids(oldinfo->outer_relids,
																  context->chunk_rel->relid,
																  context->compressed_rel->relid);
#if PG16_LT
		newinfo->nullable_relids =
			columnar_scan_adjust_child_relids(oldinfo->nullable_relids,
											  context->chunk_rel->relid,
											  context->compressed_rel->relid);
#endif
		newinfo->left_relids = columnar_scan_adjust_child_relids(oldinfo->left_relids,
																 context->chunk_rel->relid,
																 context->compressed_rel->relid);
		newinfo->right_relids = columnar_scan_adjust_child_relids(oldinfo->right_relids,
																  context->chunk_rel->relid,
																  context->compressed_rel->relid);

		newinfo->eval_cost.startup = -1;
		newinfo->norm_selec = -1;
		newinfo->outer_selec = -1;
		newinfo->left_em = NULL;
		newinfo->right_em = NULL;
		newinfo->scansel_cache = NIL;
		newinfo->left_bucketsize = -1;
		newinfo->right_bucketsize = -1;
		newinfo->left_mcvfreq = -1;
		newinfo->right_mcvfreq = -1;
		return (Node *) newinfo;
	}
	return expression_tree_mutator(node, chunk_joininfo_mutator, context);
}

/* Check if the expression references a compressed column in compressed chunk. */
static bool
has_compressed_vars_walker(Node *node, CompressionInfo *info)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		if ((Index) var->varno != info->compressed_rel->relid)
		{
			return false;
		}

		if (var->varattno <= 0)
		{
			/*
			 * Shouldn't see a system var here, might be a whole row var?
			 * In any case, we can't push it down to the compressed scan level.
			 */
			return true;
		}

		if (bms_is_member(var->varattno, info->compressed_attnos_in_compressed_chunk))
		{
			return true;
		}

		return false;
	}

	return expression_tree_walker(node, has_compressed_vars_walker, info);
}

static bool
has_compressed_vars(RestrictInfo *ri, CompressionInfo *info)
{
	return expression_tree_walker((Node *) ri->clause, has_compressed_vars_walker, info);
}

/* translate chunk_rel->joininfo for compressed_rel
 * this is necessary for create_index_path which gets join clauses from
 * rel->joininfo and sets up parameterized paths (in rel->ppilist).
 * ppi_clauses is finally used to add any additional filters on the
 * indexpath when creating a plan in create_indexscan_plan.
 * Otherwise we miss additional filters that need to be applied after
 * the index plan is executed (github issue 1558)
 */
static void
compressed_rel_setup_joininfo(RelOptInfo *compressed_rel, CompressionInfo *info)
{
	RelOptInfo *chunk_rel = info->chunk_rel;
	ListCell *lc;
	List *compress_joininfo = NIL;
	foreach (lc, chunk_rel->joininfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		RestrictInfo *adjusted = (RestrictInfo *) chunk_joininfo_mutator((Node *) ri, info);
		Assert(IsA(adjusted, RestrictInfo));

		if (has_compressed_vars(adjusted, info))
		{
			/*
			 * We can't check clauses that refer to compressed columns during
			 * the compressed scan.
			 */
			continue;
		}

		compress_joininfo = lappend(compress_joininfo, adjusted);
	}
	compressed_rel->joininfo = compress_joininfo;
}

typedef struct EMCreationContext
{
	Oid uncompressed_relid;
	Oid compressed_relid;
	Index uncompressed_relid_idx;
	Index compressed_relid_idx;
	CompressionSettings *settings;
} EMCreationContext;

static Node *
create_var_for_compressed_equivalence_member(Var *var, const EMCreationContext *context,
											 const char *attname)
{
	/* based on adjust_appendrel_attrs_mutator */
	Assert((Index) var->varno == context->uncompressed_relid_idx);
	Assert(var->varattno > 0);

	var = (Var *) copyObject(var);

	if (var->varlevelsup == 0)
	{
		var->varno = context->compressed_relid_idx;
		var->varattno = get_attnum(context->compressed_relid, attname);
		var->varnosyn = var->varno;
		var->varattnosyn = var->varattno;

		return (Node *) var;
	}

	return NULL;
}

/* This function is inspired by the Postgres add_child_rel_equivalences. */
static bool
add_segmentby_to_equivalence_class(PlannerInfo *root, EquivalenceClass *cur_ec,
								   CompressionInfo *info, EMCreationContext *context)
{
	TimescaleDBPrivate *compressed_fdw_private =
		(TimescaleDBPrivate *) info->compressed_rel->fdw_private;
	Assert(compressed_fdw_private != NULL);

	EquivalenceMember *cur_em;
#if PG18_GE
	/* Use specialized iterator to include child ems.
	 *
	 * https://github.com/postgres/postgres/commit/d69d45a5
	 */
	EquivalenceMemberIterator it;

	setup_eclass_member_iterator(&it, cur_ec, bms_make_singleton(info->chunk_rel->relid));
	while ((cur_em = eclass_member_iterator_next(&it)) != NULL)
	{
#else
	ListCell *lc;
	foreach (lc, cur_ec->ec_members)
	{
		cur_em = (EquivalenceMember *) lfirst(lc);
#endif
		Expr *child_expr;
		Relids new_relids;
		Var *var;
		Assert(!bms_overlap(cur_em->em_relids, info->compressed_rel->relids));

		/* only consider EquivalenceMembers that are Vars, possibly with RelabelType, of the
		 * uncompressed chunk */
		var = (Var *) cur_em->em_expr;
		while (var && IsA(var, RelabelType))
			var = (Var *) ((RelabelType *) var)->arg;
		if (!(var && IsA(var, Var)))
			continue;

		/*
		 * We want to base our equivalence member on the hypertable equivalence
		 * member, not on the uncompressed chunk one. We can't just check for
		 * em_is_child though because the hypertable might be a child itself and not
		 * a top-level EquivalenceMember. This is mostly relevant for PG16+ where
		 * we have to specify a parent for the newly created equivalence member.
		 */
		if ((Index) var->varno != info->ht_rel->relid)
			continue;

		if (var->varattno <= 0)
		{
			/*
			 * We can have equivalence members that refer to special variables,
			 * but these variables can't be segmentby, so we're not interested
			 * in them here.
			 */
			continue;
		}

		/* given that the em is a var of the uncompressed chunk, the relid of the chunk should
		 * be set on the em */
		Assert(bms_is_member(info->ht_rel->relid, cur_em->em_relids));
		Assert(OidIsValid(info->ht_rte->relid));

		const char *attname = get_attname(info->ht_rte->relid, var->varattno, false);

		if (!ts_array_is_member(context->settings->fd.segmentby, attname))
			continue;

		child_expr = (Expr *) create_var_for_compressed_equivalence_member(var, context, attname);
		if (child_expr == NULL)
			continue;

		/* #8681: coerce compressed var to current equivalence member type/collation,
		 *  in case we dug the "cur_em->em_expr" var from under RelabelTypes
		 */
		child_expr =
			canonicalize_ec_expression(child_expr, cur_em->em_datatype, cur_ec->ec_collation);

		/*
		 * Transform em_relids to match.  Note we do *not* do
		 * pull_varnos(child_expr) here, as for example the
		 * transformation might have substituted a constant, but we
		 * don't want the child member to be marked as constant.
		 */
		new_relids = bms_copy(cur_em->em_relids);
		new_relids = bms_del_member(new_relids, info->ht_rel->relid);
		new_relids = bms_add_members(new_relids, info->compressed_rel->relids);

		/* copied from add_eq_member */
		{
			EquivalenceMember *em = makeNode(EquivalenceMember);

			em->em_expr = child_expr;
			em->em_relids = new_relids;
			em->em_is_const = false;
			em->em_is_child = true;
			em->em_datatype = cur_em->em_datatype;
#if PG16_GE
			em->em_jdomain = cur_em->em_jdomain;
			em->em_parent = cur_em;
#endif

#if PG16_LT
			/*
			 * For versions less than PG16, transform and set em_nullable_relids similar to
			 * em_relids. Note that this code assumes parent and child relids are singletons.
			 */
			Relids new_nullable_relids = cur_em->em_nullable_relids;
			if (bms_is_member(info->ht_rel->relid, new_nullable_relids))
			{
				new_nullable_relids = bms_copy(new_nullable_relids);
				new_nullable_relids = bms_del_member(new_nullable_relids, info->ht_rel->relid);
				new_nullable_relids =
					bms_add_members(new_nullable_relids, info->compressed_rel->relids);
			}
			em->em_nullable_relids = new_nullable_relids;
#endif

			/*
			 * In some cases the new EC member is likely to be accessed soon, so
			 * it would make sense to add it to the front, but we cannot do that
			 * here. If we do that, the compressed chunk EM might get picked as
			 * SortGroupExpr by cost_incremental_sort, and estimate_num_groups
			 * will assert that the rel is simple rel, but it will fail because
			 * the compressed chunk rel is a deadrel. Anyway, it wouldn't make
			 * sense to estimate the group numbers by one append member,
			 * probably Postgres expects to see the parent relation first in the
			 * EMs.
			 */
#if PG18_LT
			cur_ec->ec_members = lappend(cur_ec->ec_members, em);
			cur_ec->ec_relids = bms_add_members(cur_ec->ec_relids, info->compressed_rel->relids);
#else
			ts_add_child_eq_member(root, cur_ec, em, info->compressed_rel->relid);
#endif

			/*
			 * Cache the matching EquivalenceClass and EquivalenceMember for
			 * segmentby column for future use, if we want to build a path that
			 * sorts on it. Sorting is defined by PathKeys, which refer to
			 * EquivalenceClasses, so it's a convenient form.
			 */
			compressed_fdw_private->compressed_ec_em_pairs =
				lappend(compressed_fdw_private->compressed_ec_em_pairs, list_make2(cur_ec, em));

			return true;
		}
	}
	return false;
}

static void
compressed_rel_setup_equivalence_classes(PlannerInfo *root, CompressionInfo *info)
{
	EMCreationContext context = {
		.uncompressed_relid = info->ht_rte->relid,
		.compressed_relid = info->compressed_rte->relid,

		.uncompressed_relid_idx = info->ht_rel->relid,
		.compressed_relid_idx = info->compressed_rel->relid,
		.settings = info->settings,
	};

	Assert(info->chunk_rte->relid != info->compressed_rel->relid);
	Assert(info->chunk_rel->relid != info->compressed_rel->relid);
	/* based on add_child_rel_equivalences */
	int i = -1;
	Assert(root->ec_merging_done);
	/* use chunk rel's eclass_indexes to avoid traversing all
	 * the root's eq_classes
	 */
	while ((i = bms_next_member(info->chunk_rel->eclass_indexes, i)) >= 0)
	{
		EquivalenceClass *cur_ec = (EquivalenceClass *) list_nth(root->eq_classes, i);
		/*
		 * If this EC contains a volatile expression, then generating child
		 * EMs would be downright dangerous, so skip it.  We rely on a
		 * volatile EC having only one EM.
		 */
		if (cur_ec->ec_has_volatile)
			continue;

		/* if the compressed rel is already part of this EC,
		 * we don't need to re-add it
		 */
		if (bms_overlap(cur_ec->ec_relids, info->compressed_rel->relids))
			continue;

		bool em_added = add_segmentby_to_equivalence_class(root, cur_ec, info, &context);
		/* Record this EC index for the compressed rel */
		if (em_added)
			info->compressed_rel->eclass_indexes =
				bms_add_member(info->compressed_rel->eclass_indexes, i);
	}
	info->compressed_rel->has_eclass_joins = info->chunk_rel->has_eclass_joins;
}

/*
 * create RangeTblEntry and RelOptInfo for the compressed chunk
 * and add it to PlannerInfo
 */
static void
columnar_scan_add_plannerinfo(PlannerInfo *root, CompressionInfo *info, const Chunk *chunk,
							  RelOptInfo *chunk_rel, bool needs_sequence_num)
{
	Index compressed_index = root->simple_rel_array_size;

	/*
	 * Add the compressed chunk to the baserel cache. Note that it belongs to
	 * a different hypertable, the internal compression table.
	 */
	const Chunk *compressed_chunk = ts_chunk_get_by_relid(info->settings->fd.compress_relid, true);
	ts_add_baserel_cache_entry_for_chunk(info->settings->fd.compress_relid,
										 ts_planner_get_hypertable(compressed_chunk
																	   ->hypertable_relid,
																   CACHE_FLAG_NONE));

	expand_planner_arrays(root, 1);
	info->compressed_rte = columnar_scan_make_rte(info->settings->fd.compress_relid,
												  info->chunk_rte->rellockmode,
												  root->parse);
	root->simple_rte_array[compressed_index] = info->compressed_rte;

	root->parse->rtable = lappend(root->parse->rtable, info->compressed_rte);

	root->simple_rel_array[compressed_index] = NULL;

	RelOptInfo *compressed_rel = build_simple_rel(root, compressed_index, NULL);

#if PG16_GE
	/*
	 * When initially creating the RTE we add a RTEPerminfo entry for the
	 * RTE but that is only to make build_simple_rel happy.
	 * Asserts in the permission check code will fail with an RTEPerminfo
	 * with no permissions to check so we remove it again here as we don't
	 * want permission checks on the compressed chunks when querying
	 * hypertables with compressed data.
	 */
	root->parse->rteperminfos = list_delete_last(root->parse->rteperminfos);
	info->compressed_rte->perminfoindex = 0;
#endif

	/* github issue :1558
	 * set up top_parent_relids for this rel as the same as the
	 * original hypertable, otherwise eq classes are not computed correctly
	 * in generate_join_implied_equalities (called by
	 * get_baserel_parampathinfo <- create_index_paths)
	 */
	Assert(info->single_chunk || chunk_rel->top_parent_relids != NULL);
	compressed_rel->top_parent_relids = bms_copy(chunk_rel->top_parent_relids);
	//compressed_rel->lateral_relids = bms_copy(chunk_rel->lateral_relids); // might not be needed

	root->simple_rel_array[compressed_index] = compressed_rel;
	info->compressed_rel = compressed_rel;

	Relation r = table_open(info->compressed_rte->relid, AccessShareLock);

	for (int i = 0; i < r->rd_att->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(r->rd_att, i);

		if (attr->attisdropped || attr->atttypid != info->compresseddata_oid)
			continue;

		info->compressed_attnos_in_compressed_chunk =
			bms_add_member(info->compressed_attnos_in_compressed_chunk, attr->attnum);
	}
	table_close(r, NoLock);

	compressed_rel_setup_reltarget(compressed_rel, info, needs_sequence_num);
	compressed_rel_setup_equivalence_classes(root, info);
	/* translate chunk_rel->joininfo for compressed_rel */
	compressed_rel_setup_joininfo(compressed_rel, info);

	/*
	 * Force parallel plan creation, see compute_parallel_worker().
	 * This is not compatible with ts_classify_relation(), but on the other hand
	 * the compressed chunk rel shouldn't exist anywhere outside of the
	 * decompression planning, it is removed at the end.
	 *
	 * This is not needed for direct select from a single chunk, in which case
	 * the chunk reloptkind will be RELOPT_BASEREL
	 */
	if (chunk_rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		compressed_rel->reloptkind = RELOPT_OTHER_MEMBER_REL;

		/*
		 * We have to minimally initialize the append relation info for the
		 * compressed chunks, so that the generate_implied_equalities() works.
		 * Only the parent hypertable relindex is needed.
		 */
		root->append_rel_array[compressed_rel->relid] = makeNode(AppendRelInfo);
		root->append_rel_array[compressed_rel->relid]->parent_relid = info->ht_rel->relid;
		compressed_rel->top_parent_relids = chunk_rel->top_parent_relids; // why?
	}
}

static ColumnarScanPath *
columnar_scan_path_create(PlannerInfo *root, const CompressionInfo *compression_info,
						  Path *compressed_path)
{
	ColumnarScanPath *path;

	path = (ColumnarScanPath *) newNode(sizeof(ColumnarScanPath), T_CustomPath);

	path->info = compression_info;

	path->custom_path.path.pathtype = T_CustomScan;
	path->custom_path.path.parent = compression_info->chunk_rel;
	path->custom_path.path.pathtarget = compression_info->chunk_rel->reltarget;

	if (compressed_path->param_info != NULL)
	{
		/*
		 * Note that we have to separately generate the parameterized path info
		 * for decompressed chunk path. The compressed parameterized path only
		 * checks the clauses on segmentby columns, not on the compressed
		 * columns.
		 */
		path->custom_path.path.param_info =
			get_baserel_parampathinfo(root,
									  compression_info->chunk_rel,
									  compressed_path->param_info->ppi_req_outer);
		Assert(path->custom_path.path.param_info != NULL);
	}
	else
	{
		path->custom_path.path.param_info = NULL;
	}

	path->custom_path.flags = 0;
	path->custom_path.methods = &columnar_scan_path_methods;
	path->batch_sorted_merge = false;

	/*
	 * ColumnarScan doesn't manage any parallelism itself.
	 */
	path->custom_path.path.parallel_aware = false;

	/*
	 * It can be applied per parallel worker, if its underlying scan is parallel.
	 */
	path->custom_path.path.parallel_safe = compressed_path->parallel_safe;
	path->custom_path.path.parallel_workers = compressed_path->parallel_workers;

	path->custom_path.custom_paths = list_make1(compressed_path);
	path->reverse = false;
	path->required_compressed_pathkeys = NIL;
	cost_columnar_scan(root, compression_info, &path->custom_path.path, compressed_path);

	return path;
}

/* NOTE: this needs to be called strictly after all restrictinfos have been added
 *       to the compressed rel
 */

static void
create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel,
							 const CompressionInfo *compression_info, const SortInfo *sort_info)
{
	Path *compressed_path;

	/* clamp total_table_pages to 10 pages since this is the
	 * minimum estimate for number of pages.
	 * Add the value to any existing estimates
	 */
	root->total_table_pages += Max(compressed_rel->pages, 10);

	/* create non parallel scan path */
	compressed_path =
		create_seqscan_path(root, compressed_rel, compression_info->chunk_rel->lateral_relids, 0);
	add_path(compressed_rel, compressed_path);

	/*
	 * Create parallel seq scan path.
	 * We marked the compressed rel as RELOPT_OTHER_MEMBER_REL when creating it,
	 * so we should get a nonzero number of parallel workers even for small
	 * tables, so that they don't prevent parallelism in the entire append plan.
	 * See compute_parallel_workers(). This also applies to the creation of
	 * index paths below.
	 */
	if (compressed_rel->consider_parallel)
	{
		int parallel_workers = compute_parallel_worker(compressed_rel,
													   compressed_rel->pages,
													   -1,
													   max_parallel_workers_per_gather);

		if (parallel_workers > 0)
		{
			add_partial_path(compressed_rel,
							 create_seqscan_path(root,
												 compressed_rel,
												 compression_info->chunk_rel->lateral_relids,
												 parallel_workers));
		}
	}

	if (sort_info->use_compressed_sort)
	{
		/*
		 * If we can push down sort below decompression we temporarily switch
		 * out root->query_pathkeys to allow matching to pathkeys produces by
		 * decompression
		 */
		List *orig_pathkeys = root->query_pathkeys;
		List *orig_eq_classes = root->eq_classes;
		Bitmapset *orig_eclass_indexes = compression_info->compressed_rel->eclass_indexes;
		root->query_pathkeys = sort_info->required_compressed_pathkeys;

		/* We can optimize iterating over EquivalenceClasses by reducing them to
		 * the subset which are from the compressed chunk. This only works if we don't
		 * have joins based on equivalence classes involved since those
		 * use eclass_indexes which is not valid with this optimization.
		 *
		 * Clauseless joins work fine since they don't rely on eclass_indexes.
		 */
		if (!compression_info->chunk_rel->has_eclass_joins)
		{
			int i = -1;
			List *required_eq_classes = NIL;
			while ((i = bms_next_member(compression_info->compressed_rel->eclass_indexes, i)) >= 0)
			{
				EquivalenceClass *cur_ec = (EquivalenceClass *) list_nth(root->eq_classes, i);
				required_eq_classes = lappend(required_eq_classes, cur_ec);
			}
			root->eq_classes = required_eq_classes;
			compression_info->compressed_rel->eclass_indexes = NULL;
		}

		check_index_predicates(root, compressed_rel);
		create_index_paths(root, compressed_rel);
		root->query_pathkeys = orig_pathkeys;
		root->eq_classes = orig_eq_classes;
		compression_info->compressed_rel->eclass_indexes = orig_eclass_indexes;
	}
	else
	{
		check_index_predicates(root, compressed_rel);
		create_index_paths(root, compressed_rel);
	}
}

/*
 * create RangeTblEntry for compressed chunk
 */
static RangeTblEntry *
columnar_scan_make_rte(Oid compressed_relid, LOCKMODE lockmode, Query *parse)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	Relation r = table_open(compressed_relid, lockmode);
	int varattno;

	rte->rtekind = RTE_RELATION;
	rte->relid = compressed_relid;
	rte->relkind = r->rd_rel->relkind;
	rte->rellockmode = lockmode;
	rte->eref = makeAlias(RelationGetRelationName(r), NULL);

	/*
	 * inlined from buildRelationAliases()
	 * alias handling has been stripped because we won't
	 * need alias handling at this level
	 */
	for (varattno = 0; varattno < r->rd_att->natts; varattno++)
	{
		Form_pg_attribute attr = TupleDescAttr(r->rd_att, varattno);
		/* Always insert an empty string for a dropped column */
		const char *attrname = attr->attisdropped ? "" : NameStr(attr->attname);
		rte->eref->colnames = lappend(rte->eref->colnames, makeString(pstrdup(attrname)));
	}

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	table_close(r, NoLock);

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = false;

#if PG16_LT
	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid; /* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
#else
	/* Add empty perminfo for the new RTE to make build_simple_rel happy. */
	addRTEPermissionInfo(&parse->rteperminfos, rte);
#endif

	return rte;
}

/*
 * Find segmentby columns that are equated to a constant by a toplevel
 * baserestrictinfo.
 *
 * This will detect Var = Const and Var = Param and set the corresponding bit
 * in CompressionInfo->chunk_const_segmentby.
 */
static Bitmapset *
find_const_segmentby(RelOptInfo *chunk_rel, const CompressionInfo *info)
{
	Bitmapset *segmentby_columns = NULL;

	if (chunk_rel->baserestrictinfo != NIL)
	{
		ListCell *lc_ri;
		foreach (lc_ri, chunk_rel->baserestrictinfo)
		{
			RestrictInfo *ri = lfirst(lc_ri);

			if (IsA(ri->clause, OpExpr) && list_length(castNode(OpExpr, ri->clause)->args) == 2)
			{
				OpExpr *op = castNode(OpExpr, ri->clause);
				Var *lvar, *rvar, *var;
				Expr *other;

				if (op->opretset)
					continue;

				lvar = linitial(op->args);
				while (lvar && IsA(lvar, RelabelType))
					lvar = (Var *) ((RelabelType *) lvar)->arg;

				rvar = lsecond(op->args);
				while (rvar && IsA(rvar, RelabelType))
					rvar = (Var *) ((RelabelType *) rvar)->arg;

				Assert(lvar && rvar);
				if (IsA(lvar, Var))
				{
					var = castNode(Var, lvar);
					other = lsecond(op->args);
				}
				else if (IsA(rvar, Var))
				{
					var = castNode(Var, rvar);
					other = linitial(op->args);
				}
				else
					continue;

				if ((Index) var->varno != chunk_rel->relid || var->varattno <= 0)
					continue;

				if (IsA(other, Const) || IsA(other, Param))
				{
					TypeCacheEntry *tce = lookup_type_cache(var->vartype, TYPECACHE_EQ_OPR);

					if (op->opno != tce->eq_opr)
						continue;

					if (bms_is_member(var->varattno, info->chunk_segmentby_attnos))
						segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
				}
			}
		}
	}

	return segmentby_columns;
}

/*
 * Returns whether the pathkeys starting at the given offset match the compression
 * orderby, and whether the order is reverse.
 */
static bool
match_pathkeys_to_compression_orderby(List *pathkeys, List *chunk_em_exprs,
									  int starting_pathkey_offset,
									  const CompressionInfo *compression_info, bool *out_reverse)
{
	int compressed_pk_index = 0;
	for (int i = starting_pathkey_offset; i < list_length(pathkeys); i++)
	{
		compressed_pk_index++;
		PathKey *pk = list_nth_node(PathKey, pathkeys, i);
		Expr *expr = (Expr *) list_nth(chunk_em_exprs, i);
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		if (expr == NULL || !IsA(expr, Var))
		{
			return false;
		}

		Var *var = castNode(Var, expr);

		if (var->varattno <= 0)
		{
			return false;
		}

		char *column_name = get_attname(compression_info->chunk_rte->relid, var->varattno, false);
		int orderby_index = ts_array_position(compression_info->settings->fd.orderby, column_name);

		if (orderby_index != compressed_pk_index)
		{
			return false;
		}

		bool orderby_desc =
			ts_array_get_element_bool(compression_info->settings->fd.orderby_desc, orderby_index);
		bool orderby_nullsfirst =
			ts_array_get_element_bool(compression_info->settings->fd.orderby_nullsfirst,
									  orderby_index);

		/*
		 * In PG18+: pk_cmptype is either COMPARE_LT (for ASC) or COMPARE_GT (for DESC)
		 * For previous PG versions we have compatibility macros to make these new names available.
		 */
		bool this_pathkey_reverse = false;
		if (pk->pk_cmptype == COMPARE_LT)
		{
			if (!orderby_desc && orderby_nullsfirst == pk->pk_nulls_first)
			{
				this_pathkey_reverse = false;
			}
			else if (orderby_desc && orderby_nullsfirst != pk->pk_nulls_first)
			{
				this_pathkey_reverse = true;
			}
			else
			{
				return false;
			}
		}
		else if (pk->pk_cmptype == COMPARE_GT)
		{
			if (orderby_desc && orderby_nullsfirst == pk->pk_nulls_first)
			{
				this_pathkey_reverse = false;
			}
			else if (!orderby_desc && orderby_nullsfirst != pk->pk_nulls_first)
			{
				this_pathkey_reverse = true;
			}
			else
			{
				return false;
			}
		}

		/*
		 * first pathkey match determines if this is forward or backward scan
		 * any further pathkey items need to have same direction
		 */
		if (compressed_pk_index == 1)
		{
			*out_reverse = this_pathkey_reverse;
		}
		else if (this_pathkey_reverse != *out_reverse)
		{
			return false;
		}
	}

	return true;
}

/*
 * Check if we can push down the sort below the ColumnarSacn node and fill
 * SortInfo accordingly
 *
 * The following conditions need to be true for pushdown:
 *  - all segmentby columns need to be prefix of pathkeys or have equality constraint
 *  - the rest of pathkeys needs to match compress_orderby
 *
 * If query pathkeys is shorter than segmentby + compress_orderby pushdown can still be done
 */
static SortInfo
build_sortinfo(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
			   const CompressionInfo *compression_info, List *pathkeys)
{
	Var *var;
	char *column_name;
	ListCell *lc;
	SortInfo sort_info = { 0 };

	if (pathkeys == NIL)
	{
		return sort_info;
	}

	/*
	 * Translate the pathkeys to chunk expressions, creating a List of them
	 * parallel to the pathkeys list, with NULL entries if we didn't find a
	 * match.
	 */
	List *chunk_em_exprs = NIL;
	foreach (lc, pathkeys)
	{
		PathKey *pk = lfirst(lc);
		EquivalenceClass *ec = pk->pk_eclass;
		Expr *em_expr = NULL;
		if (!ec->ec_has_volatile)
		{
			em_expr = ts_find_em_expr_for_rel(pk->pk_eclass, compression_info->chunk_rel);
		}
		chunk_em_exprs = lappend(chunk_em_exprs, em_expr);
	}
	Assert(list_length(chunk_em_exprs) == list_length(pathkeys));

	/* Find the pathkeys we can use for explicitly sorting after decompression. */
	List *sort_pathkey_exprs = NIL;
	List *sort_pathkeys = NIL;
	for (int i = 0; i < list_length(chunk_em_exprs); i++)
	{
		PathKey *pk = list_nth_node(PathKey, pathkeys, i);
		Expr *chunk_em_expr = (Expr *) list_nth(chunk_em_exprs, i);
		if (chunk_em_expr == NULL)
		{
			break;
		}

		sort_pathkeys = lappend(sort_pathkeys, pk);
		sort_pathkey_exprs = lappend(sort_pathkey_exprs, chunk_em_expr);
	}

	if (sort_pathkeys == NIL)
	{
		return sort_info;
	}

	sort_info.decompressed_sort_pathkeys = sort_pathkeys;
	cost_qual_eval(&sort_info.decompressed_sort_pathkeys_cost, sort_pathkey_exprs, root);

	/*
	 * Next, check if we can push the sort down to the uncompressed part.
	 *
	 * Not possible if the chunk is unordered.
	 */
	if (ts_chunk_is_unordered(chunk))
		return sort_info;

	/* all segmentby columns need to be prefix of pathkeys */
	int i = 0;
	if (compression_info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns;

		/*
		 * initialize segmentby with equality constraints from baserestrictinfo because
		 * those columns dont need to be prefix of pathkeys
		 */
		segmentby_columns = bms_copy(compression_info->chunk_const_segmentby);

		/*
		 * loop over pathkeys until we find one that is not a segmentby column
		 * we keep looping even if we found all segmentby columns in case a
		 * columns appears both in baserestrictinfo and in ORDER BY clause
		 */
		for (i = 0; i < list_length(pathkeys); i++)
		{
			Assert(bms_num_members(segmentby_columns) <= compression_info->num_segmentby_columns);

			Expr *expr = (Expr *) list_nth(chunk_em_exprs, i);
			while (expr && IsA(expr, RelabelType))
				expr = ((RelabelType *) expr)->arg;

			if (expr == NULL || !IsA(expr, Var))
				break;
			var = castNode(Var, expr);

			if (var->varattno <= 0)
				break;

			column_name = get_attname(compression_info->chunk_rte->relid, var->varattno, false);
			if (!ts_array_is_member(compression_info->settings->fd.segmentby, column_name))
				break;

			segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
		}

		/*
		 * If pathkeys still has items, but we didn't find all segmentby columns,
		 * we cannot satisfy these pathkeys by sorting the compressed chunk table.
		 */
		if (i != list_length(pathkeys) &&
			bms_num_members(segmentby_columns) != compression_info->num_segmentby_columns)
		{
			/*
			 * If we didn't have any segmentby columns in pathkeys, try batch sorted merge
			 * instead.
			 */
			if (i == 0)
			{
				sort_info.use_batch_sorted_merge =
					match_pathkeys_to_compression_orderby(pathkeys,
														  chunk_em_exprs,
														  /* starting_pathkey_offset = */ 0,
														  compression_info,
														  &sort_info.reverse);
			}
			return sort_info;
		}
	}

	if (i == list_length(pathkeys))
	{
		/*
		 * Pathkeys satisfied by sorting the compressed data on segmentby columns.
		 */
		sort_info.use_compressed_sort = true;
		return sort_info;
	}

	/*
	 * Pathkeys includes columns past segmentby columns, so we need sequence_num
	 * in the targetlist for ordering.
	 */
	sort_info.needs_sequence_num = true;

	/*
	 * loop over the rest of pathkeys
	 * this needs to exactly match the configured compress_orderby
	 */
	sort_info.use_compressed_sort = match_pathkeys_to_compression_orderby(pathkeys,
																		  chunk_em_exprs,
																		  i,
																		  compression_info,
																		  &sort_info.reverse);

	return sort_info;
}

/* Check if the provided path is a ColumnarScanPath */
bool
ts_is_columnar_scan_path(Path *path)
{
	return IsA(path, CustomPath) &&
		   castNode(CustomPath, path)->methods == &columnar_scan_path_methods;
}
