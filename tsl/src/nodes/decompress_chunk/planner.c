/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/plancat.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "guc.h"
#include "import/list.h"
#include "import/planner.h"
#include "nodes/chunk_append/transform.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/vector_agg/exec.h"
#include "ts_catalog/array_utils.h"
#include "vector_predicates.h"

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk",
	.CreateCustomScanState = decompress_chunk_state_create,
};

void
_decompress_chunk_init(void)
{
	TryRegisterCustomScanMethods(&decompress_chunk_plan_methods);
}

static void
check_for_system_columns(Bitmapset *attrs_used)
{
	int bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
	{
		/* we support tableoid so skip that */
		if (bit == TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber)
			bit = bms_next_member(attrs_used, bit);

		if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
			elog(ERROR, "transparent decompression only supports tableoid system column");
	}
}

typedef struct
{
	bool bulk_decompression_possible;
	int custom_scan_attno;
} UncompressedColumnInfo;

typedef struct
{
	/* Can be negative if it's a metadata column, zero if not decompressed. */
	int uncompressed_chunk_attno;
	bool bulk_decompression_possible;
	bool is_segmentby;
} CompressedColumnInfo;

/*
 * Scratch space for mapping out the decompressed columns.
 */
typedef struct
{
	PlannerInfo *root;

	DecompressChunkPath *decompress_path;

	Bitmapset *uncompressed_attrs_needed;

	/*
	 * If we produce at least some columns that support bulk decompression.
	 */
	bool have_bulk_decompression_columns;

	/*
	 * Maps the uncompressed chunk attno to the respective column compression
	 * info. This lives only during planning so that we can understand on which
	 * columns we can apply vectorized quals, and which uncompressed attno goes
	 * to which custom scan attno (it's not the same if we're using a custom
	 * scan targetlist).
	 */
	UncompressedColumnInfo *uncompressed_attno_info;

	/*
	 * Maps the compressed chunk attno to the respective column compression info.
	 */
	CompressedColumnInfo *compressed_attno_info;

	/*
	 * We might use a custom scan targetlist for DecompressChunk node if it
	 * allows us to avoid projection.
	 */
	List *custom_scan_targetlist;

	/*
	 * Next, we have basically the same data as the above, but expressed as
	 * several Lists, to allow passing them through the custom plan settings.
	 */

	/*
	 * decompression_map maps targetlist entries of the compressed scan to
	 * custom scan attnos. Negative values are metadata columns in the compressed
	 * scan that do not have a representation in the uncompressed chunk, but are
	 * still used for decompression.
	 */
	List *decompression_map;

	/*
	 * This Int list is parallel to the compressed scan targetlist, just like
	 * the above one. The value is true if a given targetlist entry is a
	 * segmentby column, false otherwise. Has the same length as the above list.
	 * We have to use the parallel lists and not a list of structs, because the
	 * Plans have to be copyable by the Postgres _copy functions, and we can't
	 * do that for a custom struct.
	 */
	List *is_segmentby_column;

	/*
	 * Same structure as above, says whether we support bulk decompression for this
	 * column.
	 */
	List *bulk_decompression_column;

} DecompressionMapContext;

/*
 * Try to make the custom scan targetlist that follows the order of the
 * pathtarget. This would allow us to avoid a projection from scan tuple to
 * output tuple.
 * Returns NIL if it's not possible, e.g. if there are whole-row variables or
 * variables that are used for quals but not for output.
 */
static List *
follow_uncompressed_output_tlist(const DecompressionMapContext *context)
{
	List *result = NIL;
	Bitmapset *uncompressed_attrs_found = NULL;
	const CompressionInfo *info = context->decompress_path->info;
	const PathTarget *pathtarget = context->decompress_path->custom_path.path.pathtarget;
	int custom_scan_attno = 1;
	for (int i = 0; i < list_length(pathtarget->exprs); i++)
	{
		Expr *expr = list_nth(pathtarget->exprs, i);
		if (!IsA(expr, Var))
		{
			/*
			 * The pathtarget has some non-var expressions, so we won't be able
			 * to build a matching decompressed scan targetlist.
			 */
			return NIL;
		}

		Var *var = castNode(Var, expr);

		/* This should produce uncompressed chunk columns. */
		Assert((Index) var->varno == info->chunk_rel->relid);

		const int uncompressed_chunk_attno = var->varattno;
		if (uncompressed_chunk_attno <= 0)
		{
			/*
			 * The pathtarget has some special vars so we won't be able to
			 * build a matching decompressed scan targetlist.
			 */
			return NIL;
		}

		char *attname = get_attname(info->chunk_rte->relid,
									uncompressed_chunk_attno,
									/* missing_ok = */ false);

		TargetEntry *target_entry = makeTargetEntry((Expr *) copyObject(var),
													/* resno = */ custom_scan_attno,
													/* resname = */ attname,
													/* resjunk = */ false);
		target_entry->ressortgroupref =
			pathtarget->sortgrouprefs ? pathtarget->sortgrouprefs[i] : 0;
		result = lappend(result, target_entry);

		uncompressed_attrs_found =
			bms_add_member(uncompressed_attrs_found,
						   uncompressed_chunk_attno - FirstLowInvalidHeapAttributeNumber);

		custom_scan_attno++;
	}

	if (!bms_equal(uncompressed_attrs_found, context->uncompressed_attrs_needed))
	{
		/*
		 * There are some variables that are not in the pathtarget that are used
		 * for quals. We still have to have them in the scan tuple in this case.
		 * Note that while we could possibly relax this at execution time for
		 * vectorized quals, the requirement that the qual var be found in the
		 * scan targetlist is a Postgres one.
		 */
		return NIL;
	}

	return result;
}

/*
 * Given the compressed output targetlist and the bitmapset of the needed
 * columns, determine which compressed chunk column become which uncompressed
 * chunk column.
 *
 * Note that the uncompressed_attrs_needed bitmap is offset by the
 * FirstLowInvalidHeapAttributeNumber, similar to RelOptInfo.attr_needed. This
 * allows to encode the requirement for system columns, which have negative
 * attnos.
 */
static void
build_decompression_map(DecompressionMapContext *context, List *compressed_scan_tlist)
{
	DecompressChunkPath *path = context->decompress_path;
	CompressionInfo *info = path->info;
	/*
	 * Track which normal and metadata columns we were able to find in the
	 * targetlist.
	 */
	bool missing_count = true;
	bool missing_sequence = path->needs_sequence_num;
	Bitmapset *uncompressed_attrs_found = NULL;
	Bitmapset *selectedCols = NULL;

#if PG16_LT
	selectedCols = info->ht_rte->selectedCols;
#else
	if (info->ht_rte->perminfoindex > 0)
	{
		RTEPermissionInfo *perminfo =
			getRTEPermissionInfo(context->root->parse->rteperminfos, info->ht_rte);
		selectedCols = perminfo->selectedCols;
	}
#endif
	/*
	 * FIXME this way to determine which columns are used is actually wrong, see
	 * https://github.com/timescale/timescaledb/issues/4195#issuecomment-1104238863
	 * Left as is for now, because changing it uncovers a whole new story with
	 * ctid.
	 */
	check_for_system_columns(selectedCols);

	/*
	 * We allow tableoid system column, it won't be in the targetlist but will
	 * be added at decompression time. Always mark it as found.
	 */
	if (bms_is_member(TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber,
					  context->uncompressed_attrs_needed))
	{
		uncompressed_attrs_found =
			bms_add_member(uncompressed_attrs_found,
						   TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber);
	}

	ListCell *lc;

	context->uncompressed_attno_info =
		palloc0(sizeof(*context->uncompressed_attno_info) * (info->chunk_rel->max_attr + 1));

	context->compressed_attno_info =
		palloc0(sizeof(*context->compressed_attno_info) * (info->compressed_rel->max_attr + 1));

	/*
	 * Go over the scan targetlist and determine to which output column each
	 * scan column goes, saving other additional info as we do that.
	 */
	context->have_bulk_decompression_columns = false;
	context->decompression_map = NIL;
	foreach (lc, compressed_scan_tlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);
		if (!IsA(target->expr, Var))
		{
			elog(ERROR, "compressed scan targetlist entries must be Vars");
		}

		Var *var = castNode(Var, target->expr);
		Assert((Index) var->varno == info->compressed_rel->relid);
		AttrNumber compressed_chunk_attno = var->varattno;

		if (compressed_chunk_attno == InvalidAttrNumber)
		{
			/*
			 * We shouldn't have whole-row vars in the compressed scan tlist,
			 * they are going to be built by final projection of DecompressChunk
			 * custom scan.
			 * See compressed_rel_setup_reltarget().
			 */
			elog(ERROR, "compressed scan targetlist must not have whole-row vars");
		}

		const char *column_name = get_attname(info->compressed_rte->relid,
											  compressed_chunk_attno,
											  /* missing_ok = */ false);
		AttrNumber uncompressed_chunk_attno = get_attnum(info->chunk_rte->relid, column_name);

		AttrNumber destination_attno = 0;
		if (uncompressed_chunk_attno != InvalidAttrNumber)
		{
			/*
			 * Normal column, not a metadata column.
			 */
			Assert(uncompressed_chunk_attno != InvalidAttrNumber);

			if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
							  context->uncompressed_attrs_needed))
			{
				/*
				 * attno = 0 means whole-row var. Output all the columns.
				 */
				destination_attno = uncompressed_chunk_attno;
				uncompressed_attrs_found =
					bms_add_member(uncompressed_attrs_found,
								   uncompressed_chunk_attno - FirstLowInvalidHeapAttributeNumber);
			}
			else if (bms_is_member(uncompressed_chunk_attno - FirstLowInvalidHeapAttributeNumber,
								   context->uncompressed_attrs_needed))
			{
				destination_attno = uncompressed_chunk_attno;
				uncompressed_attrs_found =
					bms_add_member(uncompressed_attrs_found,
								   uncompressed_chunk_attno - FirstLowInvalidHeapAttributeNumber);
			}
		}
		else
		{
			/*
			 * Metadata column.
			 * We always need count column, and sometimes a sequence number
			 * column. We don't output them, but use them for decompression,
			 * hence the special negative destination attnos.
			 * The min/max metadata columns are normally not required for output
			 * or decompression, they are used only as filter for the compressed
			 * scan, so we skip them here.
			 */
			Assert(strncmp(column_name,
						   COMPRESSION_COLUMN_METADATA_PREFIX,
						   strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0);

			if (strcmp(column_name, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
			{
				destination_attno = DECOMPRESS_CHUNK_COUNT_ID;
				missing_count = false;
			}
			else if (path->needs_sequence_num &&
					 strcmp(column_name, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) == 0)
			{
				destination_attno = DECOMPRESS_CHUNK_SEQUENCE_NUM_ID;
				missing_sequence = false;
			}
		}

		const bool is_segment = ts_array_is_member(info->settings->fd.segmentby, column_name);

		/*
		 * Determine if we can use bulk decompression for this column.
		 */
		Oid typoid = get_atttype(info->chunk_rte->relid, uncompressed_chunk_attno);
		const bool bulk_decompression_possible =
			!is_segment && destination_attno > 0 &&
			tsl_get_decompress_all_function(compression_get_default_algorithm(typoid), typoid) !=
				NULL;
		context->have_bulk_decompression_columns |= bulk_decompression_possible;

		/*
		 * Save information about decompressed columns in uncompressed chunk
		 * for planning of vectorized filters.
		 */
		if (uncompressed_chunk_attno != InvalidAttrNumber)
		{
			context->uncompressed_attno_info[uncompressed_chunk_attno] = (UncompressedColumnInfo){
				.bulk_decompression_possible = bulk_decompression_possible,
				.custom_scan_attno = InvalidAttrNumber,
			};
		}

		context->compressed_attno_info[compressed_chunk_attno] = (CompressedColumnInfo){
			.bulk_decompression_possible = bulk_decompression_possible,
			.uncompressed_chunk_attno = destination_attno,
			.is_segmentby = is_segment,
		};
	}

	/*
	 * Check that we have found all the needed columns in the compressed targetlist.
	 * We can't conveniently check that we have all columns for all-row vars, so
	 * skip attno 0 in this check.
	 */
	Bitmapset *attrs_not_found =
		bms_difference(context->uncompressed_attrs_needed, uncompressed_attrs_found);
	int bit = bms_next_member(attrs_not_found, 0 - FirstLowInvalidHeapAttributeNumber);
	if (bit >= 0)
	{
		elog(ERROR,
			 "column '%s' (%d) not found in the targetlist for compressed chunk '%s'",
			 get_attname(info->chunk_rte->relid,
						 bit + FirstLowInvalidHeapAttributeNumber,
						 /* missing_ok = */ true),
			 bit + FirstLowInvalidHeapAttributeNumber,
			 get_rel_name(info->compressed_rte->relid));
	}

	if (missing_count)
	{
		elog(ERROR, "the count column was not found in the compressed targetlist");
	}

	if (missing_sequence)
	{
		elog(ERROR, "the sequence column was not found in the compressed scan targetlist");
	}

	/*
	 * If possible, try to make the custom scan targetlist same as the required
	 * output targetlist, so that we can avoid a projection there.
	 */
	context->custom_scan_targetlist = follow_uncompressed_output_tlist(context);
	if (context->custom_scan_targetlist != NIL)
	{
		/*
		 * The decompression will produce a custom scan tuple, set the custom
		 * scan attnos accordingly.
		 */
		int custom_scan_attno = 1;
		foreach (lc, context->custom_scan_targetlist)
		{
			const int uncompressed_chunk_attno =
				castNode(Var, castNode(TargetEntry, lfirst(lc))->expr)->varattno;
			context->uncompressed_attno_info[uncompressed_chunk_attno].custom_scan_attno =
				custom_scan_attno;
			custom_scan_attno++;
		}
	}
	else
	{
		/*
		 * The decompression will produce the uncompressed chunk tuple, set the
		 * custom scan attnos accordingly.
		 * Note that we might have dropped columns here, but we can set these
		 * attnos for them just as well, they won't be decompressed anyway
		 * because they are not in the compressed scan output.
		 */
		for (int i = 1; i <= info->chunk_rel->max_attr; i++)
		{
			UncompressedColumnInfo *uncompressed_info = &context->uncompressed_attno_info[i];
			uncompressed_info->custom_scan_attno = i;
		}
	}

	/*
	 * Finally, we have to convert the decompression information we've build
	 * into several lists so that it can be passed through the custom path
	 * settings.
	 */
	foreach (lc, compressed_scan_tlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);
		Var *var = castNode(Var, target->expr);
		Assert((Index) var->varno == info->compressed_rel->relid);
		const AttrNumber compressed_chunk_attno = var->varattno;
		Assert(compressed_chunk_attno != InvalidAttrNumber);
		CompressedColumnInfo *compressed_info =
			&context->compressed_attno_info[compressed_chunk_attno];

		/*
		 * Note that the decompressed custom scan targetlist might follow
		 * neither its output targetlist (when we need more columns for filters)
		 * nor the uncompressed chunk tuple. So here we have to do this
		 * additional conversion.
		 */
		int compressed_column_destination;
		if (compressed_info->uncompressed_chunk_attno <= 0)
		{
			compressed_column_destination = compressed_info->uncompressed_chunk_attno;
		}
		else
		{
			UncompressedColumnInfo *uncompressed_info =
				&context->uncompressed_attno_info[compressed_info->uncompressed_chunk_attno];
			compressed_column_destination = uncompressed_info->custom_scan_attno;
		}

		context->decompression_map =
			lappend_int(context->decompression_map, compressed_column_destination);
		context->is_segmentby_column =
			lappend_int(context->is_segmentby_column, compressed_info->is_segmentby);
		context->bulk_decompression_column =
			lappend_int(context->bulk_decompression_column,
						compressed_info->bulk_decompression_possible);
	}
}

/* replace vars that reference the compressed table with ones that reference the
 * uncompressed one. Based on replace_nestloop_params
 */
static Node *
replace_compressed_vars(Node *node, CompressionInfo *info)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		Var *new_var;
		char *colname;

		/* constify tableoid in quals */
		if ((Index) var->varno == info->chunk_rel->relid &&
			var->varattno == TableOidAttributeNumber)
			return (Node *)
				makeConst(OIDOID, -1, InvalidOid, 4, (Datum) info->chunk_rte->relid, false, true);

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if ((Index) var->varno != info->compressed_rel->relid)
			return node;

		/* Create a decompressed Var to replace the compressed one */
		colname = get_attname(info->compressed_rte->relid, var->varattno, false);
		new_var = makeVar(info->chunk_rel->relid,
						  get_attnum(info->chunk_rte->relid, colname),
						  var->vartype,
						  var->vartypmod,
						  var->varcollid,
						  var->varlevelsup);

		if (!AttributeNumberIsValid(new_var->varattno))
			elog(ERROR, "cannot find column %s on decompressed chunk", colname);

		/* And return the replacement var */
		return (Node *) new_var;
	}
	if (IsA(node, PlaceHolderVar))
		elog(ERROR, "ignoring placeholders");

	return expression_tree_mutator(node, replace_compressed_vars, (void *) info);
}

/*
 * Find the resno of the given attribute in the provided target list
 */
static AttrNumber
find_attr_pos_in_tlist(List *targetlist, AttrNumber pos)
{
	ListCell *lc;

	Assert(targetlist != NIL);
	Assert(pos > 0 && pos != InvalidAttrNumber);

	foreach (lc, targetlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);

		if (!IsA(target->expr, Var))
			elog(ERROR, "compressed scan targetlist entries must be Vars");

		Var *var = castNode(Var, target->expr);
		AttrNumber compressed_attno = var->varattno;

		if (compressed_attno == pos)
			return target->resno;
	}

	elog(ERROR, "Unable to locate var %d in targetlist", pos);
	pg_unreachable();
}

static bool
contains_volatile_functions_checker(Oid func_id, void *context)
{
	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

static bool
is_not_runtime_constant_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	switch (nodeTag(node))
	{
		case T_Var:
		case T_PlaceHolderVar:
			/*
			 * We might want to support these nodes to have vectorizable join
			 * clauses (T_Var) or join clauses referencing a variable that is
			 * above outer join (T_PlaceHolderVar). We don't support them at the
			 * moment.
			 */
			return true;
		case T_Param:
			/*
			 * We support external query parameters (e.g. from parameterized
			 * prepared statements), because they are constant for the duration
			 * of the query.
			 *
			 * Join and initplan parameters are passed as PARAM_EXEC and require
			 * support in the Rescan functions of the custom scan node. We don't
			 * support them at the moment.
			 */
			return castNode(Param, node)->paramkind != PARAM_EXTERN;
		default:
			if (check_functions_in_node(node,
										contains_volatile_functions_checker,
										/* context = */ NULL))
			{
				return true;
			}
			return expression_tree_walker(node,
										  is_not_runtime_constant_walker,
										  /* context = */ NULL);
	}
}

/*
 * Check if the given node is a run-time constant, i.e. it doesn't contain
 * volatile functions or variables or parameters. This means we can evaluate
 * it at run time, allowing us to apply the vectorized comparison operators
 * that have the form "Var op Const". This applies for example to filter
 * expressions like `time > now() - interval '1 hour'`.
 * Note that we do the same evaluation when doing run time chunk exclusion, but
 * there is no good way to pass the evaluated clauses to the underlying nodes
 * like this DecompressChunk node.
 */
static bool
is_not_runtime_constant(Node *node)
{
	bool result = is_not_runtime_constant_walker(node, /* context = */ NULL);
	return result;
}

/*
 * Try to check if the current qual is vectorizable, and if needed make a
 * commuted copy. If not, return NULL.
 */
static Node *
make_vectorized_qual(DecompressionMapContext *context, DecompressChunkPath *path, Node *qual)
{
	/*
	 * We can vectorize BoolExpr (AND/OR/NOT).
	 */
	if (IsA(qual, BoolExpr))
	{
		BoolExpr *boolexpr = castNode(BoolExpr, qual);

		if (boolexpr->boolop == NOT_EXPR)
		{
			/*
			 * NOT should be removed by Postgres for all operators we can
			 * vectorize (see prepqual.c), so we don't support it.
			 */
			return NULL;
		}

		bool need_copy = false;
		List *vectorized_args = NIL;
		ListCell *lc;
		foreach (lc, boolexpr->args)
		{
			Node *arg = lfirst(lc);
			Node *vectorized_arg = make_vectorized_qual(context, path, arg);
			if (vectorized_arg == NULL)
			{
				return NULL;
			}

			if (vectorized_arg != arg)
			{
				need_copy = true;
			}

			vectorized_args = lappend(vectorized_args, vectorized_arg);
		}

		if (!need_copy)
		{
			return (Node *) boolexpr;
		}

		BoolExpr *boolexpr_copy = (BoolExpr *) copyObject(boolexpr);
		boolexpr_copy->args = vectorized_args;
		return (Node *) boolexpr_copy;
	}

	/*
	 * Among the simple predicates, we vectorize some "Var op Const" binary
	 * predicates, scalar array operations with these predicates, and null test.
	 */
	NullTest *nulltest = NULL;
	OpExpr *opexpr = NULL;
	ScalarArrayOpExpr *saop = NULL;
	Node *arg1 = NULL;
	Node *arg2 = NULL;
	Oid opno = InvalidOid;
	if (IsA(qual, OpExpr))
	{
		opexpr = castNode(OpExpr, qual);
		opno = opexpr->opno;
		if (list_length(opexpr->args) != 2)
		{
			return NULL;
		}
		arg1 = (Node *) linitial(opexpr->args);
		arg2 = (Node *) lsecond(opexpr->args);
	}
	else if (IsA(qual, ScalarArrayOpExpr))
	{
		saop = castNode(ScalarArrayOpExpr, qual);
		opno = saop->opno;
		Assert(list_length(saop->args) == 2);
		arg1 = (Node *) linitial(saop->args);
		arg2 = (Node *) lsecond(saop->args);
	}
	else if (IsA(qual, NullTest))
	{
		nulltest = castNode(NullTest, qual);
		arg1 = (Node *) nulltest->arg;
	}
	else
	{
		return NULL;
	}

	if (opexpr && IsA(arg2, Var))
	{
		/*
		 * Try to commute the operator if we have Var on the right.
		 */
		opno = get_commutator(opno);
		if (!OidIsValid(opno))
		{
			return NULL;
		}

		opexpr = (OpExpr *) copyObject(opexpr);
		opexpr->opno = opno;
		/*
		 * opfuncid is a cache, we can set it to InvalidOid like the
		 * CommuteOpExpr() does.
		 */
		opexpr->opfuncid = InvalidOid;
		opexpr->args = list_make2(arg2, arg1);
		Node *tmp = arg1;
		arg1 = arg2;
		arg2 = tmp;
	}

	/*
	 * We can vectorize the operation where the left side is a Var.
	 */
	if (!IsA(arg1, Var))
	{
		return NULL;
	}

	Var *var = castNode(Var, arg1);
	if ((Index) var->varno != path->info->chunk_rel->relid)
	{
		/*
		 * We have a Var from other relation (join clause), can't vectorize it
		 * at the moment.
		 */
		return NULL;
	}

	if (var->varattno <= 0)
	{
		/*
		 * Can't vectorize operators with special variables such as whole-row var.
		 */
		return NULL;
	}

	/*
	 * ExecQual is performed before ExecProject and operates on the decompressed
	 * scan slot, so the qual attnos are the uncompressed chunk attnos.
	 */
	if (!context->uncompressed_attno_info[var->varattno].bulk_decompression_possible)
	{
		/* This column doesn't support bulk decompression. */
		return NULL;
	}

	if (nulltest)
	{
		/*
		 * The checks we've done to this point is all that is required for null
		 * test.
		 */
		return (Node *) nulltest;
	}

	/*
	 * We can vectorize the operation where the right side is a constant or can
	 * be evaluated to a constant at run time (e.g. contains stable functions).
	 */
	Assert(arg2);
	if (is_not_runtime_constant(arg2))
	{
		return NULL;
	}

	Oid opcode = get_opcode(opno);
	if (!get_vector_const_predicate(opcode))
	{
		return NULL;
	}

	if (OidIsValid(var->varcollid) && !get_collation_isdeterministic(var->varcollid))
	{
		/*
		 * Can't vectorize string equality with a nondeterministic collation.
		 * Not sure if we have to check the collation of Const as well, but it
		 * will be known only at planning time. Currently we don't check it at
		 * all. Also this is untested because we don't have nondeterministic
		 * collations in all test configurations.
		 */
		return NULL;
	}

	if (opexpr)
	{
		/*
		 * The checks we've done to this point is all that is required for
		 * OpExpr.
		 */
		return (Node *) opexpr;
	}

	/*
	 * The only option that is left is a ScalarArrayOpExpr.
	 */
	Assert(saop != NULL);

	if (saop->hashfuncid)
	{
		/*
		 * Don't vectorize if the planner decided to build a hash table.
		 */
		return NULL;
	}

	return (Node *) saop;
}

/*
 * Find the scan qualifiers that can be vectorized and put them into a separate
 * list.
 */
static void
find_vectorized_quals(DecompressionMapContext *context, DecompressChunkPath *path, List *qual_list,
					  List **vectorized, List **nonvectorized)
{
	ListCell *lc;
	foreach (lc, qual_list)
	{
		Node *source_qual = lfirst(lc);

		/*
		 * We can't vectorize the stable cross-type operators (for example
		 * timestamp > timestamptz), so try to cast the constant to the same
		 * type to convert it to the same-type operator. We do the same thing
		 * for chunk exclusion.
		 */
		Node *transformed_comparison =
			(Node *) ts_transform_cross_datatype_comparison((Expr *) source_qual);

		Node *vectorized_qual = make_vectorized_qual(context, path, transformed_comparison);
		if (vectorized_qual)
		{
			*vectorized = lappend(*vectorized, vectorized_qual);
		}
		else
		{
			*nonvectorized = lappend(*nonvectorized, source_qual);
		}
	}
}

/*
 * Copy of the Postgres' static function from createplan.c.
 *
 * Some places in this file build Sort nodes that don't have a directly
 * corresponding Path node.  The cost of the sort is, or should have been,
 * included in the cost of the Path node we're working from, but since it's
 * not split out, we have to re-figure it using cost_sort().  This is just
 * to label the Sort node nicely for EXPLAIN.
 *
 * limit_tuples is as for cost_sort (in particular, pass -1 if no limit)
 */
static void
ts_label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples)
{
	Plan *lefttree = plan->plan.lefttree;
	Path sort_path; /* dummy for result of cost_sort */

	/*
	 * This function shouldn't have to deal with IncrementalSort plans because
	 * they are only created from corresponding Path nodes.
	 */
	Assert(IsA(plan, Sort));

	cost_sort(&sort_path,
			  root,
			  NIL,
			  lefttree->total_cost,
			  lefttree->plan_rows,
			  lefttree->plan_width,
			  0.0,
			  work_mem,
			  limit_tuples);
	plan->plan.startup_cost = sort_path.startup_cost;
	plan->plan.total_cost = sort_path.total_cost;
	plan->plan.plan_rows = lefttree->plan_rows;
	plan->plan.plan_width = lefttree->plan_width;
	plan->plan.parallel_aware = false;
	plan->plan.parallel_safe = lefttree->parallel_safe;
}

Plan *
decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
							 List *output_targetlist, List *clauses, List *custom_plans)
{
	DecompressChunkPath *dcpath = (DecompressChunkPath *) path;
	CustomScan *decompress_plan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);
	Path *compressed_path = linitial(path->custom_paths);
	List *settings;
	ListCell *lc;

	Assert(list_length(custom_plans) == 1);
	Assert(list_length(path->custom_paths) == 1);

	decompress_plan->flags = path->flags;
	decompress_plan->methods = &decompress_chunk_plan_methods;
	decompress_plan->scan.scanrelid = dcpath->info->chunk_rel->relid;

	if (IsA(compressed_path, IndexPath))
	{
		/*
		 * Check if any of the decompressed scan clauses are redundant with
		 * the compressed index scan clauses. Note that we can't use
		 * is_redundant_derived_clause() here, because it can't work with
		 * IndexClause's, so we use some custom code based on it.
		 */
		IndexPath *ipath = castNode(IndexPath, compressed_path);
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			ListCell *indexclause_cell = NULL;
			if (rinfo->parent_ec != NULL)
			{
				foreach (indexclause_cell, ipath->indexclauses)
				{
					IndexClause *indexclause = lfirst(indexclause_cell);
					RestrictInfo *index_rinfo = indexclause->rinfo;
					if (index_rinfo->parent_ec == rinfo->parent_ec)
					{
						break;
					}
				}
			}

			if (indexclause_cell != NULL)
			{
				/* We already have an index clause derived from same EquivalenceClass. */
				continue;
			}

			/*
			 * We don't have this clause in the underlying index scan, add it
			 * to the decompressed scan.
			 */
			decompress_plan->scan.plan.qual =
				lappend(decompress_plan->scan.plan.qual, rinfo->clause);
		}
	}
	else
	{
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
			decompress_plan->scan.plan.qual =
				lappend(decompress_plan->scan.plan.qual, rinfo->clause);
		}
	}

	decompress_plan->scan.plan.qual =
		(List *) replace_compressed_vars((Node *) decompress_plan->scan.plan.qual, dcpath->info);

	/*
	 * Try to use a physical tlist if possible. There's no reason to do the
	 * extra work of projecting the result of compressed chunk scan, because
	 * DecompressChunk can choose only the needed columns itself.
	 * Note that Postgres uses the CP_EXACT_TLIST option when planning the child
	 * paths of the Custom path, so we won't automatically get a physical tlist
	 * here.
	 */
	bool target_list_compressed_is_physical = false;
	if (compressed_path->pathtype == T_IndexOnlyScan)
	{
		compressed_scan->plan.targetlist = ((IndexPath *) compressed_path)->indexinfo->indextlist;
	}
	else
	{
		List *physical_tlist = build_physical_tlist(root, dcpath->info->compressed_rel);
		/* Can be null if the relation has dropped columns. */
		if (physical_tlist)
		{
			compressed_scan->plan.targetlist = physical_tlist;
			target_list_compressed_is_physical = true;
		}
	}

	/*
	 * Determine which columns we have to decompress.
	 * output_targetlist is sometimes empty, e.g. for a direct select from
	 * chunk. We have a ProjectionPath above DecompressChunk in this case, and
	 * the targetlist for this path is not built by the planner
	 * (CP_IGNORE_TLIST). This is why we have to examine rel pathtarget.
	 * Looking at the targetlist is not enough, we also have to decompress the
	 * columns participating in quals and in pathkeys.
	 */
	Bitmapset *uncompressed_attrs_needed = NULL;
	pull_varattnos((Node *) decompress_plan->scan.plan.qual,
				   dcpath->info->chunk_rel->relid,
				   &uncompressed_attrs_needed);
	pull_varattnos((Node *) dcpath->custom_path.path.pathtarget->exprs,
				   dcpath->info->chunk_rel->relid,
				   &uncompressed_attrs_needed);

	/*
	 * Determine which compressed column goes to which output column.
	 */
	DecompressionMapContext context = { .root = root,
										.decompress_path = dcpath,
										.uncompressed_attrs_needed = uncompressed_attrs_needed };
	build_decompression_map(&context, compressed_scan->plan.targetlist);

	/* Build heap sort info for batch sorted merge. */
	List *sort_options = NIL;

	if (dcpath->batch_sorted_merge)
	{
		/*
		 * 'order by' of the query and the 'order by' of the compressed batches
		 * match, so we will we use a heap to merge the batches. For the heap we
		 * need a compare function that determines the heap order. This function
		 * is constructed here.
		 *
		 * Batch sorted merge is done over the decompressed chunk scan tuple, so
		 * we must match the pathkeys to the decompressed chunk tupdesc.
		 */

		int numsortkeys = list_length(dcpath->custom_path.path.pathkeys);

		List *sort_col_idx = NIL;
		List *sort_ops = NIL;
		List *sort_collations = NIL;
		List *sort_nulls = NIL;

		/*
		 */
		ListCell *lc;
		foreach (lc, dcpath->custom_path.path.pathkeys)
		{
			PathKey *pk = lfirst(lc);
			EquivalenceClass *ec = pk->pk_eclass;

			/*
			 * Find the equivalence member that belongs to decompressed relation.
			 */
			ListCell *membercell = NULL;
			foreach (membercell, ec->ec_members)
			{
				EquivalenceMember *em = lfirst(membercell);

				if (em->em_is_const)
				{
					continue;
				}

				int em_relid;
				if (!bms_get_singleton_member(em->em_relids, &em_relid))
				{
					continue;
				}

				if ((Index) em_relid != dcpath->info->chunk_rel->relid)
				{
					continue;
				}

				Ensure(IsA(em->em_expr, Var),
					   "non-Var pathkey not expected for compressed batch sorted merge");

				/*
				 * We found a Var equivalence member that belongs to the
				 * decompressed relation. We have to convert its varattno which
				 * is the varattno of the uncompressed chunk tuple, to the
				 * decompressed scan tuple varattno.
				 */
				Var *var = castNode(Var, em->em_expr);
				Assert((Index) var->varno == (Index) em_relid);

				const int decompressed_scan_attno =
					context.uncompressed_attno_info[var->varattno].custom_scan_attno;
				Assert(decompressed_scan_attno > 0);

				/*
				 * Look up the correct sort operator from the PathKey's slightly
				 * abstracted representation.
				 */
				Oid sortop = get_opfamily_member(pk->pk_opfamily,
												 var->vartype,
												 var->vartype,
												 pk->pk_strategy);
				if (!OidIsValid(sortop)) /* should not happen */
					elog(ERROR,
						 "missing operator %d(%u,%u) in opfamily %u",
						 pk->pk_strategy,
						 var->vartype,
						 var->vartype,
						 pk->pk_opfamily);

				sort_col_idx = lappend_oid(sort_col_idx, decompressed_scan_attno);
				sort_collations = lappend_oid(sort_collations, var->varcollid);
				sort_nulls = lappend_oid(sort_nulls, pk->pk_nulls_first);
				sort_ops = lappend_oid(sort_ops, sortop);

				break;
			}

			Ensure(membercell != NULL,
				   "could not find matching decompressed chunk column for batch sorted merge "
				   "pathkey");
		}

		sort_options = list_make4(sort_col_idx, sort_ops, sort_collations, sort_nulls);

		/*
		 * Build a sort node for the compressed batches. The sort function is
		 * derived from the sort function of the pathkeys, except that it refers
		 * to the min and max metadata columns of the batches. We have already
		 * verified that the pathkeys match the compression order_by, so this
		 * mapping is possible.
		 */
		AttrNumber *sortColIdx = palloc(sizeof(AttrNumber) * numsortkeys);
		Oid *sortOperators = palloc(sizeof(Oid) * numsortkeys);
		Oid *collations = palloc(sizeof(Oid) * numsortkeys);
		bool *nullsFirst = palloc(sizeof(bool) * numsortkeys);
		for (int i = 0; i < numsortkeys; i++)
		{
			Oid sortop = list_nth_oid(sort_ops, i);

			/* Find the operator in pg_amop --- failure shouldn't happen */
			Oid opfamily, opcintype;
			int16 strategy;
			if (!get_ordering_op_properties(list_nth_oid(sort_ops, i),
											&opfamily,
											&opcintype,
											&strategy))
				elog(ERROR, "operator %u is not a valid ordering operator", sortOperators[i]);

			/*
			 * This way to determine the matching metadata column works, because
			 * we have already verified that the pathkeys match the compression
			 * orderby.
			 */
			Assert(strategy == BTLessStrategyNumber || strategy == BTGreaterStrategyNumber);
			char *meta_col_name = strategy == BTLessStrategyNumber ?
									  column_segment_min_name(i + 1) :
									  column_segment_max_name(i + 1);

			AttrNumber attr_position =
				get_attnum(dcpath->info->compressed_rte->relid, meta_col_name);

			if (attr_position == InvalidAttrNumber)
				elog(ERROR, "couldn't find metadata column \"%s\"", meta_col_name);

			/*
			 * If the the compressed target list is not based on the layout of
			 * the uncompressed chunk (see comment for physical_tlist above),
			 * adjust the position of the attribute.
			 */
			if (target_list_compressed_is_physical)
				sortColIdx[i] = attr_position;
			else
				sortColIdx[i] =
					find_attr_pos_in_tlist(compressed_scan->plan.targetlist, attr_position);

			sortOperators[i] = sortop;
			collations[i] = list_nth_oid(sort_collations, i);
			nullsFirst[i] = list_nth_oid(sort_nulls, i);
		}

		/* Now build the compressed batches sort node */
		Sort *sort = ts_make_sort((Plan *) compressed_scan,
								  numsortkeys,
								  sortColIdx,
								  sortOperators,
								  collations,
								  nullsFirst);

		ts_label_sort_with_costsize(root, sort, /* limit_tuples = */ -1.0);

		decompress_plan->custom_plans = list_make1(sort);
	}
	else
	{
		/*
		 * Add a sort if the compressed scan is not ordered appropriately.
		 */
		if (!pathkeys_contained_in(dcpath->required_compressed_pathkeys, compressed_path->pathkeys))
		{
			List *compressed_pks = dcpath->required_compressed_pathkeys;
			Sort *sort = ts_make_sort_from_pathkeys((Plan *) compressed_scan,
													compressed_pks,
													bms_make_singleton(compressed_scan->scanrelid));

			ts_label_sort_with_costsize(root, sort, /* limit_tuples = */ -1.0);

			decompress_plan->custom_plans = list_make1(sort);
		}
		else
		{
			decompress_plan->custom_plans = custom_plans;
		}
	}

	Assert(list_length(custom_plans) == 1);

	const bool enable_bulk_decompression = !dcpath->batch_sorted_merge &&
										   ts_guc_enable_bulk_decompression &&
										   context.have_bulk_decompression_columns;

	/*
	 * For some predicates, we have more efficient implementation that work on
	 * the entire compressed batch in one go. They go to this list, and the rest
	 * goes into the usual scan.plan.qual.
	 */
	List *vectorized_quals = NIL;
	if (enable_bulk_decompression)
	{
		List *nonvectorized_quals = NIL;
		find_vectorized_quals(&context,
							  dcpath,
							  decompress_plan->scan.plan.qual,
							  &vectorized_quals,
							  &nonvectorized_quals);

		decompress_plan->scan.plan.qual = nonvectorized_quals;
	}

#ifdef TS_DEBUG
	if (ts_guc_debug_require_vector_qual == DRO_Forbid && list_length(vectorized_quals) > 0)
	{
		elog(ERROR, "debug: encountered vector quals when they are disabled");
	}
	else if (ts_guc_debug_require_vector_qual == DRO_Require)
	{
		if (list_length(decompress_plan->scan.plan.qual) > 0)
		{
			elog(ERROR, "debug: encountered non-vector quals when they are disabled");
		}
		if (list_length(vectorized_quals) == 0)
		{
			elog(ERROR, "debug: did not encounter vector quals when they are required");
		}
	}
#endif

	settings = ts_new_list(T_IntList, DCS_Count);
	lfirst_int(list_nth_cell(settings, DCS_HypertableId)) = dcpath->info->hypertable_id;
	lfirst_int(list_nth_cell(settings, DCS_ChunkRelid)) = dcpath->info->chunk_rte->relid;
	lfirst_int(list_nth_cell(settings, DCS_Reverse)) = dcpath->reverse;
	lfirst_int(list_nth_cell(settings, DCS_BatchSortedMerge)) = dcpath->batch_sorted_merge;
	lfirst_int(list_nth_cell(settings, DCS_EnableBulkDecompression)) = enable_bulk_decompression;
	lfirst_int(list_nth_cell(settings, DCS_HasRowMarks)) = root->parse->rowMarks != NIL;

	/*
	 * Vectorized quals must go into custom_exprs, because Postgres has to see
	 * them and perform the varno adjustments on them when flattening the
	 * subqueries.
	 */
	decompress_plan->custom_exprs = list_make1(vectorized_quals);

	decompress_plan->custom_private = ts_new_list(T_List, DCP_Count);
	lfirst(list_nth_cell(decompress_plan->custom_private, DCP_Settings)) = settings;
	lfirst(list_nth_cell(decompress_plan->custom_private, DCP_DecompressionMap)) =
		context.decompression_map;
	lfirst(list_nth_cell(decompress_plan->custom_private, DCP_IsSegmentbyColumn)) =
		context.is_segmentby_column;
	lfirst(list_nth_cell(decompress_plan->custom_private, DCP_BulkDecompressionColumn)) =
		context.bulk_decompression_column;
	lfirst(list_nth_cell(decompress_plan->custom_private, DCP_SortInfo)) = sort_options;

	/*
	 * We might be using a custom scan tuple if it allows us to avoid the
	 * projection. Otherwise, this tlist is NIL and we'll be using the
	 * uncompressed tuple as the custom scan tuple.
	 */
	decompress_plan->custom_scan_tlist = context.custom_scan_targetlist;

	/*
	 * Note that we cannot decide here that we require a projection. It is
	 * decided at Path stage, now we must produce the requested targetlist.
	 */
	decompress_plan->scan.plan.targetlist = output_targetlist;

	return &decompress_plan->scan.plan;
}
