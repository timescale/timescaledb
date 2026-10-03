/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "ts_stats/ts_stats_defs.h"
#include <access/sysattr.h>
#include <executor/executor.h>
#include <executor/nodeSubplan.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <tcop/tcopprot.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "guc.h"
#include "import/commands/explain.h"
#include "nodes/columnar_scan/batch_array.h"
#include "nodes/columnar_scan/batch_queue_fifo.h"
#include "nodes/columnar_scan/batch_queue_heap.h"
#include "nodes/columnar_scan/batch_seek.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/columnar_scan/compressed_batch.h"
#include "nodes/columnar_scan/exec.h"
#include "nodes/columnar_scan/planner.h"
#include "ts_catalog/array_utils.h"
#include "ts_stats/ts_stats_record.h"

#if PG18_GE
#include <commands/explain_format.h>
#include <commands/explain_state.h>
#endif

static void columnar_scan_begin(CustomScanState *node, EState *estate, int eflags);
static void columnar_scan_end(CustomScanState *node);
static void columnar_scan_rescan(CustomScanState *node);
static void columnar_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomExecMethods columnar_scan_state_methods = {
	.BeginCustomScan = columnar_scan_begin,
	.ExecCustomScan = NULL, /* To be determined later. */
	.EndCustomScan = columnar_scan_end,
	.ReScanCustomScan = columnar_scan_rescan,
	.ExplainCustomScan = columnar_scan_explain,
};

/*
 * Build the sortkeys data structure from the list structure in the
 * custom_private field of the custom scan. This sort info is used to sort
 * binary heap used for batch sorted merge.
 */

Node *
columnar_scan_state_create(CustomScan *cscan)
{
	ColumnarScanState *chunk_state;

	chunk_state = (ColumnarScanState *) newNode(sizeof(ColumnarScanState), T_CustomScanState);

	chunk_state->exec_methods = columnar_scan_state_methods;
	chunk_state->csstate.methods = &chunk_state->exec_methods;

	Assert(IsA(cscan->custom_private, List));
	Assert(list_length(cscan->custom_private) == DCP_Count);
	List *settings = list_nth(cscan->custom_private, DCP_Settings);
	chunk_state->decompression_map = list_nth(cscan->custom_private, DCP_DecompressionMap);
	chunk_state->is_segmentby_column = list_nth(cscan->custom_private, DCP_IsSegmentbyColumn);
	chunk_state->bulk_decompression_column =
		list_nth(cscan->custom_private, DCP_BulkDecompressionColumn);
	chunk_state->sortinfo = list_nth(cscan->custom_private, DCP_SortInfo);
	chunk_state->metadata_qual_attnos = list_nth(cscan->custom_private, DCP_MetadataQuals);

	chunk_state->custom_scan_tlist = cscan->custom_scan_tlist;

	Assert(IsA(settings, IntList));
	Assert(list_length(settings) == DCS_Count);
	chunk_state->hypertable_id = list_nth_int(settings, DCS_HypertableId);
	chunk_state->chunk_relid = list_nth_int(settings, DCS_ChunkRelid);
	chunk_state->decompress_context.reverse = list_nth_int(settings, DCS_Reverse);
	chunk_state->decompress_context.batch_sorted_merge =
		list_nth_int(settings, DCS_BatchSortedMerge);
	chunk_state->decompress_context.chunk_status = list_nth_int(settings, DCS_ChunkStatus);
	chunk_state->decompress_context.enable_bulk_decompression =
		list_nth_int(settings, DCS_EnableBulkDecompression);
	chunk_state->has_row_marks = list_nth_int(settings, DCS_HasRowMarks);

	Assert(IsA(cscan->custom_exprs, List));
	Assert(list_length(cscan->custom_exprs) == 2);
	chunk_state->vectorized_quals_original = linitial(cscan->custom_exprs);
	Assert(list_length(chunk_state->decompression_map) ==
		   list_length(chunk_state->is_segmentby_column));

	chunk_state->done_fetching_batches = false;
	return (Node *) chunk_state;
}

typedef struct ConstifyTableOidContext
{
	Index chunk_index;
	Oid chunk_relid;
	bool made_changes;
} ConstifyTableOidContext;

static Node *
constify_tableoid_walker(Node *node, ConstifyTableOidContext *ctx)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		if ((Index) var->varno != ctx->chunk_index)
		{
			return node;
		}

		if (var->varattno == TableOidAttributeNumber)
		{
			ctx->made_changes = true;
			return (
				Node *) makeConst(OIDOID, -1, InvalidOid, 4, (Datum) ctx->chunk_relid, false, true);
		}

		/*
		 * we doublecheck system columns here because projection will
		 * segfault if any system columns get through
		 */
		if (var->varattno < SelfItemPointerAttributeNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("transparent decompression only supports tableoid system column")));
		}

		return node;
	}

	return expression_tree_mutator(node, constify_tableoid_walker, (void *) ctx);
}

static List *
constify_tableoid(List *node, Index chunk_index, Oid chunk_relid)
{
	ConstifyTableOidContext ctx = {
		.chunk_index = chunk_index,
		.chunk_relid = chunk_relid,
		.made_changes = false,
	};

	List *result = (List *) constify_tableoid_walker((Node *) node, &ctx);
	if (ctx.made_changes)
	{
		return result;
	}

	return node;
}

pg_attribute_always_inline static TupleTableSlot *
columnar_scan_exec_impl(ColumnarScanState *chunk_state, const BatchQueueFunctions *funcs);

static TupleTableSlot *
columnar_scan_exec_fifo(CustomScanState *node)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	Assert(!chunk_state->decompress_context.batch_sorted_merge);
	return columnar_scan_exec_impl(chunk_state, &BatchQueueFunctionsFifo);
}

static TupleTableSlot *
columnar_scan_exec_heap(CustomScanState *node)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	Assert(chunk_state->decompress_context.batch_sorted_merge);
	return columnar_scan_exec_impl(chunk_state, &BatchQueueFunctionsHeap);
}

static bool
is_exec_param(Node *node)
{
	return node != NULL && IsA(node, Param) && castNode(Param, node)->paramkind == PARAM_EXEC;
}

/*
 * The current value of a join or initplan parameter. An initplan that has not
 * run yet runs now.
 */
static ParamExecData *
exec_param_value(ExprContext *econtext, int paramid)
{
	ParamExecData *prm = &econtext->ecxt_param_exec_vals[paramid];
	if (prm->execPlan != NULL)
	{
		ExecSetParamPlan(prm->execPlan, econtext);
		Assert(prm->execPlan == NULL);
	}
	return prm;
}

static Const *
make_param_const(Param *param, Datum value, bool isnull)
{
	int16 typlen;
	bool typbyval;
	get_typlenbyval(param->paramtype, &typlen, &typbyval);
	return makeConst(param->paramtype,
					 param->paramtypmod,
					 param->paramcollid,
					 typlen,
					 isnull ? (Datum) 0 : datumCopy(value, typbyval, typlen),
					 isnull,
					 typbyval);
}

/* Fold stable functions and external parameters, like the planner does. */
static Node *
estimate_value(DecompressContext *dcontext, Node *node)
{
	PlannerGlobal glob = {
		.boundParams = dcontext->ps->state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};
	return estimate_expression_value(&root, node);
}

static bool
collect_exec_params(Node *node, Bitmapset **params)
{
	if (node == NULL)
	{
		return false;
	}

	if (is_exec_param(node))
	{
		*params = bms_add_member(*params, castNode(Param, node)->paramid);
		return false;
	}

	return expression_tree_walker(node, collect_exec_params, params);
}

/*
 * Replace join and initplan parameters with their current values.
 */
static Node *
exec_params_to_consts(Node *node, ExprContext *econtext)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (is_exec_param(node))
	{
		Param *param = castNode(Param, node);
		ParamExecData *prm = exec_param_value(econtext, param->paramid);
		return (Node *) make_param_const(param, prm->value, prm->isnull);
	}

	return expression_tree_mutator(node, exec_params_to_consts, econtext);
}

/*
 * Whether a join or initplan parameter is used other than in "column op
 * parameter". Such a parameter needs constant folding once its value is known,
 * so the quals can't be reused with only new parameter values.
 */
static bool
has_folded_params(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (is_exec_param(node))
	{
		return true;
	}

	List *args = IsA(node, OpExpr)			  ? castNode(OpExpr, node)->args :
				 IsA(node, ScalarArrayOpExpr) ? castNode(ScalarArrayOpExpr, node)->args :
												NIL;
	if (list_length(args) == 2 && IsA(ts_strip_relabel_types(linitial(args)), Var) &&
		is_exec_param(lsecond(args)))
	{
		return false;
	}

	return expression_tree_walker(node, has_folded_params, context);
}

typedef struct ParamConstsContext
{
	List *consts;
	List *param_ids;
} ParamConstsContext;

/* Replace each parameter with a Const whose value is set before each use. */
static Node *
params_to_placeholders(Node *node, ParamConstsContext *ctx)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (is_exec_param(node))
	{
		Param *param = castNode(Param, node);
		Const *c = make_param_const(param, (Datum) 0, true);
		ctx->consts = lappend(ctx->consts, c);
		ctx->param_ids = lappend_int(ctx->param_ids, param->paramid);
		return (Node *) c;
	}

	return expression_tree_mutator(node, params_to_placeholders, ctx);
}

static void
setup_vectorized_quals_template(DecompressContext *dcontext)
{
	List *folded = NIL;
	ListCell *lc;
	foreach (lc, dcontext->vectorized_quals_original)
	{
		Node *qual = estimate_value(dcontext, lfirst(lc));
		if (has_folded_params(qual, NULL))
		{
			return;
		}
		folded = lappend(folded, qual);
	}

	ParamConstsContext ctx = { 0 };
	dcontext->vectorized_quals_template = (List *) params_to_placeholders((Node *) folded, &ctx);
	dcontext->vectorized_quals_param_consts = ctx.consts;
	dcontext->vectorized_quals_param_ids = ctx.param_ids;
	dcontext->vectorized_quals_values_context =
		AllocSetContextCreate(CurrentMemoryContext,
							  "vectorized qual parameter values",
							  ALLOCSET_SMALL_SIZES);
}

/*
 * Put the current parameter values into the template. Returns false for a
 * NULL value, because then the qual has to be folded to a constant.
 */
static bool
update_vectorized_quals_template(DecompressContext *dcontext)
{
	ExprContext *econtext = dcontext->ps->ps_ExprContext;
	MemoryContextReset(dcontext->vectorized_quals_values_context);
	MemoryContext old = MemoryContextSwitchTo(dcontext->vectorized_quals_values_context);

	bool ok = true;
	ListCell *lc_const, *lc_id;
	forboth (lc_const,
			 dcontext->vectorized_quals_param_consts,
			 lc_id,
			 dcontext->vectorized_quals_param_ids)
	{
		Const *c = lfirst(lc_const);
		ParamExecData *prm = exec_param_value(econtext, lfirst_int(lc_id));
		if (prm->isnull)
		{
			ok = false;
			break;
		}

		c->constvalue = datumCopy(prm->value, c->constbyval, c->constlen);
		c->constisnull = false;
	}

	MemoryContextSwitchTo(old);
	return ok;
}

void
columnar_scan_constify_vectorized_quals(DecompressContext *dcontext)
{
	if (dcontext->vectorized_quals_template != NULL && update_vectorized_quals_template(dcontext))
	{
		dcontext->vectorized_quals_constified = dcontext->vectorized_quals_template;
		dcontext->vectorized_quals_stale = false;
		return;
	}

	MemoryContext old = CurrentMemoryContext;
	if (dcontext->vectorized_quals_context != NULL)
	{
		MemoryContextReset(dcontext->vectorized_quals_context);
		MemoryContextSwitchTo(dcontext->vectorized_quals_context);
	}

	List *constified = NIL;
	ListCell *lc;
	foreach (lc, dcontext->vectorized_quals_original)
	{
		Node *qual = lfirst(lc);
		if (dcontext->vectorized_quals_params != NULL)
		{
			qual = exec_params_to_consts(qual, dcontext->ps->ps_ExprContext);
		}
		constified = lappend(constified, estimate_value(dcontext, qual));
	}

	MemoryContextSwitchTo(old);
	dcontext->vectorized_quals_constified = constified;
	dcontext->vectorized_quals_stale = false;
}

static bool
collect_var_attnos(Node *node, Bitmapset **attnos)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		if (var->varlevelsup == 0)
		{
			*attnos = bms_add_member(*attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
		}
		return false;
	}

	return expression_tree_walker(node, collect_var_attnos, attnos);
}

static int
find_data_column(DecompressContext *dcontext, AttrNumber custom_scan_attno)
{
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
		if (dcontext->compressed_chunk_columns[i].custom_scan_attno == custom_scan_attno)
		{
			return i;
		}
	}
	return -1;
}

/*
 * Find the position of a compressed chunk column in the compressed scan tuple.
 */
static AttrNumber
find_compressed_scan_attno(Plan *compressed_scan, AttrNumber compressed_chunk_attno)
{
	if (compressed_chunk_attno == InvalidAttrNumber)
	{
		return InvalidAttrNumber;
	}

	ListCell *lc;
	foreach (lc, compressed_scan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (IsA(tle->expr, Var) && castNode(Var, tle->expr)->varattno == compressed_chunk_attno)
		{
			return tle->resno;
		}
	}
	return InvalidAttrNumber;
}

/*
 * Find the vectorized quals of the form "column op something" where the batch
 * metadata has the min and max values of the column, and the columns that only
 * the vectorized quals read.
 */
static void
setup_batch_metadata_quals(ColumnarScanState *chunk_state, Plan *compressed_scan)
{
	DecompressContext *dcontext = &chunk_state->decompress_context;
	Plan *plan = chunk_state->csstate.ss.ps.plan;

	if (!ts_guc_enable_columnar_batch_metadata_quals || dcontext->batch_sorted_merge ||
		dcontext->vectorized_quals_original == NIL)
	{
		return;
	}

	/* Columns that are needed for output or for the remaining quals. */
	Bitmapset *needed = NULL;
	collect_var_attnos((Node *) plan->targetlist, &needed);
	collect_var_attnos((Node *) plan->qual, &needed);
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, needed))
	{
		return;
	}

	bool *qual_only = palloc0(sizeof(bool) * dcontext->num_data_columns);
	bool any_qual_only = false;
	Bitmapset *qual_attnos = NULL;
	collect_var_attnos((Node *) dcontext->vectorized_quals_original, &qual_attnos);
	int k = -1;
	while ((k = bms_next_member(qual_attnos, k)) >= 0)
	{
		if (bms_is_member(k, needed))
		{
			continue;
		}
		int i = find_data_column(dcontext, k + FirstLowInvalidHeapAttributeNumber);
		if (i >= 0 && dcontext->compressed_chunk_columns[i].type == COMPRESSED_COLUMN)
		{
			qual_only[i] = true;
			any_qual_only = true;
		}
	}
	dcontext->qual_only_columns = any_qual_only ? qual_only : NULL;

	/* The metadata columns for each qual were found at planning time. */
	List *attnos = chunk_state->metadata_qual_attnos;
	if (attnos == NIL)
	{
		return;
	}

	int nquals = list_length(dcontext->vectorized_quals_original);
	Assert(list_length(attnos) == 2 * nquals);
	BatchMetadataQual *mquals = palloc0(sizeof(BatchMetadataQual) * nquals);
	bool any_metadata = false;
	ListCell *lc;
	foreach (lc, dcontext->vectorized_quals_original)
	{
		Node *qual = lfirst(lc);
		if (!IsA(qual, OpExpr) || list_length(castNode(OpExpr, qual)->args) != 2)
		{
			continue;
		}

		OpExpr *opexpr = castNode(OpExpr, qual);
		Node *arg = ts_strip_relabel_types(linitial(opexpr->args));
		if (!IsA(arg, Var))
		{
			continue;
		}

		int i = find_data_column(dcontext, castNode(Var, arg)->varattno);
		if (i < 0 || dcontext->compressed_chunk_columns[i].type != COMPRESSED_COLUMN)
		{
			continue;
		}
		CompressionColumnDescription *column = &dcontext->compressed_chunk_columns[i];

		AttrNumber min_attno = list_nth_int(attnos, 2 * foreach_current_index(lc));
		AttrNumber max_attno = list_nth_int(attnos, 2 * foreach_current_index(lc) + 1);
		if (min_attno == InvalidAttrNumber)
		{
			continue;
		}

		min_attno = find_compressed_scan_attno(compressed_scan, min_attno);
		max_attno = find_compressed_scan_attno(compressed_scan, max_attno);
		if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
		{
			continue;
		}

		BatchMetadataQual *mq = &mquals[foreach_current_index(lc)];
		mq->min_attno = min_attno;
		mq->max_attno = max_attno;
		mq->typlen = column->value_bytes;
		mq->typbyval = column->by_value;
		mq->inputcollid = opexpr->inputcollid;
		fmgr_info(get_opcode(opexpr->opno), &mq->opfn);
		any_metadata = true;
	}
	dcontext->metadata_quals = any_metadata ? mquals : NULL;
}

/*
 * Complete initialization of the supplied CustomScanState.
 *
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
columnar_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	DecompressContext *dcontext = &chunk_state->decompress_context;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *compressed_scan = linitial(cscan->custom_plans);
	Assert(list_length(cscan->custom_plans) == 1);

	chunk_state->done_fetching_batches = false;
	ts_stats_compression_acc_init(&dcontext->observ_acc);

	PlanState *ps = &node->ss.ps;
	if (ps->ps_ProjInfo)
	{
		/*
		 * if we are projecting we need to constify tableoid references here
		 * because decompressed tuple are virtual tuples and don't have
		 * system columns.
		 *
		 * We do the constify in executor because even after plan creation
		 * our targetlist might still get modified by parent nodes pushing
		 * down targetlist.
		 */
		List *tlist = ps->plan->targetlist;
		List *modified_tlist =
			constify_tableoid(tlist, cscan->scan.scanrelid, chunk_state->chunk_relid);

		if (modified_tlist != tlist)
		{
			ps->ps_ProjInfo =
				ExecBuildProjectionInfo(modified_tlist,
										ps->ps_ExprContext,
										ps->ps_ResultTupleSlot,
										ps,
										node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
		}
	}

	/*
	 * Sort keys should only be present at the level of this node when batch
	 * sorted merge is used.
	 * In other cases of sort pushdown, sorting is performed by the underlying
	 * compressed scan.
	 */
	Assert(dcontext->batch_sorted_merge == true || list_length(chunk_state->sortinfo) == 0);

	/*
	 * Init the underlying compressed scan.
	 */
	List *batch_seek_private = list_nth(cscan->custom_private, DCP_BatchSeek);
	if (batch_seek_private != NIL)
	{
		/* The index scan below is set up only when the seek needs it. */
		chunk_state->batch_seek = batch_seek_begin(node,
												   batch_seek_private,
												   linitial(lsecond(cscan->custom_exprs)),
												   compressed_scan,
												   chunk_state->chunk_relid,
												   estate,
												   eflags);
	}
	else
	{
		node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));
	}

	/*
	 * Count the actual data columns we have to decompress, skipping the
	 * metadata columns. We only need the metadata columns when initializing the
	 * compressed batch, so they are not saved in the compressed batch itself,
	 * it tracks only the data columns. We put the metadata columns to the end
	 * of the array to have the same column indexes in compressed batch state
	 * and in decompression context.
	 */
	int num_data_columns = 0;
	int num_columns_with_metadata = 0;

	ListCell *dest_cell;
	ListCell *is_segmentby_cell;

	forboth (dest_cell,
			 chunk_state->decompression_map,
			 is_segmentby_cell,
			 chunk_state->is_segmentby_column)
	{
		AttrNumber output_attno = lfirst_int(dest_cell);
		if (output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		if (output_attno > 0)
		{
			/*
			 * Not a metadata column.
			 */
			num_data_columns++;
		}

		num_columns_with_metadata++;
	}

	Assert(num_data_columns <= num_columns_with_metadata);
	dcontext->num_data_columns = num_data_columns;
	dcontext->num_columns_with_metadata = num_columns_with_metadata;
	dcontext->compressed_chunk_columns =
		palloc0(sizeof(CompressionColumnDescription) * num_columns_with_metadata);
	dcontext->custom_scan_slot = node->ss.ss_ScanTupleSlot;
	dcontext->uncompressed_chunk_tdesc = RelationGetDescr(node->ss.ss_currentRelation);
	dcontext->ps = &node->ss.ps;

	TupleDesc desc = dcontext->custom_scan_slot->tts_tupleDescriptor;

	/*
	 * Compressed columns go in front, and the rest go to the back, so we have
	 * separate indices for them.
	 */
	int current_compressed = 0;
	int current_not_compressed = num_data_columns;
	for (int compressed_index = 0; compressed_index < list_length(chunk_state->decompression_map);
		 compressed_index++)
	{
		CompressionColumnDescription column = {
			.compressed_scan_attno = AttrOffsetGetAttrNumber(compressed_index),
			.custom_scan_attno = list_nth_int(chunk_state->decompression_map, compressed_index),
			.bulk_decompression_supported =
				list_nth_int(chunk_state->bulk_decompression_column, compressed_index)
		};

		if (column.custom_scan_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		if (column.custom_scan_attno > 0)
		{
			/* normal column that is also present in decompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(column.custom_scan_attno));

			column.typid = attribute->atttypid;
			get_typlenbyval(column.typid, &column.value_bytes, &column.by_value);

			if (list_nth_int(chunk_state->is_segmentby_column, compressed_index))
			{
				column.type = SEGMENTBY_COLUMN;
			}
			else
			{
				column.type = COMPRESSED_COLUMN;
			}

			if (cscan->custom_scan_tlist == NIL)
			{
				column.uncompressed_chunk_attno = column.custom_scan_attno;
			}
			else
			{
				Var *var =
					castNode(Var,
							 castNode(TargetEntry,
									  list_nth(cscan->custom_scan_tlist,
											   AttrNumberGetAttrOffset(column.custom_scan_attno)))
								 ->expr);
				column.uncompressed_chunk_attno = var->varattno;
			}
		}
		else
		{
			/* metadata columns */
			switch (column.custom_scan_attno)
			{
				case COLUMNAR_SCAN_COUNT_ID:
					column.type = COUNT_COLUMN;
					break;
				case COLUMNAR_SCAN_SEQUENCE_NUM_ID:
					column.type = SEQUENCE_NUM_COLUMN;
					break;
				default:
					elog(ERROR, "Invalid column attno \"%d\"", column.custom_scan_attno);
					break;
			}
		}

		if (column.custom_scan_attno > 0)
		{
			/* Data column. */
			Assert(current_compressed < num_data_columns);
			dcontext->compressed_chunk_columns[current_compressed++] = column;
		}
		else
		{
			/* Metadata column. */
			Assert(current_not_compressed < num_columns_with_metadata);
			dcontext->compressed_chunk_columns[current_not_compressed++] = column;
		}
	}

	Assert(current_compressed == num_data_columns);
	Assert(current_not_compressed == num_columns_with_metadata);

	/*
	 * Choose which batch queue we are going to use: heap for batch sorted
	 * merge, and one-element FIFO for normal decompression.
	 */
	if (dcontext->batch_sorted_merge)
	{
		chunk_state->batch_queue =
			batch_queue_heap_create(num_data_columns,
									chunk_state->sortinfo,
									dcontext->custom_scan_slot->tts_tupleDescriptor,
									&BatchQueueFunctionsHeap);
		chunk_state->exec_methods.ExecCustomScan = columnar_scan_exec_heap;
	}
	else
	{
		chunk_state->batch_queue =
			batch_queue_fifo_create(num_data_columns, &BatchQueueFunctionsFifo);
		chunk_state->exec_methods.ExecCustomScan = columnar_scan_exec_fifo;
	}

	if ((ts_guc_debug_require_batch_sorted_merge == DRO_Require ||
		 ts_guc_debug_require_batch_sorted_merge == DRO_Force) &&
		!dcontext->batch_sorted_merge)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("debug: batch sorted merge is required but not used")));
	}

	if (ts_guc_debug_require_batch_sorted_merge == DRO_Forbid && dcontext->batch_sorted_merge)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("debug: batch sorted merge is used when it is forbidden")));
	}

	/*
	 * Constify stable expressions in vectorized predicates. With join or
	 * initplan parameters, this has to wait until their values are known, so
	 * it happens before the first batch and again after they change.
	 */
	dcontext->vectorized_quals_original = chunk_state->vectorized_quals_original;
	collect_exec_params((Node *) dcontext->vectorized_quals_original,
						&dcontext->vectorized_quals_params);
	if (dcontext->vectorized_quals_params != NULL)
	{
		dcontext->vectorized_quals_context =
			AllocSetContextCreate(CurrentMemoryContext,
								  "vectorized quals with parameters",
								  ALLOCSET_SMALL_SIZES);
		dcontext->vectorized_quals_stale = true;
		setup_vectorized_quals_template(dcontext);
	}
	else
	{
		columnar_scan_constify_vectorized_quals(dcontext);
	}

	setup_batch_metadata_quals(chunk_state, compressed_scan);

	detoaster_init(&dcontext->detoaster, CurrentMemoryContext);
}

TupleTableSlot *
columnar_scan_next_compressed(ColumnarScanState *chunk_state)
{
	if (chunk_state->batch_seek != NULL)
	{
		return batch_seek_next(chunk_state->batch_seek);
	}
	return ExecProcNode(linitial(chunk_state->csstate.custom_ps));
}

/*
 * The exec function for the ColumnarScan node. It takes the explicit queue
 * functions pointer as an optimization, to allow these functions to be
 * inlined in the FIFO case. This is important because this is a part of a
 * relatively hot loop.
 */
pg_attribute_always_inline static TupleTableSlot *
columnar_scan_exec_impl(ColumnarScanState *chunk_state, const BatchQueueFunctions *bqfuncs)
{
	DecompressContext *dcontext = &chunk_state->decompress_context;
	BatchQueue *bq = chunk_state->batch_queue;

	Assert(bq->funcs == bqfuncs);

	bqfuncs->pop(bq, dcontext);

	while (!chunk_state->done_fetching_batches && bqfuncs->needs_next_batch(bq))
	{
		TupleTableSlot *subslot = columnar_scan_next_compressed(chunk_state);
		if (TupIsNull(subslot))
		{
			/* Won't have more compressed tuples. */
			chunk_state->done_fetching_batches = true;
			break;
		}

		bqfuncs->push_batch(bq, dcontext, subslot);
	}
	TupleTableSlot *result_slot = bqfuncs->top_tuple(bq);

	if (TupIsNull(result_slot))
	{
		return NULL;
	}

	if (chunk_state->has_row_marks)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("locking compressed tuples is not supported")));
	}

	if (chunk_state->csstate.ss.ps.ps_ProjInfo)
	{
		ExprContext *econtext = chunk_state->csstate.ss.ps.ps_ExprContext;
		ResetExprContext(econtext);
		econtext->ecxt_scantuple = result_slot;
		return ExecProject(chunk_state->csstate.ss.ps.ps_ProjInfo);
	}

	return result_slot;
}

static void
columnar_scan_rescan(CustomScanState *node)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	chunk_state->done_fetching_batches = false;
	BatchQueue *bq = chunk_state->batch_queue;

	bq->funcs->reset(bq);

	DecompressContext *dcontext = &chunk_state->decompress_context;
	if (bms_overlap(node->ss.ps.chgParam, dcontext->vectorized_quals_params))
	{
		dcontext->vectorized_quals_stale = true;
	}

	if (chunk_state->batch_seek != NULL)
	{
		batch_seek_rescan(chunk_state->batch_seek, node->ss.ps.chgParam);
		return;
	}

	if (node->ss.ps.chgParam != NULL)
	{
		UpdateChangedParamSet(linitial(node->custom_ps), node->ss.ps.chgParam);
	}

	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
columnar_scan_end(CustomScanState *node)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	BatchQueue *bq = chunk_state->batch_queue;
	DecompressContext *dcontext = &chunk_state->decompress_context;

	bq->funcs->free(bq);
	if (chunk_state->batch_seek != NULL)
	{
		batch_seek_end(chunk_state->batch_seek);
	}
	else
	{
		ExecEndNode(linitial(node->custom_ps));
	}

	detoaster_close(&chunk_state->decompress_context.detoaster);

	/* Finish the observability aggregates. */
	SharedCounters sc = { 0 };
	sc.tuples_decompressed = dcontext->tuples_decompressed;
	sc.batches_decompressed = dcontext->batches_decompressed;
	TsStatsRelids relids = {
		.compressed_relid = dcontext->compressed_relid,
		.uncompressed_relid = chunk_state->chunk_relid,
	};
	ts_stats_chunk_record_cmd(relids, CMD_SELECT, &sc);

	ts_stats_chunk_record_decompression(relids, &dcontext->observ_acc);
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 */
static void
columnar_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ColumnarScanState *chunk_state = (ColumnarScanState *) node;
	DecompressContext *dcontext = &chunk_state->decompress_context;

	ts_show_scan_qual(chunk_state->vectorized_quals_original,
					  "Vectorized Filter",
					  &node->ss.ps,
					  ancestors,
					  es);

	if (chunk_state->batch_seek != NULL)
	{
		batch_seek_explain(chunk_state->batch_seek, ancestors, es);
	}

	if (!node->ss.ps.plan->qual && chunk_state->vectorized_quals_original)
	{
		/*
		 * The normal explain won't show this if there are no normal quals but
		 * only the vectorized ones.
		 */
		ts_show_instrumentation_count("Rows Removed by Filter", 1, &node->ss.ps, es);
	}

	if (es->analyze && es->verbose &&
		(node->ss.ps.instrument->ntuples2 > 0 || es->format != EXPLAIN_FORMAT_TEXT))
	{
		ExplainPropertyFloat("Batches Removed by Filter",
							 NULL,
							 node->ss.ps.instrument->ntuples2,
							 0,
							 es);
	}

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		/* Display any statuses in addition to COMPRESSED */
		if (dcontext->chunk_status > CHUNK_STATUS_COMPRESSED)
		{
			StringInfoData status_text;
			initStringInfo(&status_text);
			ArrayType *arr =
				DatumGetArrayTypeP(DirectFunctionCall1(ts_chunk_status_text,
													   Int32GetDatum(dcontext->chunk_status -
																	 CHUNK_STATUS_COMPRESSED)));
			ts_array_append_stringinfo(arr, &status_text);
			pfree(arr);
			ExplainPropertyText("Chunk Status", status_text.data, es);
		}

		if (dcontext->batch_sorted_merge)
		{
			ExplainPropertyBool("Batch Sorted Merge", dcontext->batch_sorted_merge, es);
		}

		if (dcontext->reverse)
		{
			ExplainPropertyBool("Reverse", dcontext->reverse, es);
		}

		if (es->analyze)
		{
			ExplainPropertyBool("Bulk Decompression",
								chunk_state->decompress_context.enable_bulk_decompression,
								es);
		}
	}
}
