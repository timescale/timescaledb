/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <access/sysattr.h>
#include <executor/executor.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include <tcop/tcopprot.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "guc.h"
#include "import/ts_explain.h"
#include "nodes/decompress_chunk/batch_array.h"
#include "nodes/decompress_chunk/batch_queue.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"

static void decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_end(CustomScanState *node);
static void decompress_chunk_rescan(CustomScanState *node);
static void decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomExecMethods decompress_chunk_state_methods = {
	.BeginCustomScan = decompress_chunk_begin,
	.ExecCustomScan = NULL, /* To be determined later. */
	.EndCustomScan = decompress_chunk_end,
	.ReScanCustomScan = decompress_chunk_rescan,
	.ExplainCustomScan = decompress_chunk_explain,
};

/*
 * Build the sortkeys data structure from the list structure in the
 * custom_private field of the custom scan. This sort info is used to sort
 * binary heap used for batch sorted merge.
 */

Node *
decompress_chunk_state_create(CustomScan *cscan)
{
	DecompressChunkState *chunk_state;

	chunk_state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	chunk_state->exec_methods = decompress_chunk_state_methods;
	chunk_state->csstate.methods = &chunk_state->exec_methods;

	Assert(IsA(cscan->custom_private, List));
	Assert(list_length(cscan->custom_private) == DCP_Count);
	List *settings = list_nth(cscan->custom_private, DCP_Settings);
	chunk_state->decompression_map = list_nth(cscan->custom_private, DCP_DecompressionMap);
	chunk_state->is_segmentby_column = list_nth(cscan->custom_private, DCP_IsSegmentbyColumn);
	chunk_state->bulk_decompression_column =
		list_nth(cscan->custom_private, DCP_BulkDecompressionColumn);
	chunk_state->sortinfo = list_nth(cscan->custom_private, DCP_SortInfo);

	chunk_state->custom_scan_tlist = cscan->custom_scan_tlist;

	Assert(IsA(settings, IntList));
	Assert(list_length(settings) == DCS_Count);
	chunk_state->hypertable_id = list_nth_int(settings, DCS_HypertableId);
	chunk_state->chunk_relid = list_nth_int(settings, DCS_ChunkRelid);
	chunk_state->decompress_context.reverse = list_nth_int(settings, DCS_Reverse);
	chunk_state->decompress_context.batch_sorted_merge =
		list_nth_int(settings, DCS_BatchSortedMerge);
	chunk_state->decompress_context.enable_bulk_decompression =
		list_nth_int(settings, DCS_EnableBulkDecompression);

	Assert(IsA(cscan->custom_exprs, List));
	Assert(list_length(cscan->custom_exprs) == 1);
	chunk_state->vectorized_quals_original = linitial(cscan->custom_exprs);
	Assert(list_length(chunk_state->decompression_map) ==
		   list_length(chunk_state->is_segmentby_column));

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
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		if ((Index) var->varno != ctx->chunk_index)
			return node;

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
			elog(ERROR, "transparent decompression only supports tableoid system column");

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
decompress_chunk_exec_impl(DecompressChunkState *chunk_state, const BatchQueueFunctions *funcs);

static TupleTableSlot *
decompress_chunk_exec_fifo(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	Assert(!chunk_state->decompress_context.batch_sorted_merge);
	return decompress_chunk_exec_impl(chunk_state, &BatchQueueFunctionsFifo);
}

static TupleTableSlot *
decompress_chunk_exec_heap(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	Assert(chunk_state->decompress_context.batch_sorted_merge);
	return decompress_chunk_exec_impl(chunk_state, &BatchQueueFunctionsHeap);
}

/*
 * Complete initialization of the supplied CustomScanState.
 *
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	DecompressContext *dcontext = &chunk_state->decompress_context;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *compressed_scan = linitial(cscan->custom_plans);
	Assert(list_length(cscan->custom_plans) == 1);

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
	/* Sort keys should only be present when sorted_merge_append is used */
	Assert(dcontext->batch_sorted_merge == true || list_length(chunk_state->sortinfo) == 0);

	/*
	 * Init the underlying compressed scan.
	 */
	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));

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
	dcontext->decompressed_slot = node->ss.ss_ScanTupleSlot;
	dcontext->ps = &node->ss.ps;

	TupleDesc desc = dcontext->decompressed_slot->tts_tupleDescriptor;

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
			.output_attno = list_nth_int(chunk_state->decompression_map, compressed_index),
			.bulk_decompression_supported =
				list_nth_int(chunk_state->bulk_decompression_column, compressed_index)
		};

		if (column.output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		if (column.output_attno > 0)
		{
			/* normal column that is also present in decompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(column.output_attno));

			column.typid = attribute->atttypid;
			get_typlenbyval(column.typid, &column.value_bytes, &column.by_value);

			if (list_nth_int(chunk_state->is_segmentby_column, compressed_index))
				column.type = SEGMENTBY_COLUMN;
			else
				column.type = COMPRESSED_COLUMN;
		}
		else
		{
			/* metadata columns */
			switch (column.output_attno)
			{
				case DECOMPRESS_CHUNK_COUNT_ID:
					column.type = COUNT_COLUMN;
					break;
				case DECOMPRESS_CHUNK_SEQUENCE_NUM_ID:
					column.type = SEQUENCE_NUM_COLUMN;
					break;
				default:
					elog(ERROR, "Invalid column attno \"%d\"", column.output_attno);
					break;
			}
		}

		if (column.output_attno > 0)
		{
			/* Data column. */
			Assert(current_compressed < num_columns_with_metadata);
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
									dcontext->decompressed_slot->tts_tupleDescriptor,
									&BatchQueueFunctionsHeap);
		chunk_state->exec_methods.ExecCustomScan = decompress_chunk_exec_heap;
	}
	else
	{
		chunk_state->batch_queue =
			batch_queue_fifo_create(num_data_columns, &BatchQueueFunctionsFifo);
		chunk_state->exec_methods.ExecCustomScan = decompress_chunk_exec_fifo;
	}

	if (ts_guc_debug_require_batch_sorted_merge && !dcontext->batch_sorted_merge)
	{
		elog(ERROR, "debug: batch sorted merge is required but not used");
	}

	/* Constify stable expressions in vectorized predicates. */
	PlannerGlobal glob = {
		.boundParams = node->ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};
	ListCell *lc;
	foreach (lc, chunk_state->vectorized_quals_original)
	{
		Node *constified = estimate_expression_value(&root, (Node *) lfirst(lc));

		dcontext->vectorized_quals_constified =
			lappend(dcontext->vectorized_quals_constified, constified);
	}

	detoaster_init(&dcontext->detoaster, CurrentMemoryContext);
}

/*
 * The exec function for the DecompressChunk node. It takes the explicit queue
 * functions pointer as an optimization, to allow these functions to be
 * inlined in the FIFO case. This is important because this is a part of a
 * relatively hot loop.
 */
pg_attribute_always_inline static TupleTableSlot *
decompress_chunk_exec_impl(DecompressChunkState *chunk_state, const BatchQueueFunctions *bqfuncs)
{
	DecompressContext *dcontext = &chunk_state->decompress_context;
	BatchQueue *bq = chunk_state->batch_queue;

	Assert(bq->funcs == bqfuncs);

	bqfuncs->pop(bq, dcontext);

	while (bqfuncs->needs_next_batch(bq))
	{
		TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));
		if (TupIsNull(subslot))
		{
			/* Won't have more compressed tuples. */
			break;
		}

		bqfuncs->push_batch(bq, dcontext, subslot);
	}
	TupleTableSlot *result_slot = bqfuncs->top_tuple(bq);

	if (TupIsNull(result_slot))
	{
		return NULL;
	}

	if (chunk_state->csstate.ss.ps.ps_ProjInfo)
	{
		ExprContext *econtext = chunk_state->csstate.ss.ps.ps_ExprContext;
		econtext->ecxt_scantuple = result_slot;
		return ExecProject(chunk_state->csstate.ss.ps.ps_ProjInfo);
	}

	return result_slot;
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	BatchQueue *bq = chunk_state->batch_queue;

	bq->funcs->reset(bq);

	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(linitial(node->custom_ps), node->ss.ps.chgParam);

	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
decompress_chunk_end(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	BatchQueue *bq = chunk_state->batch_queue;

	bq->funcs->free(bq);
	ExecEndNode(linitial(node->custom_ps));

	detoaster_close(&chunk_state->decompress_context.detoaster);
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 */
static void
decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	DecompressContext *dcontext = &chunk_state->decompress_context;

	ts_show_scan_qual(chunk_state->vectorized_quals_original,
					  "Vectorized Filter",
					  &node->ss.ps,
					  ancestors,
					  es);

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
		if (dcontext->batch_sorted_merge)
		{
			ExplainPropertyBool("Batch Sorted Merge", dcontext->batch_sorted_merge, es);
		}

		if (es->analyze && (es->verbose || es->format != EXPLAIN_FORMAT_TEXT))
		{
			ExplainPropertyBool("Bulk Decompression",
								chunk_state->decompress_context.enable_bulk_decompression,
								es);
		}
	}
}
