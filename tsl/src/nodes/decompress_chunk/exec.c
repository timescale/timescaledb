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
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "guc.h"
#include "nodes/decompress_chunk/batch_array.h"
#include "nodes/decompress_chunk/batch_queue_fifo.h"
#include "nodes/decompress_chunk/batch_queue_heap.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "ts_catalog/hypertable_compression.h"

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

struct BatchQueueFunctions
{
	void (*create)(DecompressChunkState *);
	void (*free)(DecompressChunkState *);
	bool (*needs_next_batch)(DecompressChunkState *);
	void (*pop)(DecompressChunkState *);
	void (*push_batch)(DecompressChunkState *, TupleTableSlot *);
	void (*reset)(DecompressChunkState *);
	TupleTableSlot *(*top_tuple)(DecompressChunkState *);
};

static const struct BatchQueueFunctions BatchQueueFunctionsFifo = {
	.create = batch_queue_fifo_create,
	.free = batch_queue_fifo_free,
	.needs_next_batch = batch_queue_fifo_needs_next_batch,
	.pop = batch_queue_fifo_pop,
	.push_batch = batch_queue_fifo_push_batch,
	.reset = batch_queue_fifo_reset,
	.top_tuple = batch_queue_fifo_top_tuple,
};

static const struct BatchQueueFunctions BatchQueueFunctionsHeap = {
	.create = batch_queue_heap_create,
	.free = batch_queue_heap_free,
	.needs_next_batch = batch_queue_heap_needs_next_batch,
	.pop = batch_queue_heap_pop,
	.push_batch = batch_queue_heap_push_batch,
	.reset = batch_queue_heap_reset,
	.top_tuple = batch_queue_heap_top_tuple,
};

/*
 * Build the sortkeys data structure from the list structure in the
 * custom_private field of the custom scan. This sort info is used to sort
 * binary heap used for sorted merge append.
 */
static void
build_batch_sorted_merge_info(DecompressChunkState *chunk_state)
{
	List *sortinfo = chunk_state->sortinfo;
	if (sortinfo == NIL)
	{
		chunk_state->n_sortkeys = 0;
		chunk_state->sortkeys = NULL;
		return;
	}

	List *sort_col_idx = linitial(sortinfo);
	List *sort_ops = lsecond(sortinfo);
	List *sort_collations = lthird(sortinfo);
	List *sort_nulls = lfourth(sortinfo);

	chunk_state->n_sortkeys = list_length(linitial((sortinfo)));

	Assert(list_length(sort_col_idx) == list_length(sort_ops));
	Assert(list_length(sort_ops) == list_length(sort_collations));
	Assert(list_length(sort_collations) == list_length(sort_nulls));
	Assert(chunk_state->n_sortkeys > 0);

	SortSupportData *sortkeys = palloc0(sizeof(SortSupportData) * chunk_state->n_sortkeys);

	/* Inspired by nodeMergeAppend.c */
	for (int i = 0; i < chunk_state->n_sortkeys; i++)
	{
		SortSupportData *sortKey = &sortkeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = list_nth_oid(sort_collations, i);
		sortKey->ssup_nulls_first = list_nth_oid(sort_nulls, i);
		sortKey->ssup_attno = list_nth_oid(sort_col_idx, i);

		/*
		 * It isn't feasible to perform abbreviated key conversion, since
		 * tuples are pulled into mergestate's binary heap as needed.  It
		 * would likely be counter-productive to convert tuples into an
		 * abbreviated representation as they're pulled up, so opt out of that
		 * additional optimization entirely.
		 */
		sortKey->abbreviate = false;

		PrepareSortSupportFromOrderingOp(list_nth_oid(sort_ops, i), sortKey);
	}

	chunk_state->sortkeys = sortkeys;
}

Node *
decompress_chunk_state_create(CustomScan *cscan)
{
	DecompressChunkState *chunk_state;

	chunk_state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	chunk_state->exec_methods = decompress_chunk_state_methods;
	chunk_state->csstate.methods = &chunk_state->exec_methods;

	Assert(IsA(cscan->custom_private, List));
	Assert(list_length(cscan->custom_private) == 5);
	List *settings = linitial(cscan->custom_private);
	chunk_state->decompression_map = lsecond(cscan->custom_private);
	chunk_state->is_segmentby_column = lthird(cscan->custom_private);
	chunk_state->bulk_decompression_column = lfourth(cscan->custom_private);
	chunk_state->sortinfo = lfifth(cscan->custom_private);

	Assert(IsA(settings, IntList));
	Assert(list_length(settings) == 5);
	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->reverse = lthird_int(settings);
	chunk_state->batch_sorted_merge = lfourth_int(settings);
	chunk_state->enable_bulk_decompression = lfifth_int(settings);

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
decompress_chunk_exec_impl(DecompressChunkState *chunk_state,
						   const struct BatchQueueFunctions *queue);

static TupleTableSlot *
decompress_chunk_exec_fifo(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	Assert(!chunk_state->batch_sorted_merge);
	return decompress_chunk_exec_impl(chunk_state, &BatchQueueFunctionsFifo);
}

static TupleTableSlot *
decompress_chunk_exec_heap(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	Assert(chunk_state->batch_sorted_merge);
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

	/* Extract sort info */
	build_batch_sorted_merge_info(chunk_state);
	/* Sort keys should only be present when sorted_merge_append is used */
	Assert(chunk_state->batch_sorted_merge == true || chunk_state->n_sortkeys == 0);
	Assert(chunk_state->n_sortkeys == 0 || chunk_state->sortkeys != NULL);

	/*
	 * Init the underlying compressed scan.
	 */
	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));

	/*
	 * Determine which columns we are going to decompress. Since in the hottest
	 * loop we work only with compressed columns, we'll put them in front of the
	 * array. So first, count how many compressed and not compressed columns
	 * we have.
	 */
	int num_compressed = 0;
	int num_total = 0;

	ListCell *dest_cell;
	ListCell *is_segmentby_cell;
	Assert(list_length(chunk_state->decompression_map) ==
		   list_length(chunk_state->is_segmentby_column));
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

		if (output_attno > 0 && !lfirst_int(is_segmentby_cell))
		{
			/*
			 * Not a metadata column and not a segmentby column, hence a
			 * compressed one.
			 */
			num_compressed++;
		}

		num_total++;
	}

	Assert(num_compressed <= num_total);
	chunk_state->num_compressed_columns = num_compressed;
	chunk_state->num_total_columns = num_total;
	chunk_state->template_columns = palloc0(sizeof(DecompressChunkColumnDescription) * num_total);

	TupleDesc desc = chunk_state->csstate.ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/*
	 * Compressed columns go in front, and the rest go to the back, so we have
	 * separate indices for them.
	 */
	int current_compressed = 0;
	int current_not_compressed = num_compressed;
	for (int compressed_index = 0; compressed_index < list_length(chunk_state->decompression_map);
		 compressed_index++)
	{
		DecompressChunkColumnDescription column = {
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
			column.value_bytes = get_typlen(column.typid);

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

		if (column.type == COMPRESSED_COLUMN)
		{
			Assert(current_compressed < num_total);
			chunk_state->template_columns[current_compressed++] = column;
		}
		else
		{
			Assert(current_not_compressed < num_total);
			chunk_state->template_columns[current_not_compressed++] = column;
		}
	}

	Assert(current_compressed == num_compressed);
	Assert(current_not_compressed == num_total);

	chunk_state->n_batch_state_bytes =
		sizeof(DecompressBatchState) +
		sizeof(CompressedColumnValues) * chunk_state->num_compressed_columns;

	/*
	 * Calculate the desired size of the batch memory context. Especially if we
	 * use bulk decompression, the results should fit into the first page of the
	 * context, otherwise it's going to do malloc/free on every
	 * MemoryContextReset.
	 *
	 * Start with the default size.
	 */
	chunk_state->batch_memory_context_bytes = ALLOCSET_DEFAULT_INITSIZE;
	if (chunk_state->enable_bulk_decompression)
	{
		for (int i = 0; i < num_total; i++)
		{
			DecompressChunkColumnDescription *column = &chunk_state->template_columns[i];
			if (column->bulk_decompression_supported)
			{
				/* Values array, with 64 element padding (actually we have less). */
				chunk_state->batch_memory_context_bytes +=
					(GLOBAL_MAX_ROWS_PER_COMPRESSION + 64) * column->value_bytes;
				/* Also nulls bitmap. */
				chunk_state->batch_memory_context_bytes +=
					GLOBAL_MAX_ROWS_PER_COMPRESSION / (64 * sizeof(uint64));
				/* Arrow data structure. */
				chunk_state->batch_memory_context_bytes +=
					sizeof(ArrowArray) + sizeof(void *) * 2 /* buffers */;
				/* Memory context header overhead for the above parts. */
				chunk_state->batch_memory_context_bytes += sizeof(void *) * 3;
			}
		}
	}

	/* Round up to even number of 4k pages. */
	chunk_state->batch_memory_context_bytes =
		((chunk_state->batch_memory_context_bytes + 4095) / 4096) * 4096;

	/* As a precaution, limit it to 1MB. */
	chunk_state->batch_memory_context_bytes =
		Min(chunk_state->batch_memory_context_bytes, 1 * 1024 * 1024);

	elog(DEBUG3,
		 "Batch memory context has initial capacity of  %d bytes",
		 chunk_state->batch_memory_context_bytes);

	/*
	 * Choose which batch queue we are going to use: heap for batch sorted
	 * merge, and one-element FIFO for normal decompression.
	 */
	if (chunk_state->batch_sorted_merge)
	{
		chunk_state->batch_queue = &BatchQueueFunctionsHeap;
		chunk_state->exec_methods.ExecCustomScan = decompress_chunk_exec_heap;
	}
	else
	{
		chunk_state->batch_queue = &BatchQueueFunctionsFifo;
		chunk_state->exec_methods.ExecCustomScan = decompress_chunk_exec_fifo;
	}

	chunk_state->batch_queue->create(chunk_state);

	if (ts_guc_debug_require_batch_sorted_merge && !chunk_state->batch_sorted_merge)
	{
		elog(ERROR, "debug: batch sorted merge is required but not used");
	}
}

/*
 * The exec function for the DecompressChunk node. It takes the explicit queue
 * functions pointer as an optimization, to allow these functions to be
 * inlined in the FIFO case. This is important because this is a part of a
 * relatively hot loop.
 */
pg_attribute_always_inline static TupleTableSlot *
decompress_chunk_exec_impl(DecompressChunkState *chunk_state,
						   const struct BatchQueueFunctions *queue)
{
	queue->pop(chunk_state);
	while (queue->needs_next_batch(chunk_state))
	{
		TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));
		if (TupIsNull(subslot))
		{
			/* Won't have more compressed tuples. */
			break;
		}

		queue->push_batch(chunk_state, subslot);
	}
	TupleTableSlot *result_slot = queue->top_tuple(chunk_state);

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

	chunk_state->batch_queue->reset(chunk_state);

	for (int i = 0; i < chunk_state->n_batch_states; i++)
	{
		batch_array_free_at(chunk_state, i);
	}

	Assert(bms_num_members(chunk_state->unused_batch_states) == chunk_state->n_batch_states);

	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
decompress_chunk_end(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	chunk_state->batch_queue->free(chunk_state);

	ExecEndNode(linitial(node->custom_ps));
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 */
static void
decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (chunk_state->batch_sorted_merge)
		{
			ExplainPropertyBool("Sorted merge append", chunk_state->batch_sorted_merge, es);
		}

		if (es->analyze && (es->verbose || es->format != EXPLAIN_FORMAT_TEXT))
		{
			ExplainPropertyBool("Bulk Decompression", chunk_state->enable_bulk_decompression, es);
		}
	}
}
