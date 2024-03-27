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
	Assert(list_length(cscan->custom_private) == 6);
	List *settings = linitial(cscan->custom_private);
	chunk_state->decompression_map = lsecond(cscan->custom_private);
	chunk_state->is_segmentby_column = lthird(cscan->custom_private);
	chunk_state->bulk_decompression_column = lfourth(cscan->custom_private);
	chunk_state->aggregated_column_type = lfifth(cscan->custom_private);
	chunk_state->sortinfo = lsixth(cscan->custom_private);
	chunk_state->custom_scan_tlist = cscan->custom_scan_tlist;

	Assert(IsA(settings, IntList));
	Assert(list_length(settings) == 6);
	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->decompress_context.reverse = lthird_int(settings);
	chunk_state->decompress_context.batch_sorted_merge = lfourth_int(settings);
	chunk_state->decompress_context.enable_bulk_decompression = lfifth_int(settings);
	chunk_state->perform_vectorized_aggregation = lsixth_int(settings);

	Assert(IsA(cscan->custom_exprs, List));
	Assert(list_length(cscan->custom_exprs) == 1);
	chunk_state->vectorized_quals_original = linitial(cscan->custom_exprs);
	Assert(list_length(chunk_state->decompression_map) ==
		   list_length(chunk_state->is_segmentby_column));

#ifdef USE_ASSERT_CHECKING
	if (chunk_state->perform_vectorized_aggregation)
	{
		Assert(list_length(chunk_state->decompression_map) ==
			   list_length(chunk_state->aggregated_column_type));
	}
#endif

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

static int
compressed_column_cmp(const void *_left, const void *_right)
{
	const CompressionColumnDescription *left = (const CompressionColumnDescription *) _left;
	const CompressionColumnDescription *right = (const CompressionColumnDescription *) _right;

	Assert(left->type == COMPRESSED_COLUMN);
	Assert(right->type == COMPRESSED_COLUMN);

	if (left->output_attno < right->output_attno)
	{
		return -1;
	}

	if (left->output_attno > right->output_attno)
	{
		return 1;
	}

	return 0;
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

	dcontext->decompressed_slot = node->ss.ss_ScanTupleSlot;

	PlanState *ps = &node->ss.ps;

	ps->scanopsfixed = false;
	ps->resultopsfixed = false;

	/* initialize child expressions */
	ps->qual = ExecInitQual(cscan->scan.plan.qual, ps);

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

		ps->ps_ProjInfo = ExecBuildProjectionInfo(modified_tlist,
												  ps->ps_ExprContext,
												  ps->ps_ResultTupleSlot,
												  ps,
												  node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	}
	/* Sort keys should only be present when sorted_merge_append is used */
	Assert(dcontext->batch_sorted_merge == true || list_length(chunk_state->sortinfo) == 0);

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
	dcontext->num_compressed_columns = num_compressed;
	dcontext->num_total_columns = num_total;
	dcontext->template_columns = palloc0(sizeof(CompressionColumnDescription) * num_total);
	dcontext->ps = &node->ss.ps;

	TupleDesc desc = dcontext->decompressed_slot->tts_tupleDescriptor;

	/*
	 * Compressed columns go in front, and the rest go to the back, so we have
	 * separate indices for them.
	 */
	int current_compressed = 0;
	int current_not_compressed = num_compressed;
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
			if (chunk_state->perform_vectorized_aggregation &&
				lfirst_int(list_nth_cell(chunk_state->aggregated_column_type, compressed_index)) !=
					-1)
			{
				column.typid = lfirst_int(
					list_nth_cell(chunk_state->aggregated_column_type, compressed_index));
			}
			else
			{
				/* normal column that is also present in decompressed chunk */
				Form_pg_attribute attribute =
					TupleDescAttr(desc, AttrNumberGetAttrOffset(column.output_attno));

				column.typid = attribute->atttypid;
				column.value_bytes = get_typlen(column.typid);
			}

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
			dcontext->template_columns[current_compressed++] = column;
		}
		else
		{
			Assert(current_not_compressed < num_total);
			dcontext->template_columns[current_not_compressed++] = column;
		}
	}

	Assert(current_compressed == num_compressed);
	Assert(current_not_compressed == num_total);

	qsort(dcontext->template_columns,
		  num_compressed,
		  sizeof(*dcontext->template_columns),
		  compressed_column_cmp);
	/*
	 * Choose which batch queue we are going to use: heap for batch sorted
	 * merge, and one-element FIFO for normal decompression.
	 */
	if (dcontext->batch_sorted_merge)
	{
		chunk_state->batch_queue =
			batch_queue_heap_create(num_compressed,
									chunk_state->sortinfo,
									dcontext->decompressed_slot->tts_tupleDescriptor,
									&BatchQueueFunctionsHeap);
		chunk_state->exec_methods.ExecCustomScan = decompress_chunk_exec_heap;
	}
	else
	{
		chunk_state->batch_queue =
			batch_queue_fifo_create(num_compressed, &BatchQueueFunctionsFifo);
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
 * Perform a vectorized aggregation on int4 values
 */
static TupleTableSlot *
perform_vectorized_sum_int4(DecompressChunkState *chunk_state, Aggref *aggref)
{
	DecompressContext *dcontext = &chunk_state->decompress_context;
	BatchQueue *batch_queue = chunk_state->batch_queue;

	Assert(chunk_state != NULL);
	Assert(aggref != NULL);

	/* Partial result is a int8 */
	Assert(aggref->aggtranstype == INT8OID);

	/* Two columns are decompressed, the column that needs to be aggregated and the count column */
	Assert(dcontext->num_total_columns == 2);

	CompressionColumnDescription *value_column_description = &dcontext->template_columns[0];
	CompressionColumnDescription *count_column_description = &dcontext->template_columns[1];
	if (count_column_description->type != COUNT_COLUMN)
	{
		/*
		 * The count and value columns can go in different order based on their
		 * order in compressed chunk, so check which one we are seeing.
		 */
		CompressionColumnDescription *tmp = value_column_description;
		value_column_description = count_column_description;
		count_column_description = tmp;
	}
	Assert(value_column_description->type == COMPRESSED_COLUMN ||
		   value_column_description->type == SEGMENTBY_COLUMN);
	Assert(count_column_description->type == COUNT_COLUMN);

	/* Get a free batch slot */
	const int new_batch_index = batch_array_get_unused_slot(&batch_queue->batch_array);

	/* Nobody else should use batch states */
	Assert(new_batch_index == 0);
	DecompressBatchState *batch_state =
		batch_array_get_at(&batch_queue->batch_array, new_batch_index);

	/* Init per batch memory context */
	Assert(batch_state != NULL);
	Assert(batch_state->per_batch_context == NULL);
	batch_state->per_batch_context = create_per_batch_mctx(dcontext);
	Assert(batch_state->per_batch_context != NULL);

	/* Init bulk decompression memory context */
	Assert(dcontext->bulk_decompression_context == NULL);
	dcontext->bulk_decompression_context = create_bulk_decompression_mctx(CurrentMemoryContext);
	Assert(dcontext->bulk_decompression_context != NULL);

	/* Get a reference the the output TupleTableSlot */
	TupleTableSlot *decompressed_scan_slot = chunk_state->csstate.ss.ss_ScanTupleSlot;
	Assert(decompressed_scan_slot->tts_tupleDescriptor->natts == 1);

	/* Set all attributes of the result tuple to NULL. So, we return NULL if no data is processed
	 * by our implementation. In addition, the call marks the slot as being used (i.e., no
	 * ExecStoreVirtualTuple call is required). */
	ExecStoreAllNullTuple(decompressed_scan_slot);
	Assert(!TupIsNull(decompressed_scan_slot));

	int64 result_sum = 0;

	if (value_column_description->type == SEGMENTBY_COLUMN)
	{
		/*
		 * To calculate the sum for a segment by value, we need to multiply the value of the segment
		 * by column with the number of compressed tuples in this batch.
		 */
		while (true)
		{
			TupleTableSlot *compressed_slot =
				ExecProcNode(linitial(chunk_state->csstate.custom_ps));

			if (TupIsNull(compressed_slot))
			{
				/* All segment by values are processed. */
				break;
			}

			MemoryContext old_mctx = MemoryContextSwitchTo(batch_state->per_batch_context);
			MemoryContextReset(batch_state->per_batch_context);

			bool isnull_value, isnull_elements;
			Datum value = slot_getattr(compressed_slot,
									   value_column_description->compressed_scan_attno,
									   &isnull_value);

			/* We have multiple compressed tuples for this segment by value. Get number of
			 * compressed tuples */
			Datum elements = slot_getattr(compressed_slot,
										  count_column_description->compressed_scan_attno,
										  &isnull_elements);

			if (!isnull_value && !isnull_elements)
			{
				int32 intvalue = DatumGetInt32(value);
				int32 amount = DatumGetInt32(elements);
				int64 batch_sum = 0;

				Assert(amount > 0);

				/* We have at least one value */
				decompressed_scan_slot->tts_isnull[0] = false;

				/* Multiply the number of tuples with the actual value */
				if (unlikely(pg_mul_s64_overflow(intvalue, amount, &batch_sum)))
				{
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("bigint out of range")));
				}

				/* Add the value to our sum */
				if (unlikely(pg_add_s64_overflow(result_sum, batch_sum, ((int64 *) &result_sum))))
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("bigint out of range")));
			}
			MemoryContextSwitchTo(old_mctx);
		}
	}
	else if (value_column_description->type == COMPRESSED_COLUMN)
	{
		Assert(dcontext->enable_bulk_decompression);
		Assert(value_column_description->bulk_decompression_supported);
		Assert(list_length(aggref->args) == 1);

		while (true)
		{
			TupleTableSlot *compressed_slot =
				ExecProcNode(linitial(chunk_state->csstate.custom_ps));
			if (TupIsNull(compressed_slot))
			{
				/* All compressed batches are processed. */
				break;
			}

			MemoryContext old_mctx = MemoryContextSwitchTo(batch_state->per_batch_context);
			MemoryContextReset(batch_state->per_batch_context);

			/* Decompress data */
			bool isnull;
			Datum value = slot_getattr(compressed_slot,
									   value_column_description->compressed_scan_attno,
									   &isnull);

			Ensure(isnull == false, "got unexpected NULL attribute value from compressed batch");

			/* We have at least one value */
			decompressed_scan_slot->tts_isnull[0] = false;

			CompressedDataHeader *header =
				(CompressedDataHeader *) detoaster_detoast_attr((struct varlena *) DatumGetPointer(
																	value),
																&dcontext->detoaster,
																CurrentMemoryContext);

			ArrowArray *arrow = NULL;

			DecompressAllFunction decompress_all =
				tsl_get_decompress_all_function(header->compression_algorithm,
												value_column_description->typid);
			Assert(decompress_all != NULL);

			MemoryContextSwitchTo(dcontext->bulk_decompression_context);

			arrow = decompress_all(PointerGetDatum(header),
								   value_column_description->typid,
								   batch_state->per_batch_context);

			Assert(arrow != NULL);

			MemoryContextReset(dcontext->bulk_decompression_context);
			MemoryContextSwitchTo(batch_state->per_batch_context);

			/*
			 * We accumulate the sum as int64, so we can sum INT_MAX = 2^31 - 1
			 * at least 2^31 times without incurrint an overflow of the int64
			 * accumulator. The same is true for negative numbers. The
			 * compressed batch size is currently capped at 1000 rows, but even
			 * if it's changed in the future, it's unlikely that we support
			 * batches larger than 65536 rows, not to mention 2^31. Therefore,
			 * we don't need to check for overflows within the loop, which would
			 * slow down the calculation.
			 */
			Assert(arrow->length <= INT_MAX);

			int64 batch_sum = 0;
			for (int i = 0; i < arrow->length; i++)
			{
				const bool arrow_isnull = !arrow_row_is_valid(arrow->buffers[0], i);

				if (likely(!arrow_isnull))
				{
					const int32 arrow_value = ((int32 *) arrow->buffers[1])[i];
					batch_sum += arrow_value;
				}
			}

			if (unlikely(pg_add_s64_overflow(result_sum, batch_sum, ((int64 *) &result_sum))))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("bigint out of range")));
			MemoryContextSwitchTo(old_mctx);
		}
	}
	else
	{
		elog(ERROR, "unsupported column type");
	}

	/* Use Int64GetDatum to store the result since a 64-bit value is not pass-by-value on 32-bit
	 * systems */
	decompressed_scan_slot->tts_values[0] = Int64GetDatum(result_sum);

	return decompressed_scan_slot;
}

/*
 * Directly execute an aggregation function on decompressed data and emit a partial aggregate
 * result.
 *
 * Executing the aggregation directly in this node makes it possible to use the columnar data
 * directly before it is converted into row-based tuples.
 */
static TupleTableSlot *
perform_vectorized_aggregation(DecompressChunkState *chunk_state)
{
	BatchQueue *bq = chunk_state->batch_queue;

	Assert(list_length(chunk_state->custom_scan_tlist) == 1);

	/* Checked by planner */
	Assert(ts_guc_enable_vectorized_aggregation);
	Assert(ts_guc_enable_bulk_decompression);

	/* When using vectorized aggregates, only one result tuple is produced. So, if we have
	 * already initialized a batch state, the aggregation was already performed.
	 */
	if (batch_array_has_active_batches(&bq->batch_array))
	{
		ExecClearTuple(chunk_state->csstate.ss.ss_ScanTupleSlot);
		return chunk_state->csstate.ss.ss_ScanTupleSlot;
	}

	/* Determine which kind of vectorized aggregation we should perform */
	TargetEntry *tlentry = (TargetEntry *) linitial(chunk_state->custom_scan_tlist);
	Assert(IsA(tlentry->expr, Aggref));
	Aggref *aggref = castNode(Aggref, tlentry->expr);

	/* The aggregate should be a partial aggregate */
	Assert(aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL);

	switch (aggref->aggfnoid)
	{
		case F_SUM_INT4:
			return perform_vectorized_sum_int4(chunk_state, aggref);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("vectorized aggregation for function %d is not supported",
							aggref->aggfnoid)));
			pg_unreachable();
	}
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

	if (chunk_state->perform_vectorized_aggregation)
	{
		return perform_vectorized_aggregation(chunk_state);
	}

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

		if (chunk_state->perform_vectorized_aggregation)
		{
			ExplainPropertyBool("Vectorized Aggregation",
								chunk_state->perform_vectorized_aggregation,
								es);
		}
	}
}
