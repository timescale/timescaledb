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
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "guc.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/decompress_chunk/sorted_merge.h"
#include "ts_catalog/hypertable_compression.h"

static TupleTableSlot *decompress_chunk_exec(CustomScanState *node);
static void decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_end(CustomScanState *node);
static void decompress_chunk_rescan(CustomScanState *node);
static void decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static void decompress_chunk_create_tuple(DecompressChunkState *chunk_state,
										  DecompressBatchState *batch_state);

static void decompress_initialize_batch_state(DecompressChunkState *chunk_state,
											  DecompressBatchState *batch_state);

static CustomExecMethods decompress_chunk_state_methods = {
	.BeginCustomScan = decompress_chunk_begin,
	.ExecCustomScan = decompress_chunk_exec,
	.EndCustomScan = decompress_chunk_end,
	.ReScanCustomScan = decompress_chunk_rescan,
	.ExplainCustomScan = decompress_chunk_explain,
};

/*
 * Build the sortkeys data structure from the list structure in the
 * custom_private field of the custom scan. This sort info is used to sort
 * binary heap used for sorted merge append.
 */
static void
build_batch_sorted_merge_info(DecompressChunkState *chunk_state, List *sortinfo)
{
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
	List *settings;

	chunk_state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	chunk_state->csstate.methods = &decompress_chunk_state_methods;

	settings = linitial(cscan->custom_private);

	Assert(list_length(settings) == 4);

	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->reverse = lthird_int(settings);
	chunk_state->sorted_merge_append = lfourth_int(settings);
	chunk_state->decompression_map = lsecond(cscan->custom_private);
	chunk_state->is_segmentby_column = lthird(cscan->custom_private);

	/* Extract sort info */
	List *sortinfo = lfourth(cscan->custom_private);
	build_batch_sorted_merge_info(chunk_state, sortinfo);

	/* Sort keys should only be present when sorted_merge_append is used */
	Assert(chunk_state->sorted_merge_append == true || chunk_state->n_sortkeys == 0);
	Assert(chunk_state->n_sortkeys == 0 || chunk_state->sortkeys != NULL);

	return (Node *) chunk_state;
}

/*
 * Create states to hold information for up to n batches
 */
static void
batch_states_create(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches >= 0);

	chunk_state->n_batch_states = nbatches;
	chunk_state->batch_states = palloc0(sizeof(DecompressBatchState) * nbatches);

	for (int segment = 0; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		decompress_initialize_batch_state(chunk_state, batch_state);
	}

	chunk_state->unused_batch_states = bms_add_range(NULL, 0, nbatches - 1);

	Assert(bms_num_members(chunk_state->unused_batch_states) == chunk_state->n_batch_states);
}

/*
 * Enhance the capacity of existing batch states
 */
static void
batch_states_enlarge(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches > chunk_state->n_batch_states);

	/* Request additional memory */
	chunk_state->batch_states =
		(DecompressBatchState *) repalloc(chunk_state->batch_states,
										  sizeof(DecompressBatchState) * nbatches);

	/* Init new batch states (lazy initialization, expensive data structures
	 * like TupleTableSlot are created on demand) */
	for (int segment = chunk_state->n_batch_states; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		decompress_initialize_batch_state(chunk_state, batch_state);
	}

	/* Register the new states as unused */
	chunk_state->unused_batch_states =
		bms_add_range(chunk_state->unused_batch_states, chunk_state->n_batch_states, nbatches - 1);

	Assert(bms_num_members(chunk_state->unused_batch_states) ==
		   nbatches - chunk_state->n_batch_states);

	/* Update number of available batch states */
	chunk_state->n_batch_states = nbatches;
}

/*
 * Mark a DecompressBatchState as unused
 */
void
decompress_set_batch_state_to_unused(DecompressChunkState *chunk_state, int batch_id)
{
	Assert(batch_id >= 0);
	Assert(batch_id < chunk_state->n_batch_states);

	DecompressBatchState *batch_state = &chunk_state->batch_states[batch_id];

	/* Reset batch state */
	batch_state->initialized = false;
	batch_state->total_batch_rows = 0;
	batch_state->current_batch_row = 0;

	if (batch_state->compressed_slot != NULL)
		ExecClearTuple(batch_state->compressed_slot);

	if (batch_state->decompressed_slot_projected != NULL)
		ExecClearTuple(batch_state->decompressed_slot_projected);

	if (batch_state->decompressed_slot_scan != NULL)
		ExecClearTuple(batch_state->decompressed_slot_scan);

	MemoryContextReset(batch_state->per_batch_context);

	chunk_state->unused_batch_states = bms_add_member(chunk_state->unused_batch_states, batch_id);
}

/*
 * Get the next free and unused batch state and mark as used
 */
DecompressSlotNumber
decompress_get_free_batch_state_id(DecompressChunkState *chunk_state)
{
	if (bms_is_empty(chunk_state->unused_batch_states))
		batch_states_enlarge(chunk_state, chunk_state->n_batch_states * 2);

	Assert(!bms_is_empty(chunk_state->unused_batch_states));

	DecompressSlotNumber next_free_batch = bms_next_member(chunk_state->unused_batch_states, -1);

	Assert(next_free_batch >= 0);
	Assert(next_free_batch < chunk_state->n_batch_states);
	Assert(chunk_state->batch_states[next_free_batch].initialized == false);

	bms_del_member(chunk_state->unused_batch_states, next_free_batch);

	return next_free_batch;
}

/*
 * initialize column chunk_state
 *
 * the column chunk_state indexes are based on the index
 * of the columns of the decompressed chunk because
 * that is the tuple layout we are creating
 */
static void
decompress_initialize_batch_state(DecompressChunkState *chunk_state,
								  DecompressBatchState *batch_state)
{
	ScanState *ss = (ScanState *) chunk_state;
	TupleDesc desc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;

	if (list_length(chunk_state->decompression_map) == 0)
	{
		elog(ERROR, "no columns specified to decompress");
	}

	batch_state->per_batch_context = AllocSetContextCreate(CurrentMemoryContext,
														   "DecompressChunk per_batch",
														   ALLOCSET_DEFAULT_SIZES);

	batch_state->columns =
		palloc0(list_length(chunk_state->decompression_map) * sizeof(DecompressChunkColumnState));

	batch_state->initialized = false;

	/* The slots will be created on first usage of the batch state */
	batch_state->decompressed_slot_projected = NULL;
	batch_state->decompressed_slot_scan = NULL;
	batch_state->compressed_slot = NULL;

	AttrNumber next_compressed_scan_attno = 0;
	chunk_state->num_columns = 0;

	ListCell *dest_cell;
	ListCell *is_segmentby_cell;
	Assert(list_length(chunk_state->decompression_map) ==
		   list_length(chunk_state->is_segmentby_column));
	forboth (dest_cell,
			 chunk_state->decompression_map,
			 is_segmentby_cell,
			 chunk_state->is_segmentby_column)
	{
		next_compressed_scan_attno++;

		AttrNumber output_attno = lfirst_int(dest_cell);
		if (output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		DecompressChunkColumnState *column = &batch_state->columns[chunk_state->num_columns];
		chunk_state->num_columns++;

		column->output_attno = output_attno;
		column->compressed_scan_attno = next_compressed_scan_attno;

		if (output_attno > 0)
		{
			/* normal column that is also present in decompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(output_attno));

			column->typid = attribute->atttypid;

			if (lfirst_int(is_segmentby_cell))
				column->type = SEGMENTBY_COLUMN;
			else
				column->type = COMPRESSED_COLUMN;
		}
		else
		{
			/* metadata columns */
			switch (column->output_attno)
			{
				case DECOMPRESS_CHUNK_COUNT_ID:
					column->type = COUNT_COLUMN;
					break;
				case DECOMPRESS_CHUNK_SEQUENCE_NUM_ID:
					column->type = SEQUENCE_NUM_COLUMN;
					break;
				default:
					elog(ERROR, "Invalid column attno \"%d\"", column->output_attno);
					break;
			}
		}
	}
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

/*
 * Complete initialization of the supplied CustomScanState.
 *
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags)
{
	DecompressChunkState *state = (DecompressChunkState *) node;
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
		List *modified_tlist = constify_tableoid(tlist, cscan->scan.scanrelid, state->chunk_relid);

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

	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));
}

void
decompress_initialize_batch(DecompressChunkState *chunk_state, DecompressBatchState *batch_state,
							TupleTableSlot *subslot)
{
	Datum value;
	bool isnull;
	int i;

	Assert(batch_state->initialized == false);

	/* Batch states can be re-used skip tuple slot creation in that case */
	if (batch_state->compressed_slot == NULL)
	{
		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->compressed_slot_tdesc == NULL)
			chunk_state->compressed_slot_tdesc =
				CreateTupleDescCopyConstr(subslot->tts_tupleDescriptor);
		Assert(chunk_state->compressed_slot_tdesc->tdrefcount == -1);

		batch_state->compressed_slot =
			MakeSingleTupleTableSlot(chunk_state->compressed_slot_tdesc, subslot->tts_ops);
	}
	else
	{
		ExecClearTuple(batch_state->compressed_slot);
	}

	ExecCopySlot(batch_state->compressed_slot, subslot);
	Assert(!TupIsNull(batch_state->compressed_slot));

	/* DecompressBatchState can be re-used. The expensive TupleTableSlot are created on demand as
	 * soon as this state is used for the first time.
	 */
	if (batch_state->decompressed_slot_scan == NULL)
	{
		/* Get a reference the the output TupleTableSlot */
		TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;

		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->decompressed_slot_scan_tdesc == NULL)
			chunk_state->decompressed_slot_scan_tdesc =
				CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
		Assert(chunk_state->decompressed_slot_scan_tdesc->tdrefcount == -1);

		batch_state->decompressed_slot_scan =
			MakeSingleTupleTableSlot(chunk_state->decompressed_slot_scan_tdesc, slot->tts_ops);
	}
	else
	{
		ExecClearTuple(batch_state->decompressed_slot_scan);
	}

	/* Ensure that all fields are empty. Calling ExecClearTuple is not enough
	 * because some attributes might not be populated (e.g., due to a dropped
	 * column) and these attributes need to be set to null. */
	ExecStoreAllNullTuple(batch_state->decompressed_slot_scan);

	if (batch_state->decompressed_slot_projected == NULL)
	{
		if (chunk_state->csstate.ss.ps.ps_ProjInfo != NULL)
		{
			TupleTableSlot *slot = chunk_state->csstate.ss.ps.ps_ProjInfo->pi_state.resultslot;

			/* Create a non ref-counted copy of the tuple descriptor */
			if (chunk_state->decompressed_slot_projected_tdesc == NULL)
				chunk_state->decompressed_slot_projected_tdesc =
					CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
			Assert(chunk_state->decompressed_slot_projected_tdesc->tdrefcount == -1);

			batch_state->decompressed_slot_projected =
				MakeSingleTupleTableSlot(chunk_state->decompressed_slot_projected_tdesc,
										 slot->tts_ops);
		}
		else
		{
			/* If we don't have any projection info, set decompressed_slot_scan to
			 * decompressed_slot_projected. So, we don't need to copy the content after the
			 * scan to the output slot in decompress_chunk_perform_select_project() */
			batch_state->decompressed_slot_projected = batch_state->decompressed_slot_scan;
		}
	}
	else
	{
		ExecClearTuple(batch_state->decompressed_slot_projected);
	}

	Assert(!TTS_EMPTY(batch_state->compressed_slot));

	batch_state->total_batch_rows = 0;
	batch_state->current_batch_row = 0;

	MemoryContext old_context = MemoryContextSwitchTo(batch_state->per_batch_context);
	MemoryContextReset(batch_state->per_batch_context);

	for (i = 0; i < chunk_state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &batch_state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				column->compressed.iterator = NULL;
				column->compressed.arrow = NULL;
				column->compressed.value_bytes = -1;
				value = slot_getattr(batch_state->compressed_slot,
									 column->compressed_scan_attno,
									 &isnull);
				if (isnull)
				{
					/*
					 * The column will have a default value for the entire batch,
					 * set it now.
					 */
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

					batch_state->decompressed_slot_scan->tts_values[attr] =
						getmissingattr(batch_state->decompressed_slot_scan->tts_tupleDescriptor,
									   attr + 1,
									   &batch_state->decompressed_slot_scan->tts_isnull[attr]);
					break;
				}

				/* Decompress the entire batch if it is supported. */
				CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

				/*
				 * For now we disable bulk decompression for batch sorted
				 * merge plans. They involve keeping many open batches at
				 * the same time, so the memory usage might increase greatly.
				 */
				ArrowArray *arrow = NULL;
				if (!chunk_state->sorted_merge_append && ts_guc_enable_bulk_decompression)
				{
					if (chunk_state->bulk_decompression_context == NULL)
					{
						chunk_state->bulk_decompression_context =
							AllocSetContextCreate(MemoryContextGetParent(
													  batch_state->per_batch_context),
												  "bulk decompression",
												  /* minContextSize = */ 0,
												  /* initBlockSize = */ 64 * 1024,
												  /* maxBlockSize = */ 64 * 1024);
					}

					DecompressAllFunction decompress_all =
						tsl_get_decompress_all_function(header->compression_algorithm);
					if (decompress_all)
					{
						MemoryContext context_before_decompression =
							MemoryContextSwitchTo(chunk_state->bulk_decompression_context);

						arrow = decompress_all(PointerGetDatum(header),
											   column->typid,
											   batch_state->per_batch_context);

						MemoryContextReset(chunk_state->bulk_decompression_context);

						MemoryContextSwitchTo(context_before_decompression);
					}
				}

				if (arrow)
				{
					if (batch_state->total_batch_rows == 0)
					{
						batch_state->total_batch_rows = arrow->length;
					}
					else if (batch_state->total_batch_rows != arrow->length)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}

					column->compressed.arrow = arrow;

					/*
					 * Note the fact that we are using bulk decompression, for
					 * EXPLAIN ANALYZE.
					 */
					chunk_state->using_bulk_decompression = true;

					column->compressed.value_bytes = get_typlen(column->typid);

					break;
				}

				/* As a fallback, decompress row-by-row. */
				column->compressed.iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														chunk_state->reverse)(PointerGetDatum(
																				  header),
																			  column->typid);

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				/*
				 * A segmentby column is not going to change during one batch,
				 * and our output tuples are read-only, so it's enough to only
				 * save it once per batch, which we do here.
				 */
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
				batch_state->decompressed_slot_scan->tts_values[attr] =
					slot_getattr(batch_state->compressed_slot,
								 column->compressed_scan_attno,
								 &batch_state->decompressed_slot_scan->tts_isnull[attr]);
				break;
			}
			case COUNT_COLUMN:
			{
				value = slot_getattr(batch_state->compressed_slot,
									 column->compressed_scan_attno,
									 &isnull);
				/* count column should never be NULL */
				Assert(!isnull);
				int count_value = DatumGetInt32(value);
				if (count_value <= 0)
				{
					ereport(ERROR,
							(errmsg("the compressed data is corrupt: got a segment with length %d",
									count_value)));
				}

				if (batch_state->total_batch_rows == 0)
				{
					batch_state->total_batch_rows = count_value;
				}
				else if (batch_state->total_batch_rows != count_value)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

				break;
			}
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}
	batch_state->initialized = true;
	MemoryContextSwitchTo(old_context);
}

/* Perform the projection and selection of the decompressed tuple */
static bool pg_nodiscard
decompress_chunk_perform_select_project(CustomScanState *node,
										TupleTableSlot *decompressed_slot_scan,
										TupleTableSlot *decompressed_slot_projected)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	/*
	 * Reset expression memory context to clean out any cruft from
	 * previous batch. Our batches are 1000 rows max, and this memory
	 * context is used by ExecProject and ExecQual, which shouldn't
	 * leak too much. So we only do this per batch and not per tuple to
	 * save some CPU.
	 */
	econtext->ecxt_scantuple = decompressed_slot_scan;
	ResetExprContext(econtext);

	if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext))
	{
		InstrCountFiltered1(node, 1);
		return false;
	}

	if (node->ss.ps.ps_ProjInfo)
	{
		TupleTableSlot *projected = ExecProject(node->ss.ps.ps_ProjInfo);
		ExecCopySlot(decompressed_slot_projected, projected);
	}

	return true;
}

static TupleTableSlot *
decompress_chunk_exec(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (node->custom_ps == NIL)
		return NULL;

	/* If the sorted_merge_append flag is set, the compression order_by and the
	 * query order_by do match. Therefore, we use a binary heap to decompress the compressed
	 * segments and merge the tuples.
	 */
	if (chunk_state->sorted_merge_append)
	{
		/* Create the heap on the first call. */
		if (chunk_state->merge_heap == NULL)
		{
			batch_states_create(chunk_state, INITIAL_BATCH_CAPACITY);
			decompress_sorted_merge_init(chunk_state);
		}
		else
		{
			/* Remove the tuple returned in the last iteration and refresh the heap.
			 * This operation is delayed up to this point where the next tuple actually
			 * needs to be decompressed.
			 */
			decompress_sorted_merge_remove_top_tuple_and_decompress_next(chunk_state);
		}

		return decompress_sorted_merge_get_next_tuple(chunk_state);
	}
	else
	{
		if (chunk_state->batch_states == NULL)
			batch_states_create(chunk_state, 1);

		DecompressBatchState *batch_state = &chunk_state->batch_states[0];
		decompress_chunk_create_tuple(chunk_state, batch_state);

		return batch_state->decompressed_slot_projected;
	}
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (chunk_state->merge_heap != NULL)
	{
		decompress_sorted_merge_free(chunk_state);
		Assert(chunk_state->merge_heap == NULL);
	}

	for (int i = 0; i < chunk_state->n_batch_states; i++)
	{
		decompress_set_batch_state_to_unused(chunk_state, i);
	}

	Assert(bms_num_members(chunk_state->unused_batch_states) == chunk_state->n_batch_states);

	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
decompress_chunk_end(CustomScanState *node)
{
	int i;
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (chunk_state->merge_heap != NULL)
	{
		decompress_sorted_merge_free(chunk_state);
		Assert(chunk_state->merge_heap == NULL);
	}

	for (i = 0; i < chunk_state->n_batch_states; i++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[i];
		Assert(batch_state != NULL);

		if (batch_state->compressed_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->compressed_slot);

		if (batch_state->decompressed_slot_scan != NULL)
			ExecDropSingleTupleTableSlot(batch_state->decompressed_slot_scan);

		/* If we dont have any projection info decompressed_slot_scan and
		 * decompressed_slot_projected can be equal */
		if (batch_state->decompressed_slot_projected != NULL &&
			batch_state->decompressed_slot_scan != batch_state->decompressed_slot_projected)
			ExecDropSingleTupleTableSlot(batch_state->decompressed_slot_projected);

		batch_state = NULL;
	}

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
		if (chunk_state->sorted_merge_append)
		{
			ExplainPropertyBool("Sorted merge append", chunk_state->sorted_merge_append, es);
		}

		if (es->analyze && (es->verbose || es->format != EXPLAIN_FORMAT_TEXT))
		{
			ExplainPropertyBool("Bulk Decompression", chunk_state->using_bulk_decompression, es);
		}
	}
}

/*
 * Decompress the next tuple from the batch indicated by batch state. The result is stored
 * in batch_state->decompressed_slot_projected. The slot will be empty if the batch
 * is entirely processed.
 */
bool
decompress_get_next_tuple_from_batch(DecompressChunkState *chunk_state,
									 DecompressBatchState *batch_state)
{
	bool first_tuple_returned = true;
	TupleTableSlot *decompressed_slot_scan = batch_state->decompressed_slot_scan;
	TupleTableSlot *decompressed_slot_projected = batch_state->decompressed_slot_projected;

	Assert(decompressed_slot_scan != NULL);
	Assert(decompressed_slot_projected != NULL);

	while (true)
	{
		if (batch_state->current_batch_row >= batch_state->total_batch_rows)
		{
			/*
			 * Reached end of batch. Check that the columns that we're decompressing
			 * row-by-row have also ended.
			 */
			batch_state->initialized = false;
			for (int i = 0; i < chunk_state->num_columns; i++)
			{
				DecompressChunkColumnState *column = &batch_state->columns[i];
				if (column->type == COMPRESSED_COLUMN && column->compressed.iterator)
				{
					DecompressResult result =
						column->compressed.iterator->try_next(column->compressed.iterator);
					if (!result.is_done)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}
				}
			}

			/* Clear old slot state */
			ExecClearTuple(decompressed_slot_projected);

			return first_tuple_returned;
		}

		Assert(batch_state->initialized);
		Assert(batch_state->total_batch_rows > 0);
		Assert(batch_state->current_batch_row < batch_state->total_batch_rows);

		const int output_row = batch_state->current_batch_row++;
		const size_t arrow_row = unlikely(chunk_state->reverse) ?
									 batch_state->total_batch_rows - 1 - output_row :
									 output_row;

		for (int i = 0; i < chunk_state->num_columns; i++)
		{
			DecompressChunkColumnState *column = &batch_state->columns[i];

			if (column->type != COMPRESSED_COLUMN)
			{
				continue;
			}

			const AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
			if (column->compressed.iterator != NULL)
			{
				DecompressResult result =
					column->compressed.iterator->try_next(column->compressed.iterator);

				if (result.is_done)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

				decompressed_slot_scan->tts_isnull[attr] = result.is_null;
				decompressed_slot_scan->tts_values[attr] = result.val;
			}
			else if (column->compressed.arrow != NULL)
			{
				const char *src = column->compressed.arrow->buffers[1];
				Assert(column->compressed.value_bytes > 0);

				/*
				 * The conversion of Datum to more narrow types will truncate
				 * the higher bytes, so we don't care if we read some garbage
				 * into them. These are unaligned reads, so technically we have
				 * to do memcpy.
				 */
				uint64 value;
				memcpy(&value, &src[column->compressed.value_bytes * arrow_row], 8);

#ifdef USE_FLOAT8_BYVAL
				Datum datum = Int64GetDatum(value);
#else
				/*
				 * On 32-bit systems, the data larger than 4 bytes go by
				 * reference, so we have to jump through these hoops.
				 */
				Datum datum;
				if (column->compressed.value_bytes <= 4)
				{
					datum = Int32GetDatum((uint32) value);
				}
				else
				{
					datum = Int64GetDatum(value);
				}
#endif
				const AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
				decompressed_slot_scan->tts_values[attr] = datum;
				decompressed_slot_scan->tts_isnull[attr] =
					!arrow_row_is_valid(column->compressed.arrow->buffers[0], arrow_row);
			}
		}

		/*
		 * It's a virtual tuple slot, so no point in clearing/storing it
		 * per each row, we can just update the values in-place. This saves
		 * some CPU. We have to store it after ExecQual returns false (the tuple
		 * didn't pass the filter), or after a new batch. The standard protocol
		 * is to clear and set the tuple slot for each row, but our output tuple
		 * slots are read-only, and the memory is owned by this node, so it is
		 * safe to violate this protocol.
		 */
		Assert(TTS_IS_VIRTUAL(decompressed_slot_scan));
		if (TTS_EMPTY(decompressed_slot_scan))
		{
			ExecStoreVirtualTuple(decompressed_slot_scan);
		}

		/* Perform selection and projection if needed */
		bool is_valid_tuple = decompress_chunk_perform_select_project(&chunk_state->csstate,
																	  decompressed_slot_scan,
																	  decompressed_slot_projected);

		/* Non empty result, return it */
		if (is_valid_tuple)
		{
			Assert(!TTS_EMPTY(decompressed_slot_projected));
			return first_tuple_returned;
		}

		first_tuple_returned = false;

		/* Otherwise fetch the next tuple in the next iteration */
	}
}

/*
 * Create generated tuple according to column chunk_state
 */
static void
decompress_chunk_create_tuple(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	while (true)
	{
		if (!batch_state->initialized)
		{
			TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));

			if (TupIsNull(subslot))
			{
				Assert(TupIsNull(batch_state->decompressed_slot_projected));
				return;
			}

			decompress_initialize_batch(chunk_state, batch_state, subslot);
		}

		/* Decompress next tuple from batch */
		decompress_get_next_tuple_from_batch(chunk_state, batch_state);

		if (!TupIsNull(batch_state->decompressed_slot_projected))
			return;

		batch_state->initialized = false;
	}
}
