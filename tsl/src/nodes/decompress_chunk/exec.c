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
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "ts_catalog/hypertable_compression.h"

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnState
{
	DecompressChunkColumnType type;
	Oid typid;

	/*
	 * Attno of the decompressed column in the output of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the uncompressed chunk, but are still used for decompression. They should
	 * have the respective `type` field.
	 */
	AttrNumber output_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	union
	{
		struct
		{
			Datum value;
			bool isnull;
			int count;
		} segmentby;
		struct
		{
			DecompressionIterator *iterator;
		} compressed;

		ArrowArray data;
	};
} DecompressChunkColumnState;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	int num_columns;
	DecompressChunkColumnState *columns;

	bool initialized;
	bool reverse;
	int hypertable_id;
	Oid chunk_relid;
	List *hypertable_compression_info;
	int total_rows;
	int current_row;
	MemoryContext per_batch_context;
} DecompressChunkState;

static TupleTableSlot *decompress_chunk_exec(CustomScanState *node);
static void decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_end(CustomScanState *node);
static void decompress_chunk_rescan(CustomScanState *node);
static TupleTableSlot *decompress_chunk_create_tuple(DecompressChunkState *state);

static CustomExecMethods decompress_chunk_state_methods = {
	.BeginCustomScan = decompress_chunk_begin,
	.ExecCustomScan = decompress_chunk_exec,
	.EndCustomScan = decompress_chunk_end,
	.ReScanCustomScan = decompress_chunk_rescan,
};

Node *
decompress_chunk_state_create(CustomScan *cscan)
{
	DecompressChunkState *state;
	List *settings;

	state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	state->csstate.methods = &decompress_chunk_state_methods;

	settings = linitial(cscan->custom_private);
	state->hypertable_id = linitial_int(settings);
	state->chunk_relid = lsecond_int(settings);
	state->reverse = lthird_int(settings);
	state->decompression_map = lsecond(cscan->custom_private);

	return (Node *) state;
}

/*
 * initialize column state
 *
 * the column state indexes are based on the index
 * of the columns of the uncompressed chunk because
 * that is the tuple layout we are creating
 */
static void
initialize_column_state(DecompressChunkState *state)
{
	ScanState *ss = (ScanState *) state;
	TupleDesc desc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	ListCell *lc;

	if (list_length(state->decompression_map) == 0)
	{
		elog(ERROR, "no columns specified to decompress");
	}

	state->columns =
		palloc0(list_length(state->decompression_map) * sizeof(DecompressChunkColumnState));

	AttrNumber next_compressed_scan_attno = 0;
	state->num_columns = 0;
	foreach (lc, state->decompression_map)
	{
		next_compressed_scan_attno++;

		AttrNumber output_attno = lfirst_int(lc);
		if (output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		DecompressChunkColumnState *column = &state->columns[state->num_columns];
		state->num_columns++;

		column->output_attno = output_attno;
		column->compressed_scan_attno = next_compressed_scan_attno;

		if (output_attno > 0)
		{
			/* normal column that is also present in uncompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(output_attno));
			FormData_hypertable_compression *ht_info =
				get_column_compressioninfo(state->hypertable_compression_info,
										   NameStr(attribute->attname));

			column->typid = attribute->atttypid;

			if (ht_info->segmentby_column_index > 0)
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

	state->hypertable_compression_info = ts_hypertable_compression_get(state->hypertable_id);

	initialize_column_state(state);

	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));

	state->per_batch_context = AllocSetContextCreate(CurrentMemoryContext,
													 "DecompressChunk per_batch",
													 ALLOCSET_DEFAULT_SIZES);
}

static bool
arrow_validity_bitmap_get(const uint64 *bitmap, int row_number)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return (bitmap[qword_index] & mask) ? 1 : 0;
}

static void
arrow_validity_bitmap_set(uint64 *bitmap, int row_number, bool value)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;

	if (value)
	{
		bitmap[qword_index] |= mask;
	}
	else
	{
		bitmap[qword_index] &= ~mask;
	}

	Assert(arrow_validity_bitmap_get(bitmap, row_number) == value);
}

static void
initialize_batch(DecompressChunkState *state, TupleTableSlot *compressed_slot,
				 TupleTableSlot *decompressed_slot)
{
	Datum value;
	bool isnull;
	int i;
	MemoryContext old_context = MemoryContextSwitchTo(state->per_batch_context);
	MemoryContextReset(state->per_batch_context);

	state->total_rows = 0;
	state->current_row = 0;

	for (i = 0; i < state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				value = slot_getattr(compressed_slot, column->compressed_scan_attno, &isnull);
				if (!isnull)
				{
					CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

					column->compressed.iterator =
						tsl_get_decompression_iterator_init(header->compression_algorithm,
															state->reverse)(PointerGetDatum(header),
																			column->typid);
					int current_row = 0;
					int nulls1 = 0;
					uint64 *restrict validity_bitmap =
						palloc0(sizeof(uint64) * ((GLOBAL_MAX_ROWS_PER_COMPRESSION + 64 - 1) / 64));
					uint64 *restrict values =
						palloc(sizeof(uint64) * GLOBAL_MAX_ROWS_PER_COMPRESSION);
					for (DecompressResult res =
							 column->compressed.iterator->try_next(column->compressed.iterator);
						 !res.is_done;
						 res = column->compressed.iterator->try_next(column->compressed.iterator))
					{
						arrow_validity_bitmap_set(validity_bitmap, current_row, !res.is_null);
						values[current_row] = res.val;
						current_row++;

						if (res.is_null)
						{
							nulls1++;
						}
					}

					int nulls2 = 0;
					for (int i = 0; i < current_row; i++)
					{
						if (!arrow_validity_bitmap_get(validity_bitmap, i))
						{
							nulls2++;
						}
					}

					Assert(nulls1 == nulls2);

					column->data.n_buffers = 2;
					column->data.buffers = palloc(sizeof(void *) * 2);
					column->data.buffers[0] = validity_bitmap;
					column->data.buffers[1] = values;
					column->data.length = current_row;
					column->data.null_count = -1;

					if (state->total_rows == 0)
					{
						state->total_rows = current_row;
					}
					else if (state->total_rows != current_row)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}
				}
				else
				{
					column->compressed.iterator = NULL;
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
					decompressed_slot->tts_values[attr] =
						getmissingattr(decompressed_slot->tts_tupleDescriptor,
									   attr + 1,
									   &decompressed_slot->tts_isnull[attr]);
				}

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
				decompressed_slot->tts_values[attr] =
					slot_getattr(compressed_slot,
								 column->compressed_scan_attno,
								 &decompressed_slot->tts_isnull[attr]);
				break;
			}
			case COUNT_COLUMN:
				value = slot_getattr(compressed_slot, column->compressed_scan_attno, &isnull);
				int count_value = DatumGetInt32(value);
				if (state->total_rows == 0)
				{
					state->total_rows = count_value;
				}
				else if (state->total_rows != count_value)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}
				/* count column should never be NULL */
				Assert(!isnull);
				break;
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}
	state->initialized = true;
	MemoryContextSwitchTo(old_context);
}

static TupleTableSlot *
decompress_chunk_exec(CustomScanState *node)
{
	DecompressChunkState *state = (DecompressChunkState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	if (node->custom_ps == NIL)
		return NULL;

	while (true)
	{
		TupleTableSlot *slot = decompress_chunk_create_tuple(state);

		if (TupIsNull(slot))
			return NULL;

		econtext->ecxt_scantuple = slot;

		if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext))
		{
			InstrCountFiltered1(node, 1);
			ExecClearTuple(slot);
			continue;
		}

		if (!node->ss.ps.ps_ProjInfo)
			return slot;

		return ExecProject(node->ss.ps.ps_ProjInfo);
	}
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	((DecompressChunkState *) node)->initialized = false;
	ExecReScan(linitial(node->custom_ps));
}

static void
decompress_chunk_end(CustomScanState *node)
{
	MemoryContextReset(((DecompressChunkState *) node)->per_batch_context);
	ExecEndNode(linitial(node->custom_ps));
}

/*
 * Create generated tuple according to column state
 */
static TupleTableSlot *
decompress_chunk_create_tuple(DecompressChunkState *state)
{
	TupleTableSlot *slot = state->csstate.ss.ss_ScanTupleSlot;

	while (true)
	{
		if (state->initialized && state->current_row >= state->total_rows)
		{
			/*
			 * Reached end of batch.
			 */
			state->initialized = false;
		}

		if (!state->initialized)
		{
			ExecClearTuple(slot);

			/*
			 * Reset expression memory context to clean out any cruft from
			 * previous batch. Our batches are 1000 rows max, and this memory
			 * context is used by ExecProject and ExecQual, which shouldn't
			 * leak too much. So we only do this per batch and not per tuple to
			 * save some CPU.
			 */
			ExprContext *econtext = state->csstate.ss.ps.ps_ExprContext;
			ResetExprContext(econtext);

			TupleTableSlot *subslot = ExecProcNode(linitial(state->csstate.custom_ps));

			if (TupIsNull(subslot))
				return NULL;

			initialize_batch(state, subslot, slot);
		}

		Assert(state->initialized);
		Assert(state->total_rows > 0);
		Assert(state->current_row < state->total_rows);

		for (int i = 0; i < state->num_columns; i++)
		{
			DecompressChunkColumnState *column = &state->columns[i];
			if (column->type == COMPRESSED_COLUMN && column->compressed.iterator)
			{
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
				/* FIXME add code for passing float8 by reference on 32-bit systems. */
				slot->tts_values[attr] = ((Datum *) column->data.buffers[1])[state->current_row];
				slot->tts_isnull[attr] =
					!arrow_validity_bitmap_get(column->data.buffers[0], state->current_row);
			}
		}

		/*
		 * It's a virtual tuple slot, so no point in clearing/storing it
		 * per each row, we can just update the values in-place. This saves
		 * some CPU. We have to store it after ExecQual fails or after a new
		 * batch.
		 */
		if (TTS_EMPTY(slot))
		{
			ExecStoreVirtualTuple(slot);
		}

		state->current_row++;
		return slot;
	}
}
