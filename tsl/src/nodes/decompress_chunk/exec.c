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
			ArrowArray *data;
		} compressed;
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

static inline void
datum_to_arrow(Datum datum, void *buffer, int row_index, Oid typeoid)
{
	switch (typeoid)
	{
		case BOOLOID:
			((int8 *) buffer)[row_index] = DatumGetBool(datum);
			break;
		case CHAROID:
			((int8 *) buffer)[row_index] = DatumGetChar(datum);
			break;
		case INT2OID:
			((int16 *) buffer)[row_index] = DatumGetInt16(datum);
			break;
		case INT4OID:
			((int32 *) buffer)[row_index] = DatumGetInt32(datum);
			break;
		case INT8OID:
			((int64 *) buffer)[row_index] = DatumGetInt64(datum);
			break;
		case FLOAT4OID:
			((float4 *) buffer)[row_index] = DatumGetFloat4(datum);
			break;
		case FLOAT8OID:
			((float8 *) buffer)[row_index] = DatumGetFloat8(datum);
			break;
		case DATEOID:
			((int32 *) buffer)[row_index] = DatumGetDateADT(datum);
			break;
		case TIMESTAMPTZOID:
			((int64 *) buffer)[row_index] = DatumGetTimestampTz(datum);
			break;
		case TIMESTAMPOID:
			((int64 *) buffer)[row_index] = DatumGetTimestamp(datum);
			break;
		default:
			elog(ERROR, "cannot convert datum with oid %d to arrow type", typeoid);
	}
}

/* For experiments. */
static pg_attribute_always_inline ArrowArray *
default_decompress_all_forward_direction(
	struct DecompressionIterator *iterator,
	DecompressResult (*try_next_impl)(struct DecompressionIterator *))
{
	Assert(iterator->forward);
	ArrowArray *result = palloc0(sizeof(ArrowArray));

	int current_row = 0;
	int typlen = get_typlen(iterator->element_type);
	Assert(typlen > 0);
	uint64 *validity_bitmap =
		palloc0(sizeof(uint64) * ((GLOBAL_MAX_ROWS_PER_COMPRESSION + 64 - 1) / 64));
	void *values = palloc(typlen * GLOBAL_MAX_ROWS_PER_COMPRESSION);
	for (DecompressResult res = try_next_impl(iterator); !res.is_done;
		 res = try_next_impl(iterator))
	{
		arrow_validity_bitmap_set(validity_bitmap, current_row, !res.is_null);
		datum_to_arrow(res.val, values, current_row, iterator->element_type);
		current_row++;
	}

	result->n_buffers = 2;
	result->buffers = palloc(sizeof(void *) * 2);
	result->buffers[0] = validity_bitmap;
	result->buffers[1] = values;
	result->length = current_row;
	result->null_count = -1;

	return result;
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
				column->compressed.data = NULL;
				column->compressed.iterator = NULL;

				value = slot_getattr(compressed_slot, column->compressed_scan_attno, &isnull);

				if (isnull)
				{
					column->compressed.iterator = NULL;
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);
					decompressed_slot->tts_values[attr] =
						getmissingattr(decompressed_slot->tts_tupleDescriptor,
									   attr + 1,
									   &decompressed_slot->tts_isnull[attr]);
					break;
				}

				/* Decompress the entire batch if it is supported. */
				CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

				column->compressed.data = tsl_try_decompress_all(header->compression_algorithm,
																 PointerGetDatum(header),
																 column->typid);

				if (!column->compressed.data && header->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA)
				{
					/* Just some experimental stuff. */
					DecompressionIterator *iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														/* reverse = */ false)(PointerGetDatum(header),
																		column->typid);
					column->compressed.data = default_decompress_all_forward_direction(
						iterator, iterator->try_next);
				}

				if (column->compressed.data)
				{
					Assert(column->compressed.data->length > 0);
					if (state->total_rows == 0)
					{
						state->total_rows = column->compressed.data->length;
					}
					else if (state->total_rows != column->compressed.data->length)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}

					column->compressed.iterator = NULL;

					break;
				}

				/* As a fallback, decompress row-by-row. */
				column->compressed.iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														state->reverse)(PointerGetDatum(header),
																		column->typid);

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
				Assert(count_value > 0);
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

static Datum
arrow_to_datum(const void *buffer, int row_index, Oid typeoid)
{
	switch (typeoid)
	{
		case BOOLOID:
			return BoolGetDatum(((int8 *) buffer)[row_index]);
		case CHAROID:
			return CharGetDatum(((int8 *) buffer)[row_index]);
		case INT2OID:
			return Int16GetDatum(((int16 *) buffer)[row_index]);
		case INT4OID:
			return Int32GetDatum(((int32 *) buffer)[row_index]);
		case INT8OID:
			return Int64GetDatum(((int64 *) buffer)[row_index]);
		case FLOAT4OID:
			return Float4GetDatum(((float4 *) buffer)[row_index]);
		case FLOAT8OID:
			return Float8GetDatum(((float8 *) buffer)[row_index]);
		case DATEOID:
			return DateADTGetDatum(((int32 *) buffer)[row_index]);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(((int64 *) buffer)[row_index]);
		case TIMESTAMPOID:
			return TimestampGetDatum(((int64 *) buffer)[row_index]);
		default:
			elog(ERROR, "cannot convert arrow type to datum with oid %d", typeoid);
			return (Datum) 0;
	}
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
			 * Reached end of batch. Check that the columns that we're decompressing
			 * row-by-row have also ended.
			 */
			state->initialized = false;
			for (int i = 0; i < state->num_columns; i++)
			{
				DecompressChunkColumnState *column = &state->columns[i];
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
			if (column->type != COMPRESSED_COLUMN)
			{
				continue;
			}

			const AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

			if (column->compressed.data)
			{
				Assert(column->compressed.iterator == NULL);

				const int row_index = state->reverse ? state->total_rows - state->current_row - 1 :
													   state->current_row;
				Assert(row_index < column->compressed.data->length);
				/* FIXME add code for passing float8 by reference on 32-bit systems. */
				slot->tts_isnull[attr] =
					!arrow_validity_bitmap_get(column->compressed.data->buffers[0], row_index);
				slot->tts_values[attr] =
					arrow_to_datum(column->compressed.data->buffers[1], row_index, column->typid);
			}
			else if (column->compressed.iterator != NULL)
			{
				DecompressResult result =
					column->compressed.iterator->try_next(column->compressed.iterator);

				if (result.is_done)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

				slot->tts_isnull[attr] = result.is_null;
				slot->tts_values[attr] = result.val;
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
