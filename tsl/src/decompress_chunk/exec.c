/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <executor/executor.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/relation.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/plancat.h>
#include <optimizer/predtest.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "decompress_chunk/decompress_chunk.h"
#include "decompress_chunk/exec.h"
#include "decompress_chunk/planner.h"
#include "hypertable_compression.h"

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnState
{
	DecompressChunkColumnType type;
	Oid typid;
	Oid attno;
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
	};
} DecompressChunkColumnState;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *varattno_map;
	int num_columns;
	DecompressChunkColumnState *columns;

	bool initialized;
	bool reverse;
	int hypertable_id;
	List *hypertable_compression_info;
	int counter;
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
	state->reverse = lsecond_int(settings);
	state->varattno_map = lsecond(cscan->custom_private);

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
	int i;

	state->num_columns = list_length(state->varattno_map);

	state->columns = palloc0(state->num_columns * sizeof(DecompressChunkColumnState));

	for (i = 0, lc = list_head(state->varattno_map); i < state->num_columns; lc = lnext(lc), i++)
	{
		DecompressChunkColumnState *column = &state->columns[i];
		Assert(lfirst_int(lc) >= 0);
		column->attno = lfirst_int(lc);

		if (column->attno > 0)
		{
			/* normal column that is also present in uncompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(lfirst_int(lc)));
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
			/* special column with metadata */
			column->type = COUNT_COLUMN;
		}
	}
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

	if (eflags & EXEC_FLAG_BACKWARD)
		state->reverse = !state->reverse;

	state->hypertable_compression_info = get_hypertablecompression_info(state->hypertable_id);

	initialize_column_state(state);

	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));
}

static void
initialize_batch(DecompressChunkState *state, TupleTableSlot *slot)
{
	Datum value;
	bool isnull;
	int i;

	for (i = 0; i < state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
				if (!isnull)
				{
					CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

					column->compressed.iterator =
						tsl_get_decompression_iterator_init(header->compression_algorithm,
															state->reverse)(PointerGetDatum(header),
																			column->typid);
				}
				else
					column->compressed.iterator = NULL;

				break;
			}
			case SEGMENTBY_COLUMN:
				value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
				if (!isnull)
					column->segmentby.value = value;
				else
					column->segmentby.value = (Datum) 0;

				column->segmentby.isnull = isnull;
				break;
			case COUNT_COLUMN:
				value = slot_getattr(slot, AttrOffsetGetAttrNumber(i), &isnull);
				state->counter = DatumGetInt32(value);
				/* count column should never be NULL */
				Assert(!isnull);
				break;
		}
	}
	state->initialized = true;
}

static TupleTableSlot *
decompress_chunk_exec(CustomScanState *node)
{
	DecompressChunkState *state = (DecompressChunkState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
#if PG96
	TupleTableSlot *resultslot;
	ExprDoneCond isDone;
#endif

	if (node->custom_ps == NIL)
		return NULL;

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	ResetExprContext(econtext);

	while (true)
	{
		TupleTableSlot *slot = decompress_chunk_create_tuple(state);

		if (TupIsNull(slot))
			return NULL;

		econtext->ecxt_scantuple = slot;

#if PG96
		if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext, false))
#else
		if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext))
#endif
		{
			InstrCountFiltered1(node, 1);
			ExecClearTuple(slot);
			continue;
		}

		if (!node->ss.ps.ps_ProjInfo)
			return slot;

#if PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#else
		return ExecProject(node->ss.ps.ps_ProjInfo);
#endif
	}
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static void
decompress_chunk_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

/*
 * Create generated tuple according to column state
 */
static TupleTableSlot *
decompress_chunk_create_tuple(DecompressChunkState *state)
{
	TupleTableSlot *slot = state->csstate.ss.ss_ScanTupleSlot;
	bool batch_done = false;
	int i;

	while (true)
	{
		if (!state->initialized)
		{
			TupleTableSlot *subslot = ExecProcNode(linitial(state->csstate.custom_ps));

			if (TupIsNull(subslot))
				return NULL;

			batch_done = false;
			initialize_batch(state, subslot);
		}

		ExecClearTuple(slot);

		for (i = 0; i < state->num_columns; i++)
		{
			DecompressChunkColumnState *column = &state->columns[i];
			switch (column->type)
			{
				case COUNT_COLUMN:
					if (state->counter <= 0)
						/*
						 * we continue checking other columns even if counter
						 * reaches zero to sanity check all columns are in sync
						 * and agree about batch end
						 */
						batch_done = true;
					else
						state->counter--;
					break;
				case COMPRESSED_COLUMN:
				{
					AttrNumber attr = AttrNumberGetAttrOffset(column->attno);

					if (column->compressed.iterator != NULL)
					{
						DecompressResult result;
						result = column->compressed.iterator->try_next(column->compressed.iterator);

						if (result.is_done)
						{
							batch_done = true;
							continue;
						}
						else if (batch_done)
						{
							/*
							 * since the count column is the first column batch_done
							 * might be true if compressed column is out of sync with
							 * the batch counter.
							 */
							elog(ERROR, "compressed column out of sync with batch counter");
						}

						slot->tts_values[attr] = result.val;
						slot->tts_isnull[attr] = result.is_null;
					}
					else
						slot->tts_isnull[attr] = true;

					break;
				}
				case SEGMENTBY_COLUMN:
				{
					AttrNumber attr = AttrNumberGetAttrOffset(column->attno);

					slot->tts_values[attr] = column->segmentby.value;
					slot->tts_isnull[attr] = column->segmentby.isnull;
					break;
				}
			}
		}

		if (batch_done)
		{
			state->initialized = false;
			continue;
		}

		ExecStoreVirtualTuple(slot);

		return slot;
	}
}
