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
	int counter;
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

static void
initialize_batch(DecompressChunkState *state, TupleTableSlot *slot)
{
	Datum value;
	bool isnull;
	int i;
	MemoryContext old_context = MemoryContextSwitchTo(state->per_batch_context);
	MemoryContextReset(state->per_batch_context);

	for (i = 0; i < state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
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
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
				if (!isnull)
					column->segmentby.value = value;
				else
					column->segmentby.value = (Datum) 0;

				column->segmentby.isnull = isnull;
				break;
			case COUNT_COLUMN:
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
				state->counter = DatumGetInt32(value);
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

		/* Reset expression memory context to clean out any cruft from
		 * previous tuple. */
		ResetExprContext(econtext);

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
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

					if (!column->compressed.iterator)
					{
						slot->tts_values[attr] = getmissingattr(slot->tts_tupleDescriptor,
																attr + 1,
																&slot->tts_isnull[attr]);
					}
					else
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

					break;
				}
				case SEGMENTBY_COLUMN:
				{
					AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

					slot->tts_values[attr] = column->segmentby.value;
					slot->tts_isnull[attr] = column->segmentby.isnull;
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

		if (batch_done)
		{
			state->initialized = false;
			continue;
		}

		ExecStoreVirtualTuple(slot);

		return slot;
	}
}
