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
#include "compression/compression.h"
#include "guc.h"
#include "nodes/compressed_skip_scan/exec.h"
#include "ts_catalog/hypertable_compression.h"

static TupleTableSlot *compressed_skip_scan_exec(CustomScanState *node);
static void compressed_skip_scan_begin(CustomScanState *node, EState *estate, int eflags);
static void compressed_skip_scan_end(CustomScanState *node);
static void compressed_skip_scan_rescan(CustomScanState *node);
static TupleTableSlot *compressed_skip_scan_create_tuple(CompressedSkipScanState *chunk_state);

static CustomExecMethods compressed_skip_scan_state_methods = {
	.BeginCustomScan = compressed_skip_scan_begin,
	.ExecCustomScan = compressed_skip_scan_exec,
	.EndCustomScan = compressed_skip_scan_end,
	.ReScanCustomScan = compressed_skip_scan_rescan,
};

Node *
compressed_skip_scan_state_create(CustomScan *cscan)
{
	CompressedSkipScanState *chunk_state;
	List *settings;

	chunk_state =
		(CompressedSkipScanState *) newNode(sizeof(CompressedSkipScanState), T_CustomScanState);

	chunk_state->csstate.methods = &compressed_skip_scan_state_methods;

	settings = linitial(cscan->custom_private);

	Assert(list_length(settings) == 3);

	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->reverse = lthird_int(settings);
	chunk_state->decompression_map = lsecond(cscan->custom_private);
	return (Node *) chunk_state;
}

static void
initialize_column_state(CompressedSkipScanState *state)
{
	ScanState *ss = (ScanState *) state;
	TupleDesc desc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	ListCell *lc;

	if (list_length(state->decompression_map) == 0)
	{
		elog(ERROR, "no columns specified to decompress");
	}

	state->columns =
		palloc0(list_length(state->decompression_map) * sizeof(CompressedChunkColumnState));

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

		CompressedChunkColumnState *column = &state->columns[state->num_columns];
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

static void
compressed_skip_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	CompressedSkipScanState *state = (CompressedSkipScanState *) node;
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
initialize_batch(CompressedSkipScanState *state, TupleTableSlot *slot)
{
	Datum value;
	bool isnull;
	int i;
	MemoryContext old_context = MemoryContextSwitchTo(state->per_batch_context);
	MemoryContextReset(state->per_batch_context);

	for (i = 0; i < state->num_columns; i++)
	{
		CompressedChunkColumnState *column = &state->columns[i];

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
compressed_skip_scan_exec(CustomScanState *node)
{
	CompressedSkipScanState *state = (CompressedSkipScanState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	if (node->custom_ps == NIL)
		return NULL;

	while (true)
	{
		TupleTableSlot *slot = compressed_skip_scan_create_tuple(state);

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
compressed_skip_scan_rescan(CustomScanState *node)
{
	((CompressedSkipScanState *) node)->initialized = false;
	ExecReScan(linitial(node->custom_ps));
}

static void
compressed_skip_scan_end(CustomScanState *node)
{
	MemoryContextReset(((CompressedSkipScanState *) node)->per_batch_context);
	ExecEndNode(linitial(node->custom_ps));
}

static TupleTableSlot *
compressed_skip_scan_create_tuple(CompressedSkipScanState *state)
{
	TupleTableSlot *slot = state->csstate.ss.ss_ScanTupleSlot;
	bool batch_done = false;
	int i;

	while (true)
	{
		if (!state->initialized)
		{
			ExecStoreAllNullTuple(slot);

			TupleTableSlot *subslot = ExecProcNode(linitial(state->csstate.custom_ps));

			if (TupIsNull(subslot))
				return NULL;

			batch_done = false;
			initialize_batch(state, subslot);
		}

		ExecClearTuple(slot);

		for (i = 0; i < state->num_columns; i++)
		{
			CompressedChunkColumnState *column = &state->columns[i];
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
