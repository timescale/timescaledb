/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>

#include "exec.h"

#include "compression/arrow_c_data_interface.h"
#include "functions.h"
#include "guc.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/vector_agg.h"

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));

	VectorAggState *vector_agg_state = (VectorAggState *) node;
	vector_agg_state->input_ended = false;

	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);

	/*
	 * The aggregated targetlist with Aggrefs is in the custom scan targetlist
	 * of the custom scan node that is performing the vectorized aggregation.
	 * We do this to avoid projections at this node, because the postgres
	 * projection functions complain when they see an Aggref in a custom
	 * node output targetlist.
	 * The output targetlist, in turn, consists of just the INDEX_VAR references
	 * into the custom_scan_tlist.
	 */
	List *aggregated_tlist =
		castNode(CustomScan, vector_agg_state->custom.ss.ps.plan)->custom_scan_tlist;
	ListCell *lc;
	foreach (lc, aggregated_tlist)
	{
		/* Determine which kind of vectorized aggregation we should perform */
		TargetEntry *tlentry = (TargetEntry *) lfirst(lc);
		Assert(IsA(tlentry->expr, Aggref));
		Aggref *aggref = castNode(Aggref, tlentry->expr);

		VectorAggDef *def = palloc0(sizeof(VectorAggDef));
		VectorAggFunctions *func = get_vector_aggregate(aggref->aggfnoid);
		Assert(func != NULL);
		def->func = func;

		if (list_length(aggref->args) > 0)
		{
			Assert(list_length(aggref->args) == 1);

			/* The aggregate should be a partial aggregate */
			Assert(aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL);

			Var *var = castNode(Var, castNode(TargetEntry, linitial(aggref->args))->expr);

			DecompressContext *dcontext = &decompress_state->decompress_context;

			CompressionColumnDescription *value_column_description = NULL;
			for (int i = 0; i < dcontext->num_total_columns; i++)
			{
				CompressionColumnDescription *current_column = &dcontext->template_columns[i];
				if (current_column->output_attno == var->varattno)
				{
					value_column_description = current_column;
					break;
				}
			}
			Ensure(value_column_description != NULL, "aggregated compressed column not found");

			Assert(value_column_description->type == COMPRESSED_COLUMN ||
				   value_column_description->type == SEGMENTBY_COLUMN);

			def->column = value_column_description - dcontext->template_columns;
		}
		else
		{
			def->column = -1;
		}

		vector_agg_state->agg_defs = lappend(vector_agg_state->agg_defs, def);

		vector_agg_state->agg_state_row_bytes += MAXALIGN(func->state_bytes);
	}

	vector_agg_state->num_agg_state_rows = 1;
	vector_agg_state->agg_states = palloc(vector_agg_state->agg_state_row_bytes);
}

static void
vector_agg_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
vector_agg_rescan(CustomScanState *node)
{
	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(linitial(node->custom_ps), node->ss.ps.chgParam);

	ExecReScan(linitial(node->custom_ps));

	VectorAggState *state = (VectorAggState *) node;
	state->input_ended = false;
}

static TupleTableSlot *
vector_agg_exec(CustomScanState *node)
{
	/*
	 * Early exit if the input has ended.
	 */
	VectorAggState *vector_agg_state = (VectorAggState *) node;
	if (vector_agg_state->input_ended)
	{
		return NULL;
	}

	VectorAggDef *def = (VectorAggDef *) linitial(vector_agg_state->agg_defs);
	def->func->agg_init(vector_agg_state->agg_states);

	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);

	DecompressContext *dcontext = &decompress_state->decompress_context;

	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	/* Get a reference the the output TupleTableSlot */
	TupleTableSlot *aggregated_slot = vector_agg_state->custom.ss.ps.ps_ResultTupleSlot;
	Assert(aggregated_slot->tts_tupleDescriptor->natts == 1);
	ExecClearTuple(aggregated_slot);

	for (;;)
	{
		/*
		 * Have to skip the batches that are fully filtered out. This condition also
		 * handles the batch that was consumed on the previous step.
		 */
		while (batch_state->next_batch_row >= batch_state->total_batch_rows)
		{
			TupleTableSlot *compressed_slot =
				ExecProcNode(linitial(decompress_state->csstate.custom_ps));

			if (TupIsNull(compressed_slot))
			{
				/* All values are processed. */
				vector_agg_state->input_ended = true;
				break;
			}

			compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);
		}

		if (vector_agg_state->input_ended)
		{
			break;
		}

		/*
		 * To calculate the sum for a segment by value or default compressed
		 * column value, we need to multiply this value with the number of
		 * passing decompressed tuples in this batch.
		 */
		int n = batch_state->total_batch_rows;
		if (batch_state->vector_qual_result)
		{
			n = arrow_num_valid(batch_state->vector_qual_result, n);
			Assert(n > 0);
		}

		if (def->column >= 0)
		{
			ArrowArray *arrow = NULL;
			CompressionColumnDescription *value_column_description =
				&dcontext->template_columns[def->column];
			if (value_column_description->type == COMPRESSED_COLUMN)
			{
				Assert(dcontext->enable_bulk_decompression);
				Assert(value_column_description->bulk_decompression_supported);
				CompressedColumnValues *values = &batch_state->compressed_columns[def->column];
				Assert(values->decompression_type != DT_Invalid);
				Assert(values->decompression_type != DT_Iterator);
				arrow = values->arrow;
			}
			else
			{
				Assert(value_column_description->type == SEGMENTBY_COLUMN);
			}
			if (arrow == NULL)
			{
				const int offs = AttrNumberGetAttrOffset(value_column_description->output_attno);
				const Datum value = batch_state->decompressed_scan_slot_data.base.tts_values[offs];
				const bool is_null = batch_state->decompressed_scan_slot_data.base.tts_isnull[offs];
				def->func->agg_const(vector_agg_state->agg_states, value, is_null, n);
			}
			else
			{
				def->func->agg_vector(vector_agg_state->agg_states,
									  arrow,
									  batch_state->vector_qual_result);
			}
		}
		else
		{
		    /*
			 * We have only one function w/o arguments -- count(*). Unfortunately
			 * it has to have a special code path everywhere.
			 */
			def->func->agg_const(vector_agg_state->agg_states, 0, true, n);
		}

		compressed_batch_discard_tuples(batch_state);
	}

	def->func->agg_emit(vector_agg_state->agg_states,
						&aggregated_slot->tts_values[0],
						&aggregated_slot->tts_isnull[0]);

	ExecStoreVirtualTuple(aggregated_slot);

	return aggregated_slot;
}

static void
vector_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* No additional output is needed. */
}

static struct CustomExecMethods exec_methods = {
	.CustomName = VECTOR_AGG_NODE_NAME,
	.BeginCustomScan = vector_agg_begin,
	.ExecCustomScan = vector_agg_exec,
	.EndCustomScan = vector_agg_end,
	.ReScanCustomScan = vector_agg_rescan,
	.ExplainCustomScan = vector_agg_explain,
};

Node *
vector_agg_state_create(CustomScan *cscan)
{
	VectorAggState *state = (VectorAggState *) newNode(sizeof(VectorAggState), T_CustomScanState);
	state->custom.methods = &exec_methods;
	return (Node *) state;
}
