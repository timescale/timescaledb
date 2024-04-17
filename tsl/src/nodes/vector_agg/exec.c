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

#include "nodes/vector_agg/exec.h"

#include "compression/arrow_c_data_interface.h"
#include "functions.h"
#include "guc.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/vector_agg.h"

static void
get_input_offset(DecompressChunkState *decompress_state, Var *var, int *input_offset)
{
	DecompressContext *dcontext = &decompress_state->decompress_context;

	CompressionColumnDescription *value_column_description = NULL;
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
		CompressionColumnDescription *current_column = &dcontext->compressed_chunk_columns[i];
		if (current_column->output_attno == var->varattno)
		{
			value_column_description = current_column;
			break;
		}
	}
	Ensure(value_column_description != NULL, "aggregated compressed column not found");

	Assert(value_column_description->type == COMPRESSED_COLUMN ||
		   value_column_description->type == SEGMENTBY_COLUMN);

	*input_offset = value_column_description - dcontext->compressed_chunk_columns;
}

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
	const int naggs = list_length(aggregated_tlist);
	for (int i = 0; i < naggs; i++)
	{
		/* Determine which kind of vectorized aggregation we should perform */
		TargetEntry *tlentry = (TargetEntry *) list_nth(aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
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
				get_input_offset(decompress_state, var, &def->input_offset);
			}
			else
			{
				def->input_offset = -1;
			}

			def->output_offset = i;

			vector_agg_state->agg_defs = lappend(vector_agg_state->agg_defs, def);
		}
		else
		{
			Assert(IsA(tlentry->expr, Var));
			Var *var = castNode(Var, tlentry->expr);
			GroupingColumn *col = palloc0(sizeof(GroupingColumn));
			col->output_offset = i;
			get_input_offset(decompress_state, var, &col->input_offset);
			vector_agg_state->output_grouping_columns =
				lappend(vector_agg_state->output_grouping_columns, col);
		}
	}

	List *grouping_column_offsets = linitial(cscan->custom_private);
	if (grouping_column_offsets == NIL)
	{
		vector_agg_state->grouping = create_grouping_policy_all(vector_agg_state->agg_defs);
	}
	else
	{
		vector_agg_state->grouping =
			create_grouping_policy_segmentby(vector_agg_state->agg_defs,
											 vector_agg_state->output_grouping_columns);
	}
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

	state->grouping->gp_reset(state->grouping);
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

	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);

	DecompressContext *dcontext = &decompress_state->decompress_context;

	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	/* Get a reference the the output TupleTableSlot */
	TupleTableSlot *aggregated_slot = vector_agg_state->custom.ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(aggregated_slot);

	GroupingPolicy *grouping = vector_agg_state->grouping;

	bool have_tuples_this_loop = false;
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

		have_tuples_this_loop = true;

		grouping->gp_add_batch(grouping, batch_state);

		compressed_batch_discard_tuples(batch_state);

		if (grouping->gp_should_emit(grouping))
		{
			break;
		}
	}

	if (!have_tuples_this_loop)
	{
		Assert(vector_agg_state->input_ended);
		return NULL;
	}

	grouping->gp_do_emit(grouping, aggregated_slot);

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
