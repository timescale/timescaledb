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
#include "guc.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/vector_agg.h"

static int
get_input_offset(DecompressChunkState *decompress_state, Var *var)
{
	DecompressContext *dcontext = &decompress_state->decompress_context;

	CompressionColumnDescription *value_column_description = NULL;
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
		/*
		 * See the column lookup in compute_plain_qual() for the discussion of
		 * which attribute numbers occur where. At the moment here it is
		 * uncompressed_scan_attno, but it might be an oversight of not rewriting
		 * the references into INDEX_VAR (or OUTER_VAR...?) when we create the
		 * VectorAgg node.
		 */
		CompressionColumnDescription *current_column = &dcontext->compressed_chunk_columns[i];
		if (current_column->uncompressed_chunk_attno == var->varattno)
		{
			value_column_description = current_column;
			break;
		}
	}
	Ensure(value_column_description != NULL, "aggregated compressed column not found");

	Assert(value_column_description->type == COMPRESSED_COLUMN ||
		   value_column_description->type == SEGMENTBY_COLUMN);

	return value_column_description - dcontext->compressed_chunk_columns;
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
	 * Now, iterate through the aggregated targetlist to collect aggregates and
	 * output grouping columns.
	 */
	List *aggregated_tlist =
		castNode(CustomScan, vector_agg_state->custom.ss.ps.plan)->custom_scan_tlist;
	const int naggs = list_length(aggregated_tlist);
	for (int i = 0; i < naggs; i++)
	{
		TargetEntry *tlentry = (TargetEntry *) list_nth(aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
			/* This is an aggregate function. */
			VectorAggDef *def = palloc0(sizeof(VectorAggDef));
			vector_agg_state->agg_defs = lappend(vector_agg_state->agg_defs, def);
			def->output_offset = i;

			Aggref *aggref = castNode(Aggref, tlentry->expr);
			VectorAggFunctions *func = get_vector_aggregate(aggref->aggfnoid);
			Assert(func != NULL);
			def->func = func;

			if (list_length(aggref->args) > 0)
			{
				Assert(list_length(aggref->args) == 1);

				/* The aggregate should be a partial aggregate */
				Assert(aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL);

				Var *var = castNode(Var, castNode(TargetEntry, linitial(aggref->args))->expr);
				def->input_offset = get_input_offset(decompress_state, var);
			}
			else
			{
				def->input_offset = -1;
			}
		}
		else
		{
			/* This is a grouping column. */
			Assert(IsA(tlentry->expr, Var));

			GroupingColumn *col = palloc0(sizeof(GroupingColumn));
			vector_agg_state->output_grouping_columns =
				lappend(vector_agg_state->output_grouping_columns, col);
			col->output_offset = i;

			Var *var = castNode(Var, tlentry->expr);
			col->input_offset = get_input_offset(decompress_state, var);
		}
	}

	List *grouping_column_offsets = linitial(cscan->custom_private);
	vector_agg_state->grouping =
		create_grouping_policy_batch(vector_agg_state->agg_defs,
									 vector_agg_state->output_grouping_columns,
									 /* partial_per_batch = */ grouping_column_offsets != NIL);
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
	VectorAggState *vector_agg_state = (VectorAggState *) node;

	TupleTableSlot *aggregated_slot = vector_agg_state->custom.ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(aggregated_slot);

	GroupingPolicy *grouping = vector_agg_state->grouping;
	if (grouping->gp_do_emit(grouping, aggregated_slot))
	{
		/* The grouping policy produced a partial aggregation result. */
		return ExecStoreVirtualTuple(aggregated_slot);
	}

	if (vector_agg_state->input_ended)
	{
		/*
		 * The partial aggregation results have ended, and the input has ended,
		 * so we're done.
		 */
		return NULL;
	}

	/*
	 * Have no more partial aggregation results and still have input, have to
	 * reset the grouping policy and start a new cycle of partial aggregation.
	 */
	grouping->gp_reset(grouping);

	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);

	DecompressContext *dcontext = &decompress_state->decompress_context;

	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	/*
	 * Now we loop through the input compressed tuples, until they end or until
	 * the grouping policy asks us to emit partials.
	 */
	while (!grouping->gp_should_emit(grouping))
	{
		TupleTableSlot *compressed_slot =
			ExecProcNode(linitial(decompress_state->csstate.custom_ps));

		if (TupIsNull(compressed_slot))
		{
			/* The input has ended. */
			vector_agg_state->input_ended = true;
			break;
		}

		compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);

		if (batch_state->next_batch_row >= batch_state->total_batch_rows)
		{
			/* This batch was fully filtered out. */
			continue;
		}

		/*
		 * Count rows filtered out by vectorized filters for EXPLAIN. Normally
		 * this is done in tuple-by-tuple interface of DecompressChunk, so that
		 * it doesn't say it filtered out more rows that were returned (e.g.
		 * with LIMIT). Here we always work in full batches. The batches that
		 * were fully filtered out, and their rows, were already counted in
		 * compressed_batch_set_compressed_tuple().
		 */
		const int not_filtered_rows =
			arrow_num_valid(batch_state->vector_qual_result, batch_state->total_batch_rows);
		InstrCountFiltered1(dcontext->ps, batch_state->total_batch_rows - not_filtered_rows);
		if (dcontext->ps->instrument)
		{
			/*
			 * These values are normally updated by InstrStopNode(), and are
			 * required so that the calculations in InstrEndLoop() run properly.
			 */
			dcontext->ps->instrument->running = true;
			dcontext->ps->instrument->tuplecount += not_filtered_rows;
		}

		grouping->gp_add_batch(grouping, batch_state);

		compressed_batch_discard_tuples(batch_state);
	}

	if (grouping->gp_do_emit(grouping, aggregated_slot))
	{
		/* Have partial aggregation results. */
		return ExecStoreVirtualTuple(aggregated_slot);
	}

	if (vector_agg_state->input_ended)
	{
		/*
		 * Have no partial aggregation results and the input has ended, so we're
		 * done. We can get here only if we had no input at all, otherwise the
		 * grouping policy would have produced some partials above.
		 */
		return NULL;
	}

	/*
	 * We cannot get here. This would mean we still have input, and the
	 * grouping policy asked us to stop but couldn't produce any partials.
	 */
	Assert(false);
	pg_unreachable();
	return NULL;
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
