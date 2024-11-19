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
#include "nodes/decompress_chunk/vector_quals.h"
#include "nodes/vector_agg.h"

static int
get_input_offset(DecompressChunkState *decompress_state, Var *var)
{
	DecompressContext *dcontext = &decompress_state->decompress_context;

	/*
	 * All variable references in the vectorized aggregation node were
	 * translated to uncompressed chunk variables when it was created.
	 */
	CustomScan *cscan = castNode(CustomScan, decompress_state->csstate.ss.ps.plan);
	Ensure((Index) var->varno == (Index) cscan->scan.scanrelid,
		   "got vector varno %d expected %d",
		   var->varno,
		   cscan->scan.scanrelid);

	CompressionColumnDescription *value_column_description = NULL;
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
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

	const int index = value_column_description - dcontext->compressed_chunk_columns;
	return index;
}

static int
grouping_column_comparator(const void *a_ptr, const void *b_ptr)
{
	const GroupingColumn *a = (GroupingColumn *) a_ptr;
	const GroupingColumn *b = (GroupingColumn *) b_ptr;

	if (a->value_bytes == b->value_bytes)
	{
		return 0;
	}

	if (a->value_bytes > b->value_bytes)
	{
		return -1;
	}

	return 1;
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
	const int tlist_length = list_length(aggregated_tlist);

	/*
	 * First, count how many grouping columns and aggregate functions we have.
	 */
	int agg_functions_counter = 0;
	int grouping_column_counter = 0;
	for (int i = 0; i < tlist_length; i++)
	{
		TargetEntry *tlentry = list_nth_node(TargetEntry, aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
			agg_functions_counter++;
		}
		else
		{
			/* This is a grouping column. */
			Assert(IsA(tlentry->expr, Var));
			grouping_column_counter++;
		}
	}
	Assert(agg_functions_counter + grouping_column_counter == tlist_length);

	/*
	 * Allocate the storage for definitions of aggregate function and grouping
	 * columns.
	 */
	vector_agg_state->num_agg_defs = agg_functions_counter;
	vector_agg_state->agg_defs =
		palloc0(sizeof(*vector_agg_state->agg_defs) * vector_agg_state->num_agg_defs);

	vector_agg_state->num_grouping_columns = grouping_column_counter;
	vector_agg_state->grouping_columns = palloc0(sizeof(*vector_agg_state->grouping_columns) *
												 vector_agg_state->num_grouping_columns);

	/*
	 * Loop through the aggregated targetlist again and fill the definitions.
	 */
	agg_functions_counter = 0;
	grouping_column_counter = 0;
	for (int i = 0; i < tlist_length; i++)
	{
		TargetEntry *tlentry = list_nth_node(TargetEntry, aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
			/* This is an aggregate function. */
			VectorAggDef *def = &vector_agg_state->agg_defs[agg_functions_counter++];
			def->output_offset = i;

			Aggref *aggref = castNode(Aggref, tlentry->expr);

			//			fprintf(stderr, "the aggref at execution is:\n");
			//			my_print(aggref);

			VectorAggFunctions *func = get_vector_aggregate(aggref->aggfnoid);
			Assert(func != NULL);
			def->func = *func;

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

			if (aggref->aggfilter != NULL)
			{
				def->filter_clauses = list_make1(aggref->aggfilter);
			}
		}
		else
		{
			/* This is a grouping column. */
			Assert(IsA(tlentry->expr, Var));

			GroupingColumn *col = &vector_agg_state->grouping_columns[grouping_column_counter++];
			col->output_offset = i;

			Var *var = castNode(Var, tlentry->expr);
			col->input_offset = get_input_offset(decompress_state, var);

			DecompressContext *dcontext = &decompress_state->decompress_context;
			CompressionColumnDescription *desc =
				&dcontext->compressed_chunk_columns[col->input_offset];

			col->typid = desc->typid;
			col->value_bytes = desc->value_bytes;
			col->by_value = desc->by_value;
			if (col->value_bytes == -1)
			{
				/*
				 * long varlena requires 4 byte alignment, not sure why text has 'i'
				 * typalign in pg catalog.
				 */
				col->alignment_bytes = 4;
			}
			else
			{
				switch (desc->typalign)
				{
					case TYPALIGN_CHAR:
						col->alignment_bytes = 1;
						break;
					case TYPALIGN_SHORT:
						col->alignment_bytes = ALIGNOF_SHORT;
						break;
					case TYPALIGN_INT:
						col->alignment_bytes = ALIGNOF_INT;
						break;
					case TYPALIGN_DOUBLE:
						col->alignment_bytes = ALIGNOF_DOUBLE;
						break;
					default:
						Assert(false);
						col->alignment_bytes = 1;
				}
			}
		}
	}

	/*
	 * Sort grouping columns by descending column size, variable size last. This
	 * helps improve branch predictability and key packing when we use hashed
	 * serialized multi-column keys.
	 */
	qsort(vector_agg_state->grouping_columns,
		  vector_agg_state->num_grouping_columns,
		  sizeof(GroupingColumn),
		  grouping_column_comparator);

	/*
	 * Determine which grouping policy we are going to use.
	 */
	bool all_segmentby = true;
	for (int i = 0; i < vector_agg_state->num_grouping_columns; i++)
	{
		GroupingColumn *col = &vector_agg_state->grouping_columns[i];
		DecompressContext *dcontext = &decompress_state->decompress_context;
		CompressionColumnDescription *desc = &dcontext->compressed_chunk_columns[col->input_offset];
		//		if (desc->type == COMPRESSED_COLUMN && desc->by_value && desc->value_bytes > 0 &&
		//			(size_t) desc->value_bytes <= sizeof(Datum))
		if (desc->type != SEGMENTBY_COLUMN)
		{
			all_segmentby = false;
			break;
		}
	}
	if (all_segmentby)
	{
		/*
		 * Per-batch grouping.
		 */
		vector_agg_state->grouping =
			create_grouping_policy_batch(vector_agg_state->num_agg_defs,
										 vector_agg_state->agg_defs,
										 vector_agg_state->num_grouping_columns,
										 vector_agg_state->grouping_columns);
	}
	else
	{
		/*
		 * Hash grouping.
		 */
		vector_agg_state->grouping =
			create_grouping_policy_hash(vector_agg_state->num_agg_defs,
										vector_agg_state->agg_defs,
										vector_agg_state->num_grouping_columns,
										vector_agg_state->grouping_columns);
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
	VectorAggState *vector_agg_state = (VectorAggState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ResetExprContext(econtext);

	TupleTableSlot *aggregated_slot = vector_agg_state->custom.ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(aggregated_slot);

	/*
	 * If we have more partial aggregation results, continue returning them.
	 */
	GroupingPolicy *grouping = vector_agg_state->grouping;
	MemoryContext old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	bool have_partial = grouping->gp_do_emit(grouping, aggregated_slot);
	MemoryContextSwitchTo(old_context);
	if (have_partial)
	{
		/* The grouping policy produced a partial aggregation result. */
		return ExecStoreVirtualTuple(aggregated_slot);
	}

	/*
	 * If the partial aggregation results have ended, and the input has ended,
	 * we're done.
	 */
	if (vector_agg_state->input_ended)
	{
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
		/*
		 * We discard the previous compressed batch here and not earlier,
		 * because the grouping column values returned by the batch grouping
		 * policy are owned by the compressed batch memory context. This is done
		 * to avoid generic value copying in the grouping policy to simplify its
		 * code.
		 */
		compressed_batch_discard_tuples(batch_state);

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

		/*
		 * Compute the vectorized filters for the aggregate function FILTER
		 * clauses.
		 */
		const int naggs = vector_agg_state->num_agg_defs;
		for (int i = 0; i < naggs; i++)
		{
			VectorAggDef *agg_def = &vector_agg_state->agg_defs[i];
			if (agg_def->filter_clauses == NIL)
			{
				continue;
			}
			CompressedBatchVectorQualState cbvqstate = {
				.vqstate = {
					.vectorized_quals_constified = agg_def->filter_clauses,
					.num_results = batch_state->total_batch_rows,
					.per_vector_mcxt = batch_state->per_batch_context,
					.slot = compressed_slot,
					.get_arrow_array = compressed_batch_get_arrow_array,
				},
				.batch_state = batch_state,
				.dcontext = dcontext,
			};
			VectorQualState *vqstate = &cbvqstate.vqstate;
			vector_qual_compute(vqstate);
			agg_def->filter_result = vqstate->vector_qual_result;
		}

		/*
		 * Finally, pass the compressed batch to the grouping policy.
		 */
		grouping->gp_add_batch(grouping, batch_state);
	}

	/*
	 * If we have partial aggregation results, start returning them.
	 */
	old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	have_partial = grouping->gp_do_emit(grouping, aggregated_slot);
	MemoryContextSwitchTo(old_context);
	if (have_partial)
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
	VectorAggState *state = (VectorAggState *) node;
	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyText("Grouping Policy", state->grouping->gp_explain(state->grouping), es);
	}
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
