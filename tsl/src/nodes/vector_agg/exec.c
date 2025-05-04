/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/optimizer.h>

#include "nodes/vector_agg/exec.h"

#include "compression/arrow_c_data_interface.h"
#include "hypercore/arrow_tts.h"
#include "hypercore/vector_quals.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/vector_quals.h"
#include "nodes/vector_agg.h"
#include "nodes/vector_agg/plan.h"

static int
get_input_offset_decompress_chunk(const DecompressChunkState *decompress_state, const Var *var)
{
	const DecompressContext *dcontext = &decompress_state->decompress_context;

	/*
	 * All variable references in the vectorized aggregation node were
	 * translated to uncompressed chunk variables when it was created.
	 */
	const CustomScan *cscan = castNode(CustomScan, decompress_state->csstate.ss.ps.plan);
	Ensure((Index) var->varno == (Index) cscan->scan.scanrelid,
		   "got vector varno %d expected %d",
		   var->varno,
		   cscan->scan.scanrelid);

	const CompressionColumnDescription *value_column_description = NULL;
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
		const CompressionColumnDescription *current_column = &dcontext->compressed_chunk_columns[i];
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
get_column_storage_properties_decompress_chunk(const DecompressChunkState *state, int input_offset,
											   GroupingColumn *result)
{
	const DecompressContext *dcontext = &state->decompress_context;
	const CompressionColumnDescription *desc = &dcontext->compressed_chunk_columns[input_offset];
	result->value_bytes = desc->value_bytes;
	result->by_value = desc->by_value;
}

/*
 * Given a Var reference, get the offset of the corresponding attribute in the
 * input tuple.
 *
 * For a node returning arrow slots, this is just the attribute number in the
 * Var. But if the node is DecompressChunk, it is necessary to translate
 * between the compressed and non-compressed columns.
 */
static int
get_input_offset(const CustomScanState *state, const Var *var)
{
	if (TTS_IS_ARROWTUPLE(state->ss.ss_ScanTupleSlot))
		return AttrNumberGetAttrOffset(var->varattno);

	return get_input_offset_decompress_chunk((const DecompressChunkState *) state, var);
}

/*
 * Get the type length and "byval" properties for the grouping column given by
 * the input offset.
 *
 * For a node returning arrow slots, the properties can be read directly from
 * the scanned relation's tuple descriptor. For DecompressChunk, the input
 * offset references the compressed relation.
 */
static void
get_column_storage_properties(const CustomScanState *state, int input_offset,
							  GroupingColumn *result)
{
	if (TTS_IS_ARROWTUPLE(state->ss.ss_ScanTupleSlot))
	{
		const TupleDesc tupdesc = RelationGetDescr(state->ss.ss_currentRelation);
		result->by_value = TupleDescAttr(tupdesc, input_offset)->attbyval;
		result->value_bytes = TupleDescAttr(tupdesc, input_offset)->attlen;
		return;
	}

	get_column_storage_properties_decompress_chunk((const DecompressChunkState *) state,
												   input_offset,
												   result);
}

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));

	VectorAggState *vector_agg_state = (VectorAggState *) node;
	vector_agg_state->input_ended = false;
	CustomScanState *childstate = (CustomScanState *) linitial(vector_agg_state->custom.custom_ps);

	/*
	 * Set up the helper structures used to evaluate stable expressions in
	 * vectorized FILTER clauses.
	 */
	PlannerGlobal glob = {
		.boundParams = node->ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

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

			VectorAggFunctions *func = get_vector_aggregate(aggref->aggfnoid);
			Assert(func != NULL);
			def->func = *func;

			if (list_length(aggref->args) > 0)
			{
				Assert(list_length(aggref->args) == 1);

				/* The aggregate should be a partial aggregate */
				Assert(aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL);

				Var *var = castNode(Var, castNode(TargetEntry, linitial(aggref->args))->expr);
				def->input_offset = get_input_offset(childstate, var);
			}
			else
			{
				def->input_offset = -1;
			}

			if (aggref->aggfilter != NULL)
			{
				Node *constified = estimate_expression_value(&root, (Node *) aggref->aggfilter);
				def->filter_clauses = list_make1(constified);
			}
		}
		else
		{
			/* This is a grouping column. */
			Assert(IsA(tlentry->expr, Var));

			GroupingColumn *col = &vector_agg_state->grouping_columns[grouping_column_counter++];
			col->output_offset = i;

			Var *var = castNode(Var, tlentry->expr);
			col->input_offset = get_input_offset(childstate, var);
			get_column_storage_properties(childstate, col->input_offset, col);
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
	 * Create the grouping policy chosen at plan time.
	 */
	const VectorAggGroupingType grouping_type =
		intVal(list_nth(cscan->custom_private, VASI_GroupingType));
	if (grouping_type == VAGT_Batch)
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
										vector_agg_state->grouping_columns,
										grouping_type);
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

/*
 * Get the next slot to aggregate for a compressed batch.
 *
 * Implements "get next slot" on top of DecompressChunk. Note that compressed
 * tuples are read directly from the DecompressChunk child node, which means
 * that the processing normally done in DecompressChunk is actually done here
 * (batch processing and filtering).
 *
 * Returns an TupleTableSlot that implements a compressed batch.
 */
static TupleTableSlot *
compressed_batch_get_next_slot(VectorAggState *vector_agg_state)
{
	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);
	DecompressContext *dcontext = &decompress_state->decompress_context;
	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	do
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
			vector_agg_state->input_ended = true;
			return NULL;
		}

		compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);

		/* If the entire batch is filtered out, then immediately read the next
		 * one */
	} while (batch_state->next_batch_row >= batch_state->total_batch_rows);

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

	return &batch_state->decompressed_scan_slot_data.base;
}

/*
 * Get the next slot to aggregate for a arrow tuple table slot.
 *
 * Implements "get next slot" on top of ColumnarScan (or any node producing
 * ArrowTupleTableSlots). It just reads the slot from the child node.
 */
static TupleTableSlot *
arrow_get_next_slot(VectorAggState *vector_agg_state)
{
	TupleTableSlot *slot = vector_agg_state->custom.ss.ss_ScanTupleSlot;

	if (!TTS_EMPTY(slot))
	{
		Assert(TTS_IS_ARROWTUPLE(slot));

		/* If we read an arrow slot previously, the entire arrow array should
		 * have been aggregated so we should mark it is consumed so that we
		 * get the next array (or end) when we read the next slot. */

		arrow_slot_mark_consumed(slot);
	}

	slot = ExecProcNode(linitial(vector_agg_state->custom.custom_ps));

	if (TupIsNull(slot))
	{
		/* The input has ended. */
		vector_agg_state->input_ended = true;
		return NULL;
	}

	Assert(TTS_IS_ARROWTUPLE(slot));

	/* Filtering should have happened in the scan node below so the slot
	 * should not be consumed here. */
	Assert(!arrow_slot_is_consumed(slot));

	/* Remember the slot until we're called next time */
	vector_agg_state->custom.ss.ss_ScanTupleSlot = slot;

	return slot;
}

/*
 * Initialize vector quals for a compressed batch.
 *
 * Used to implement vectorized aggregate function filter clause.
 */
static VectorQualState *
compressed_batch_init_vector_quals(VectorAggState *agg_state, VectorAggDef *agg_def,
								   TupleTableSlot *slot)
{
	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(agg_state->custom.custom_ps);
	DecompressContext *dcontext = &decompress_state->decompress_context;
	DecompressBatchState *batch_state = (DecompressBatchState *) slot;

	agg_state->vqual_state = (CompressedBatchVectorQualState) {
				.vqstate = {
					.vectorized_quals_constified = agg_def->filter_clauses,
					.num_results = batch_state->total_batch_rows,
					.per_vector_mcxt = batch_state->per_batch_context,
					.slot = decompress_state->csstate.ss.ss_ScanTupleSlot,
					.get_arrow_array = compressed_batch_get_arrow_array,
				},
				.batch_state = batch_state,
				.dcontext = dcontext,
			};

	return &agg_state->vqual_state.vqstate;
}

/*
 * Initialize FILTER vector quals for an arrow tuple slot.
 *
 * Used to implement vectorized aggregate function filter clause.
 */
static VectorQualState *
arrow_init_vector_quals(VectorAggState *agg_state, VectorAggDef *agg_def, TupleTableSlot *slot)
{
	vector_qual_state_init(&agg_state->vqual_state.vqstate, agg_def->filter_clauses, slot);
	return &agg_state->vqual_state.vqstate;
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

	/*
	 * Now we loop through the input compressed tuples, until they end or until
	 * the grouping policy asks us to emit partials.
	 */
	while (!grouping->gp_should_emit(grouping))
	{
		/*
		 * Get the next slot to aggregate. It will be either a compressed
		 * batch or an arrow tuple table slot. Both hold arrow arrays of data
		 * that can be vectorized.
		 */
		TupleTableSlot *slot = vector_agg_state->get_next_slot(vector_agg_state);

		/*
		 * Exit if there is no more data. Note that it is not possible to do
		 * the standard TupIsNull() check here because the compressed batch's
		 * implementation of TupleTableSlot never clears the empty flag bit
		 * (TTS_EMPTY), so it will always look empty. Therefore, look at the
		 * "input_ended" flag instead.
		 */
		if (vector_agg_state->input_ended)
			break;

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

			VectorQualState *vqstate =
				vector_agg_state->init_vector_quals(vector_agg_state, agg_def, slot);
			vector_qual_compute(vqstate);
			agg_def->filter_result = vqstate->vector_qual_result;
		}

		/*
		 * Finally, pass the compressed batch to the grouping policy.
		 */
		grouping->gp_add_batch(grouping, slot);
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
	CustomScan *childscan = castNode(CustomScan, linitial(cscan->custom_plans));

	state->custom.methods = &exec_methods;

	/*
	 * Initialize VectorAggState to process vector slots from different
	 * subnodes.
	 *
	 * VectorAgg supports two child nodes: ColumnarScan (producing arrow tuple
	 * table slots) and DecompressChunk (producing compressed batches).
	 *
	 * When the child is ColumnarScan, VectorAgg expects Arrow slots that
	 * carry arrow arrays. ColumnarScan performs standard qual filtering and
	 * vectorized qual filtering prior to handing the slot up to VectorAgg.
	 *
	 * When the child is DecompressChunk, VectorAgg doesn't read the slot from
	 * the child node. Instead, it bypasses DecompressChunk and reads
	 * compressed tuples directly from the grandchild. It therefore needs to
	 * handle batch decompression and vectorized qual filtering itself, in its
	 * own "get next slot" implementation.
	 *
	 * The vector qual init functions are needed to implement vectorized
	 * aggregate function FILTER clauses for arrow tuple table slots and
	 * compressed batches, respectively.
	 */
	if (is_columnar_scan(&childscan->scan.plan))
	{
		state->get_next_slot = arrow_get_next_slot;
		state->init_vector_quals = arrow_init_vector_quals;
	}
	else
	{
		Assert(strcmp(childscan->methods->CustomName, "DecompressChunk") == 0);
		state->get_next_slot = compressed_batch_get_next_slot;
		state->init_vector_quals = compressed_batch_init_vector_quals;
	}

	return (Node *) state;
}
