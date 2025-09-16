/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <funcapi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/optimizer.h>

#include "nodes/vector_agg/exec.h"

#include "compat/compat.h"
#include "compression/arrow_c_data_interface.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/vector_quals.h"
#include "nodes/vector_agg.h"
#include "nodes/vector_agg/plan.h"
#include "nodes/vector_agg/vector_slot.h"

#if PG18_GE
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif

static int
get_input_offset(const DecompressContext *dcontext, const Var *var)
{
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

/*
 * Create an arrow array with memory for buffers.
 *
 * The space for buffers are allocated after the main structure.
 */
static ArrowArray *
arrow_create_with_buffers(MemoryContext mcxt, int n_buffers)
{
	struct
	{
		ArrowArray array;
		const void *buffers[FLEXIBLE_ARRAY_MEMBER];
	} *array_with_buffers =
		MemoryContextAllocZero(mcxt, sizeof(ArrowArray) + (sizeof(const void *) * n_buffers));

	ArrowArray *array = &array_with_buffers->array;

	array->n_buffers = n_buffers;
	array->buffers = array_with_buffers->buffers;

	return array;
}

/*
 * Variable-size primitive layout ArrowArray from decompression iterator.
 */
static ArrowArray *
arrow_from_const_varlen(MemoryContext mcxt, int nrows, Datum value)
{
	const int value_bytes = VARSIZE_ANY_EXHDR(value);

	int32 *restrict offsets_buffer =
		MemoryContextAlloc(mcxt, pad_to_multiple(64, nrows * sizeof(*offsets_buffer)));
	for (int i = 0; i < nrows; i++)
	{
		offsets_buffer[i] = value_bytes * i;
	}

	uint8 *restrict data_buffer =
		MemoryContextAlloc(mcxt, pad_to_multiple(64, nrows * value_bytes));
	for (int i = 0; i < nrows; i++)
	{
		memcpy(data_buffer + value_bytes * i, DatumGetPointer(value), value_bytes);
	}

	ArrowArray *array = arrow_create_with_buffers(mcxt, 3);
	array->length = nrows;
	array->buffers[0] = NULL;
	array->buffers[1] = offsets_buffer;
	array->buffers[2] = data_buffer;

	return array;
}

/*
 * Fixed-Size Primitive layout ArrowArray from decompression iterator.
 */
static ArrowArray *
arrow_from_const_fixlen(MemoryContext mcxt, int nrows, Datum value, int16 typlen, bool typbyval)
{
	/* Just a precaution: this should not be a varlen type */
	Assert(typlen > 0);

	uint8 *restrict data_buffer = MemoryContextAlloc(mcxt, pad_to_multiple(64, nrows * typlen));
	for (int i = 0; i < nrows; i++)
	{
		if (typbyval)
		{
			/*
			 * We use unsigned integers to avoid conversions between signed
			 * and unsigned values (which in theory could change the value)
			 * when converting to datum (which is an unsigned value).
			 *
			 * Conversions between unsigned values is well-defined in the C
			 * standard and will work here.
			 */
			switch (typlen)
			{
				case sizeof(uint8):
					data_buffer[i] = DatumGetUInt8(value);
					break;
				case sizeof(uint16):
					((uint16 *) data_buffer)[i] = DatumGetUInt16(value);
					break;
				case sizeof(uint32):
					((uint32 *) data_buffer)[i] = DatumGetUInt32(value);
					break;
				case sizeof(uint64):
					/* This branch is not called for by-reference 64-bit values */
					((uint64 *) data_buffer)[i] = DatumGetUInt64(value);
					break;
				default:
					ereport(ERROR,
							errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("not supporting writing by value length %d", typlen));
			}
		}
		else
		{
			memcpy(&data_buffer[typlen * i], DatumGetPointer(value), typlen);
		}
	}

	ArrowArray *array = arrow_create_with_buffers(mcxt, 2);
	array->length = nrows;
	array->buffers[0] = NULL;
	array->buffers[1] = data_buffer;
	return array;
}

static ArrowArray *
arrow_from_const_bool(MemoryContext mcxt, int nrows, Datum value)
{
	const size_t words = (size_t) ((nrows + 63) / 64);
	uint64 *values = (uint64 *) MemoryContextAlloc(mcxt, words * sizeof(*values));
	memset(values, DatumGetBool(value) ? (uint8) -1 : 0, words * sizeof(*values));

	ArrowArray *array = arrow_create_with_buffers(mcxt, 2);
	array->length = nrows;
	array->buffers[0] = NULL;
	array->buffers[1] = values;
	return array;
}

static ArrowArray *
arrow_from_const_null(MemoryContext mcxt, int nrows)
{
	ArrowArray *array = arrow_create_with_buffers(mcxt, 2);
	array->length = nrows;
	array->null_count = nrows;

	const size_t words = (size_t) ((nrows + 63) / 64);
	uint64 *validity = (uint64 *) MemoryContextAllocZero(mcxt, words * sizeof(uint64));
	array->buffers[0] = validity;
	return array;
}

/*
 * Read the entire contents of a decompression iterator into the arrow array.
 */
static ArrowArray *
arrow_from_constant(MemoryContext mcxt, int nrows, const Const *c)
{
	int16 typlen;
	bool typbyval;
	get_typlenbyval(c->consttype, &typlen, &typbyval);

	if (c->constisnull)
	{
		return arrow_from_const_null(mcxt, nrows);
	}
	else if (c->consttype == BOOLOID)
	{
		return arrow_from_const_bool(mcxt, nrows, c->constvalue);
	}
	else if (typlen == -1)
	{
		return arrow_from_const_varlen(mcxt, nrows, c->constvalue);
	}
	else
	{
		return arrow_from_const_fixlen(mcxt, nrows, c->constvalue, typlen, typbyval);
	}
}

static CompressedColumnValues
evaluate_function(DecompressContext *dcontext, TupleTableSlot *slot, List *args, Oid funcoid,
				  Oid inputcollid)
{
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;

	Ensure(list_length(args) <= 5, "only <= 5 args supported");

	FmgrInfo flinfo;
	fmgr_info(funcoid, &flinfo);
	LOCAL_FCINFO(fcinfo, 5);
	InitFunctionCallInfoData(*fcinfo, &flinfo, list_length(args), inputcollid, NULL, NULL);

	const int nrows = batch_state->total_batch_rows;

	const ArrowArray *arrow_args[5];
	CompressedColumnValues arg_values[5];
	ListCell *lc;
	bool have_nulls = false;
	foreach (lc, args)
	{
		CompressedColumnValues arg =
			vector_slot_get_compressed_column_values(dcontext, slot, lfirst(lc));
		Ensure(arg.arrow != NULL, "no arrow for arg");
		arrow_args[foreach_current_index(lc)] = arg.arrow;
		have_nulls = (arg.arrow->null_count > 0) || have_nulls;
		arg_values[foreach_current_index(lc)] = arg;

		arg_values[foreach_current_index(lc)].output_value =
			&fcinfo->args[foreach_current_index(lc)].value;
		arg_values[foreach_current_index(lc)].output_isnull =
			&fcinfo->args[foreach_current_index(lc)].isnull;
	}

	Oid rettype = get_func_rettype(funcoid);
	int16 rettyplen;
	bool rettypbyval;
	get_typlenbyval(rettype, &rettyplen, &rettypbyval);

	if (!rettypbyval)
	{
		elog(ERROR, "only byval for now");
	}

	ArrowArray *arrow_result = MemoryContextAllocZero(batch_state->per_batch_context,
													  sizeof(ArrowArray) + 2 * sizeof(void *));
	arrow_result->length = nrows;
	arrow_result->buffers = (void *) &arrow_result[1];
	arrow_result->buffers[1] =
		MemoryContextAllocZero(batch_state->per_batch_context, rettyplen * nrows);

	// vector_function(arrow_args, list_length(f->args), arrow_result);

	for (int i = 0; i < nrows; i++)
	{
		compressed_columns_to_postgres_data(arg_values, 2, i);

		Datum result = FunctionCallInvoke(fcinfo);

		(void) result;

		switch (rettyplen)
		{
			case 2:
				((uint16 *restrict) arrow_result->buffers[1])[i] = DatumGetUInt16(result);
				break;
			case 4:
				((uint32 *restrict) arrow_result->buffers[1])[i] = DatumGetUInt32(result);
				break;
			case 8:
				((uint64 *restrict) arrow_result->buffers[1])[i] = DatumGetUInt64(result);
				//				fprintf(stderr, "rtl %d row %d result %ld\n", rettyplen, i,
				// DatumGetUInt64(result);
				break;
			default:
				elog(ERROR, "wrong size %d", rettyplen);
		}
	}

	/*
	 * Our functions are strict so we handle validity separately.
	 */
	if (have_nulls)
	{
		const size_t num_words = (nrows + 63) / 64;
		arrow_result->buffers[0] =
			MemoryContextAlloc(batch_state->per_batch_context, sizeof(uint64) * num_words);
		arrow_combine_validity(num_words,
							   (uint64 *) arrow_result->buffers[0],
							   (uint64 *) arrow_args[0]->buffers[0],
							   (uint64 *) arrow_args[1]->buffers[0],
							   NULL);
		arrow_result->null_count =
			arrow_result->length - arrow_num_valid(arrow_result->buffers[0], arrow_result->length);
	}

	CompressedColumnValues result = {
		.decompression_type = rettyplen,
		.buffers = { arrow_result->buffers[0], arrow_result->buffers[1] },
		.arrow = arrow_result,
	};

	return result;
}

/*
 * Return the arrow array or the datum (in case of single scalar value) for a
 * given attribute as a CompressedColumnValues struct.
 */
CompressedColumnValues
vector_slot_get_compressed_column_values(DecompressContext *dcontext, TupleTableSlot *slot,
										 const Expr *argument)
{
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	switch (((Node *) argument)->type)
	{
		case T_Const:
		{
			const Const *c = (const Const *) argument;
			ArrowArray *arrow_result = arrow_from_constant(batch_state->per_batch_context,
														   batch_state->total_batch_rows,
														   c);
			CompressedColumnValues result = {
				.decompression_type = get_typlen(c->consttype),
				.buffers = { arrow_result->buffers[0], arrow_result->buffers[1] },
				.arrow = arrow_result,
			};
			return result;
		}
		case T_Var:
		{
			const Var *var = (const Var *) argument;
			const uint16 offset =
				get_input_offset(dcontext, var); // AttrNumberGetAttrOffset(var->varattno);
			const CompressedColumnValues *values = &batch_state->compressed_columns[offset];
			return *values;
		}
		case T_OpExpr:
		{
			const OpExpr *o = (const OpExpr *) argument;
			return evaluate_function(dcontext, slot, o->args, o->opfuncid, o->inputcollid);
		}
		case T_FuncExpr:
		{
			const FuncExpr *f = (const FuncExpr *) argument;
			return evaluate_function(dcontext, slot, f->args, f->funcid, f->inputcollid);
		}
		default:
			fprintf(stderr, "%s\n", ts_get_node_name((Node *) argument));
			Ensure(false,
				   "wrong node type %s for vector expression",
				   ts_get_node_name((Node *) argument));
	}
}

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));

	VectorAggState *vector_agg_state = (VectorAggState *) node;
	vector_agg_state->input_ended = false;

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

				def->argument = castNode(TargetEntry, linitial(aggref->args))->expr;
				//				Var *var = castNode(Var, ;
				//				def->input_offset =
				//					get_input_offset((const DecompressChunkState *) childstate,
				// var);
			}
			else
			{
				def->argument = NULL;
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

			GroupingColumn *col = &vector_agg_state->grouping_columns[grouping_column_counter++];
			col->expr = tlentry->expr;
			col->output_offset = i;

			TupleDesc tdesc = NULL;
			Oid type = InvalidOid;
			TypeFuncClass type_class = get_expr_result_type((Node *) tlentry->expr, &type, &tdesc);
			Ensure(type_class == TYPEFUNC_SCALAR,
				   "wrong grouping column type class %d",
				   type_class);
			get_typlenbyval(type, &col->value_bytes, &col->by_value);
		}
	}

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

		if (dcontext->ps->instrument)
		{
			/*
			 * Ensure proper EXPLAIN output for the underlying DecompressChunk
			 * node.
			 *
			 * This value is normally updated by InstrStopNode(), and is
			 * required so that the calculations in InstrEndLoop() run properly.
			 * We have to call it manually because we run the underlying
			 * DecompressChunk manually and not as a normal Postgres node.
			 */
			dcontext->ps->instrument->running = true;
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
		 * Ensure proper EXPLAIN output for the underlying DecompressChunk
		 * node.
		 *
		 * This value is normally updated by InstrStopNode(), and is
		 * required so that the calculations in InstrEndLoop() run properly.
		 * We have to call it manually because we run the underlying
		 * DecompressChunk manually and not as a normal Postgres node.
		 */
		dcontext->ps->instrument->tuplecount += not_filtered_rows;
	}

	return &batch_state->decompressed_scan_slot_data.base;
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

static TupleTableSlot *
vector_agg_exec(CustomScanState *node)
{
	VectorAggState *vector_agg_state = (VectorAggState *) node;

	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom.custom_ps);
	DecompressContext *dcontext = &decompress_state->decompress_context;

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
		grouping->gp_add_batch(grouping, dcontext, slot);
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
	Assert(ts_is_decompress_chunk_plan((Plan *) linitial(cscan->custom_plans)));

	state->custom.methods = &exec_methods;

	/*
	 * Initialize VectorAggState to process vector slots from different
	 * subnodes.
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
	state->get_next_slot = compressed_batch_get_next_slot;
	state->init_vector_quals = compressed_batch_init_vector_quals;

	return (Node *) state;
}
