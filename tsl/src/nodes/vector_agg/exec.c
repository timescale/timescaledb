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
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"
#include "guc.h"

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));
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
}

typedef struct
{
	void (*agg_init)(Datum *agg_value, bool *agg_isnull);
	void (*agg_vector)(ArrowArray *vector, uint64 *filter, Datum *agg_value, bool *agg_isnull);
	void (*agg_const)(Datum constvalue, bool constisnull, int n, Datum *agg_value,
					  bool *agg_isnull);
} VectorAggregate;

static void
int4_sum_init(Datum *agg_value, bool *agg_isnull)
{
	*agg_value = Int64GetDatum(0);
	*agg_isnull = true;
}

static void
int4_sum_vector(ArrowArray *vector, uint64 *filter, Datum *agg_value, bool *agg_isnull)
{
	Assert(vector != NULL);
	Assert(vector->length > 0);

	/*
	 * We accumulate the sum as int64, so we can sum INT_MAX = 2^31 - 1
	 * at least 2^31 times without incurring an overflow of the int64
	 * accumulator. The same is true for negative numbers. The
	 * compressed batch size is currently capped at 1000 rows, but even
	 * if it's changed in the future, it's unlikely that we support
	 * batches larger than 65536 rows, not to mention 2^31. Therefore,
	 * we don't need to check for overflows within the loop, which would
	 * slow down the calculation.
	 */
	Assert(vector->length <= INT_MAX);

	int64 batch_sum = 0;

	/*
	 * This loop is not unrolled automatically, so do it manually as usual.
	 * The value buffer is padded to an even multiple of 64 bytes, i.e. to
	 * 64 / 4 = 16 elements. The bitmap is an even multiple of 64 elements.
	 * The number of elements in the inner loop must be less than both these
	 * values so that we don't go out of bounds. The particular value was
	 * chosen because it gives some speedup, and the larger values blow up
	 * the generated code with no performance benefit (checked on clang 16).
	 */
#define INNER_LOOP_SIZE 4
	const int outer_boundary = pad_to_multiple(INNER_LOOP_SIZE, vector->length);
	for (int outer = 0; outer < outer_boundary; outer += INNER_LOOP_SIZE)
	{
		for (int inner = 0; inner < INNER_LOOP_SIZE; inner++)
		{
			const int row = outer + inner;
			const int32 arrow_value = ((int32 *) vector->buffers[1])[row];
			const bool passes_filter = filter ? arrow_row_is_valid(filter, row) : true;
			batch_sum += passes_filter * arrow_value * arrow_row_is_valid(vector->buffers[0], row);
		}
	}
#undef INNER_LOOP_SIZE

	int64 tmp = DatumGetInt64(*agg_value);
	if (unlikely(pg_add_s64_overflow(tmp, batch_sum, &tmp)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}
	*agg_value = Int64GetDatum(tmp);

	*agg_isnull = false;
}

static void
int4_sum_const(Datum constvalue, bool constisnull, int n, Datum *agg_value, bool *agg_isnull)
{
	Assert(n > 0);

	if (constisnull)
	{
		return;
	}

	int32 intvalue = DatumGetInt32(constvalue);
	int64 batch_sum = 0;

	/* We have at least one value */
	*agg_isnull = false;

	/* Multiply the number of tuples with the actual value */
	if (unlikely(pg_mul_s64_overflow(intvalue, n, &batch_sum)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	/* Add the value to our sum */
	int64 tmp = DatumGetInt64(*agg_value);
	if (unlikely(pg_add_s64_overflow(tmp, batch_sum, &tmp)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}
	*agg_value = Int64GetDatum(tmp);
}

static VectorAggregate int4_sum_agg = {
	.agg_init = int4_sum_init,
	.agg_const = int4_sum_const,
	.agg_vector = int4_sum_vector,
};

static TupleTableSlot *
vector_agg_exec(CustomScanState *vector_agg_state)
{
	DecompressChunkState *decompress_state =
		(DecompressChunkState *) linitial(vector_agg_state->custom_ps);

	/*
	 * The aggregated targetlist with Aggrefs is in the custom scan targetlist
	 * of the custom scan node that is performing the vectorized aggregation.
	 * We do this to avoid projections at this node, because the postgres
	 * projection functions complain when they see an Aggref in a custom
	 * node output targetlist.
	 * The output targetlist, in turn, consists of just the INDEX_VAR references
	 * into the custom_scan_tlist.
	 */
	List *aggregated_tlist = castNode(CustomScan, vector_agg_state->ss.ps.plan)->custom_scan_tlist;
	Assert(list_length(aggregated_tlist) == 1);

	/* Checked by planner */
	Assert(ts_guc_enable_vectorized_aggregation);
	Assert(ts_guc_enable_bulk_decompression);

	/* Determine which kind of vectorized aggregation we should perform */
	TargetEntry *tlentry = (TargetEntry *) linitial(aggregated_tlist);
	Assert(IsA(tlentry->expr, Aggref));
	Aggref *aggref = castNode(Aggref, tlentry->expr);

	my_print(aggref);

	/* Partial result is a int8 */
	Assert(aggref->aggtranstype == INT8OID);

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

	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	/* Get a reference the the output TupleTableSlot */
	TupleTableSlot *aggregated_slot = vector_agg_state->ss.ps.ps_ResultTupleSlot;
	Assert(aggregated_slot->tts_tupleDescriptor->natts == 1);

	Assert(aggref->aggfnoid == F_SUM_INT4);
	VectorAggregate *agg = &int4_sum_agg;

	agg->agg_init(&aggregated_slot->tts_values[0], &aggregated_slot->tts_isnull[0]);
	ExecClearTuple(aggregated_slot);

	TupleTableSlot *compressed_slot = ExecProcNode(linitial(decompress_state->csstate.custom_ps));

	if (TupIsNull(compressed_slot))
	{
		/* All values are processed. */
		return NULL;
	}

	compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);

	ArrowArray *arrow = NULL;
	if (value_column_description->type == COMPRESSED_COLUMN)
	{
		Assert(dcontext->enable_bulk_decompression);
		Assert(value_column_description->bulk_decompression_supported);
		CompressedColumnValues *values =
			&batch_state->compressed_columns[value_column_description - dcontext->template_columns];
		Assert(values->decompression_type != DT_Invalid);
		arrow = values->arrow;
	}
	else
	{
		Assert(value_column_description->type == SEGMENTBY_COLUMN);
	}

	if (arrow == NULL)
	{
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

		int offs = AttrNumberGetAttrOffset(value_column_description->output_attno);
		agg->agg_const(batch_state->decompressed_scan_slot_data.base.tts_values[offs],
					   batch_state->decompressed_scan_slot_data.base.tts_isnull[offs],
					   n,
					   &aggregated_slot->tts_values[0],
					   &aggregated_slot->tts_isnull[0]);
	}
	else
	{
		agg->agg_vector(arrow,
						batch_state->vector_qual_result,
						&aggregated_slot->tts_values[0],
						&aggregated_slot->tts_isnull[0]);
	}

	compressed_batch_discard_tuples(batch_state);
	/* Use Int64GetDatum to store the result since a 64-bit value is not pass-by-value on 32-bit
	 * systems */
	ExecStoreVirtualTuple(aggregated_slot);

	return aggregated_slot;
}

static void
vector_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* noop? */
}

static struct CustomExecMethods exec_methods = {
	.CustomName = "VectorAgg",
	.BeginCustomScan = vector_agg_begin,
	.ExecCustomScan = vector_agg_exec,
	.EndCustomScan = vector_agg_end,
	.ReScanCustomScan = vector_agg_rescan,
	.ExplainCustomScan = vector_agg_explain,
};

Node *
vector_agg_state_create(CustomScan *cscan)
{
	CustomScanState *state = makeNode(CustomScanState);
	state->methods = &exec_methods;
	return (Node *) state;
}
