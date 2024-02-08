/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/bitmapset.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "debug_assert.h"
#include "guc.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/vector_predicates.h"

/*
 * Create a single value ArrowArray from Postgres Datum. This is used to run
 * the usual vectorized predicates on compressed columns with default values.
 */
static ArrowArray *
make_single_value_arrow(Oid pgtype, Datum datum, bool isnull)
{
	struct ArrowWithBuffers
	{
		ArrowArray arrow;
		uint64 buffers[2];
		uint64 nulls_buffer;
		uint64 values_buffer;
	};

	struct ArrowWithBuffers *with_buffers = palloc0(sizeof(struct ArrowWithBuffers));
	ArrowArray *arrow = &with_buffers->arrow;
	arrow->length = 1;
	arrow->null_count = -1;
	arrow->n_buffers = 2;
	arrow->buffers = (const void **) &with_buffers->buffers;
	arrow->buffers[0] = &with_buffers->nulls_buffer;
	arrow->buffers[1] = &with_buffers->values_buffer;

	if (isnull)
	{
		/*
		 * The validity bitmap was initialized to invalid on allocation, and
		 * the Datum might be invalid if the value is null (important on i386
		 * where it might be pass-by-reference), so don't read it.
		 */
		return arrow;
	}

#define FOR_TYPE(PGTYPE, CTYPE, FROMDATUM)                                                         \
	case PGTYPE:                                                                                   \
		*((CTYPE *) &with_buffers->values_buffer) = FROMDATUM(datum);                              \
		break

	switch (pgtype)
	{
		FOR_TYPE(INT8OID, int64, DatumGetInt64);
		FOR_TYPE(INT4OID, int32, DatumGetInt32);
		FOR_TYPE(INT2OID, int16, DatumGetInt16);
		FOR_TYPE(FLOAT8OID, float8, DatumGetFloat8);
		FOR_TYPE(FLOAT4OID, float4, DatumGetFloat4);
		FOR_TYPE(TIMESTAMPTZOID, TimestampTz, DatumGetTimestampTz);
		FOR_TYPE(TIMESTAMPOID, Timestamp, DatumGetTimestamp);
		FOR_TYPE(DATEOID, DateADT, DatumGetDateADT);
		default:
			elog(ERROR, "unexpected column type '%s'", format_type_be(pgtype));
			pg_unreachable();
	}

	arrow_set_row_validity(&with_buffers->nulls_buffer, 0, true);

	return arrow;
}

static int
get_max_text_datum_size(ArrowArray *text_array)
{
	int maxbytes = 0;
	uint32 *offsets = (uint32 *) text_array->buffers[1];
	for (int i = 0; i < text_array->length; i++)
	{
		const int curbytes = offsets[i + 1] - offsets[i];
		if (curbytes > maxbytes)
		{
			maxbytes = curbytes;
		}
	}

	return maxbytes;
}

static void
decompress_column(DecompressContext *dcontext, DecompressBatchState *batch_state, int i)
{
	CompressionColumnDescription *column_description = &dcontext->template_columns[i];
	CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
	column_values->arrow = NULL;
	column_values->output_value =
		&batch_state->decompressed_scan_slot->tts_values[column_description->scan_column_index];
	column_values->output_isnull =
		&batch_state->decompressed_scan_slot->tts_isnull[column_description->scan_column_index];
	const int value_bytes = get_typlen(column_description->typid);
	Assert(value_bytes != 0);

	bool isnull;
	Datum value = slot_getattr(batch_state->compressed_slot,
							   column_description->compressed_scan_attno,
							   &isnull);

	if (isnull)
	{
		/*
		 * The column will have a default value for the entire batch,
		 * set it now.
		 */
		column_values->decompression_type = DT_Default;

		batch_state->decompressed_scan_slot->tts_values[column_description->scan_column_index] =
			getmissingattr(dcontext->uncompressed_chunk_tupdesc,
						   column_description->uncompressed_chunk_attno,
						   &batch_state->decompressed_scan_slot
								->tts_isnull[column_description->scan_column_index]);
		return;
	}

	/* Detoast the compressed datum. */
	value = PointerGetDatum(
		detoaster_detoast_attr((struct varlena *) DatumGetPointer(value), &dcontext->detoaster));

	/* Decompress the entire batch if it is supported. */
	CompressedDataHeader *header = (CompressedDataHeader *) value;
	ArrowArray *arrow = NULL;
	if (dcontext->enable_bulk_decompression && column_description->bulk_decompression_supported)
	{
		if (dcontext->bulk_decompression_context == NULL)
		{
			dcontext->bulk_decompression_context = create_bulk_decompression_mctx(
				MemoryContextGetParent(batch_state->per_batch_context));
		}

		DecompressAllFunction decompress_all =
			tsl_get_decompress_all_function(header->compression_algorithm,
											column_description->typid);
		Assert(decompress_all != NULL);

		MemoryContext context_before_decompression =
			MemoryContextSwitchTo(dcontext->bulk_decompression_context);

		arrow = decompress_all(PointerGetDatum(header),
							   column_description->typid,
							   batch_state->per_batch_context);

		MemoryContextReset(dcontext->bulk_decompression_context);

		MemoryContextSwitchTo(context_before_decompression);
	}

	if (arrow == NULL)
	{
		/* As a fallback, decompress row-by-row. */
		column_values->decompression_type = DT_Iterator;
		column_values->buffers[0] =
			tsl_get_decompression_iterator_init(header->compression_algorithm,
												dcontext->reverse)(PointerGetDatum(header),
																   column_description->typid);
		return;
	}

	/* Should have been filled from the count metadata column. */
	Assert(batch_state->total_batch_rows != 0);
	if (batch_state->total_batch_rows != arrow->length)
	{
		elog(ERROR, "compressed column out of sync with batch counter");
	}

	column_values->arrow = arrow;

	if (value_bytes > 0)
	{
		/* Fixed-width column. */
		column_values->decompression_type = value_bytes;
		column_values->buffers[0] = arrow->buffers[0];
		column_values->buffers[1] = arrow->buffers[1];
	}
	else
	{
		/*
		 * Text column. Pre-allocate memory for its text Datum in the
		 * decompressed scan slot. We can't put direct references to Arrow
		 * memory there, because it doesn't have the varlena headers that
		 * Postgres expects for text.
		 */
		const int maxbytes =
			VARHDRSZ + (arrow->dictionary ? get_max_text_datum_size(arrow->dictionary) :
											get_max_text_datum_size(arrow));

		*column_values->output_value =
			PointerGetDatum(MemoryContextAlloc(batch_state->per_batch_context, maxbytes));

		/*
		 * Set up the datum conversion based on whether we use the dictionary.
		 */
		if (arrow->dictionary == NULL)
		{
			column_values->decompression_type = DT_ArrowText;
			column_values->buffers[0] = arrow->buffers[0];
			column_values->buffers[1] = arrow->buffers[1];
			column_values->buffers[2] = arrow->buffers[2];
		}
		else
		{
			column_values->decompression_type = DT_ArrowTextDict;
			column_values->buffers[0] = arrow->buffers[0];
			column_values->buffers[1] = arrow->dictionary->buffers[1];
			column_values->buffers[2] = arrow->dictionary->buffers[2];
			column_values->buffers[3] = arrow->buffers[1];
		}
	}
}

static void
compute_plain_qual(DecompressContext *dcontext, DecompressBatchState *batch_state, Node *qual,
				   uint64 *restrict result)
{
	/*
	 * Some predicates can be evaluated to a Const at run time.
	 */
	if (IsA(qual, Const))
	{
		Const *c = castNode(Const, qual);
		if (c->constisnull || !DatumGetBool(c->constvalue))
		{
			/*
			 * Some predicates are evaluated to a null Const, like a
			 * strict comparison with stable expression that evaluates to null.
			 * No rows pass.
			 */
			const size_t n_batch_result_words = (batch_state->total_batch_rows + 63) / 64;
			for (size_t i = 0; i < n_batch_result_words; i++)
			{
				result[i] = 0;
			}
		}
		else
		{
			/*
			 * This is a constant true qual, every row passes and we can
			 * just ignore it. No idea how it can happen though.
			 */
			Assert(false);
		}
		return;
	}

	/*
	 * For now, we support NullTest, "Var ? Const" predicates and
	 * ScalarArrayOperations.
	 */
	List *args = NULL;
	RegProcedure vector_const_opcode = InvalidOid;
	ScalarArrayOpExpr *saop = NULL;
	OpExpr *opexpr = NULL;
	NullTest *nulltest = NULL;
	if (IsA(qual, NullTest))
	{
		nulltest = castNode(NullTest, qual);
		args = list_make1(nulltest->arg);
	}
	else if (IsA(qual, ScalarArrayOpExpr))
	{
		saop = castNode(ScalarArrayOpExpr, qual);
		args = saop->args;
		vector_const_opcode = get_opcode(saop->opno);
	}
	else
	{
		Ensure(IsA(qual, OpExpr), "expected OpExpr");
		opexpr = castNode(OpExpr, qual);
		args = opexpr->args;
		vector_const_opcode = get_opcode(opexpr->opno);
	}

	/*
	 * Find the compressed column referred to by the Var.
	 */
	Var *var = castNode(Var, linitial(args));
	CompressionColumnDescription *column_description = NULL;
	int column_index = 0;
	for (; column_index < dcontext->num_total_columns; column_index++)
	{
		column_description = &dcontext->template_columns[column_index];
//		fprintf(stderr, "column %d type %d scan index %d ucattno %d\n",
//			column_index,
//			column_description->type,
//			column_description->scan_column_index,
//			column_description->uncompressed_chunk_attno);
		if (column_description->uncompressed_chunk_attno == var->varattno)
		// if (column_description->scan_column_index + 1 == var->varattno)
		{
			break;
		}
	}
//	if (column_index == dcontext->num_total_columns)
//	{
//		fprintf(stderr, "%d total columns\n", dcontext->num_total_columns);
//		my_print(var);
//	}
	Ensure(column_index < dcontext->num_total_columns,
		   "decompressed column %d not found in batch",
		   var->varattno);
	Assert(column_description != NULL);
	Assert(column_description->typid == var->vartype);
	Ensure(column_description->type == COMPRESSED_COLUMN,
		   "only compressed columns are supported in vectorized quals");
	Assert(column_index < dcontext->num_compressed_columns);

	CompressedColumnValues *column_values = &batch_state->compressed_columns[column_index];

	if (column_values->decompression_type == DT_Invalid)
	{
		/*
		 * We decompress the compressed columns on demand, so that we can
		 * skip decompressing some columns if the entire batch doesn't pass
		 * the quals.
		 */
		decompress_column(dcontext, batch_state, column_index);
		Assert(column_values->decompression_type != DT_Invalid);
	}

	Assert(column_values->decompression_type != DT_Iterator);

	/*
	 * Prepare to compute the vector predicate. We have to handle the
	 * default values in a special way because they don't produce the usual
	 * decompressed ArrowArrays.
	 */
	uint64 default_value_predicate_result[1];
	uint64 *predicate_result = result;
	const ArrowArray *vector = column_values->arrow;
	if (column_values->arrow == NULL)
	{
		/*
		 * The compressed column had a default value. We can't fall back to
		 * the non-vectorized quals now, so build a single-value ArrowArray
		 * with this default value, check if it passes the predicate, and apply
		 * it to the entire batch.
		 */
		Assert(column_values->decompression_type == DT_Default);

		/*
		 * We saved the actual default value into the decompressed scan slot
		 * above, so pull it from there.
		 */
		vector = make_single_value_arrow(column_description->typid,
										 *column_values->output_value,
										 *column_values->output_isnull);

		/*
		 * We start from an all-valid bitmap, because the predicate is
		 * AND-ed to it.
		 */
		default_value_predicate_result[0] = 1;
		predicate_result = default_value_predicate_result;
	}

	if (nulltest)
	{
		vector_nulltest(vector, nulltest->nulltesttype, predicate_result);
	}
	else
	{
		/*
		 * Find the vector_const predicate.
		 */
		VectorPredicate *vector_const_predicate = get_vector_const_predicate(vector_const_opcode);
		Assert(vector_const_predicate != NULL);

		Ensure(IsA(lsecond(args), Const),
			   "failed to evaluate runtime constant in vectorized filter");

		/*
		 * The vectorizable predicates should be STRICT, so we shouldn't see null
		 * constants here.
		 */
		Const *constnode = castNode(Const, lsecond(args));
		Ensure(!constnode->constisnull, "vectorized predicate called for a null value");

		/*
		 * At last, compute the predicate.
		 */
		if (saop)
		{
			vector_array_predicate(vector_const_predicate,
								   saop->useOr,
								   vector,
								   constnode->constvalue,
								   predicate_result);
		}
		else
		{
			vector_const_predicate(vector, constnode->constvalue, predicate_result);
		}

		/*
		 * Account for nulls which shouldn't pass the predicate. Note that the
		 * vector here might have only one row, in contrast with the number of
		 * rows in the batch, if the column has a default value in this batch.
		 */
		const size_t n_vector_result_words = (vector->length + 63) / 64;
		Assert((predicate_result != default_value_predicate_result) ||
			   n_vector_result_words == 1); /* to placate Coverity. */
		const uint64 *restrict validity = (uint64 *restrict) vector->buffers[0];
		for (size_t i = 0; i < n_vector_result_words; i++)
		{
			predicate_result[i] &= validity[i];
		}
	}

	/* Translate the result if the column had a default value. */
	if (column_values->arrow == NULL)
	{
		Assert(column_values->decompression_type == DT_Default);
		if (!(default_value_predicate_result[0] & 1))
		{
			/*
			 * We had a default value for the compressed column, and it
			 * didn't pass the predicate, so the entire batch didn't pass.
			 */
			const size_t n_batch_result_words = (batch_state->total_batch_rows + 63) / 64;
			for (size_t i = 0; i < n_batch_result_words; i++)
			{
				result[i] = 0;
			}
		}
	}
}

static void compute_one_qual(DecompressContext *dcontext, DecompressBatchState *batch_state,
							 Node *qual, uint64 *restrict result);

static void
compute_qual_conjunction(DecompressContext *dcontext, DecompressBatchState *batch_state,
						 List *quals, uint64 *restrict result)
{
	ListCell *lc;
	foreach (lc, quals)
	{
		compute_one_qual(dcontext, batch_state, lfirst(lc), result);
		if (get_vector_qual_summary(result, batch_state->total_batch_rows) == NoRowsPass)
		{
			/*
			 * Exit early if no rows pass already. This might allow us to avoid
			 * reading the columns required for the subsequent quals.
			 */
			return;
		}
	}
}

static void
compute_qual_disjunction(DecompressContext *dcontext, DecompressBatchState *batch_state,
						 List *quals, uint64 *restrict result)
{
	const size_t n_rows = batch_state->total_batch_rows;
	const size_t n_result_words = (n_rows + 63) / 64;
	uint64 *or_result = palloc(sizeof(uint64) * n_result_words);
	for (size_t i = 0; i < n_result_words; i++)
	{
		or_result[i] = 0;
	}

	uint64 *one_qual_result = palloc(sizeof(uint64) * n_result_words);

	ListCell *lc;
	foreach (lc, quals)
	{
		for (size_t i = 0; i < n_result_words; i++)
		{
			one_qual_result[i] = (uint64) -1;
		}
		compute_one_qual(dcontext, batch_state, lfirst(lc), one_qual_result);
		for (size_t i = 0; i < n_result_words; i++)
		{
			or_result[i] |= one_qual_result[i];
		}

		if (get_vector_qual_summary(or_result, n_rows) == AllRowsPass)
		{
			/*
			 * We can sometimes avoing reading the columns required for the
			 * rest of conditions if we break out early here.
			 */
			return;
		}
	}

	for (size_t i = 0; i < n_result_words; i++)
	{
		result[i] &= or_result[i];
	}
}

static void
compute_one_qual(DecompressContext *dcontext, DecompressBatchState *batch_state, Node *qual,
				 uint64 *restrict result)
{
	if (!IsA(qual, BoolExpr))
	{
		compute_plain_qual(dcontext, batch_state, qual, result);
		return;
	}

	BoolExpr *boolexpr = castNode(BoolExpr, qual);
	if (boolexpr->boolop == AND_EXPR)
	{
		compute_qual_conjunction(dcontext, batch_state, boolexpr->args, result);
		return;
	}

	/*
	 * Postgres removes NOT for operators we can vectorize, so we don't support
	 * NOT and consider it non-vectorizable at planning time. So only OR is left.
	 */
	Ensure(boolexpr->boolop == OR_EXPR, "expected OR");
	compute_qual_disjunction(dcontext, batch_state, boolexpr->args, result);
}

/*
 * Compute the vectorized filters. Returns true if we have any passing rows. If not,
 * it means the entire batch is filtered out, and we use this for further
 * optimizations.
 */
static VectorQualSummary
compute_vector_quals(DecompressContext *dcontext, DecompressBatchState *batch_state)
{
	/*
	 * Allocate the bitmap that will hold the vectorized qual results. We will
	 * initialize it to all ones and AND the individual quals to it.
	 */
	const size_t n_rows = batch_state->total_batch_rows;
	const int bitmap_bytes = sizeof(uint64) * ((n_rows + 63) / 64);
	batch_state->vector_qual_result = palloc(bitmap_bytes);
	memset(batch_state->vector_qual_result, 0xFF, bitmap_bytes);
	if (n_rows % 64 != 0)
	{
		/*
		 * We have to zero out the bits for past-the-end elements in the last
		 * bitmap word. Since all predicates are ANDed to the result bitmap,
		 * we can do it here once instead of doing it in each predicate.
		 */
		const uint64 mask = ((uint64) -1) >> (64 - batch_state->total_batch_rows % 64);
		batch_state->vector_qual_result[batch_state->total_batch_rows / 64] = mask;
	}

	/*
	 * Compute the quals.
	 */
	compute_qual_conjunction(dcontext,
							 batch_state,
							 dcontext->vectorized_quals_constified,
							 batch_state->vector_qual_result);

	return get_vector_qual_summary(batch_state->vector_qual_result, n_rows);
}

/*
 * Initialize the batch decompression state with the new compressed  tuple.
 */
void
compressed_batch_set_compressed_tuple(DecompressContext *dcontext,
									  DecompressBatchState *batch_state, TupleTableSlot *subslot)
{
	Assert(TupIsNull(batch_state->decompressed_scan_slot));

	/*
	 * The batch states are initialized on demand, because creating the memory
	 * context and the tuple table slots is expensive.
	 */
	if (batch_state->per_batch_context == NULL)
	{
		/* Init memory context */
		batch_state->per_batch_context =
			create_per_batch_mctx(dcontext->batch_memory_context_bytes);
		Assert(batch_state->per_batch_context != NULL);

		Assert(batch_state->compressed_slot == NULL);

		/* Create a non ref-counted copy of the tuple descriptor */
		if (dcontext->compressed_slot_tdesc == NULL)
			dcontext->compressed_slot_tdesc =
				CreateTupleDescCopyConstr(subslot->tts_tupleDescriptor);
		Assert(dcontext->compressed_slot_tdesc->tdrefcount == -1);

		batch_state->compressed_slot =
			MakeSingleTupleTableSlot(dcontext->compressed_slot_tdesc, subslot->tts_ops);

		Assert(batch_state->decompressed_scan_slot == NULL);

		/* Get a reference the the output TupleTableSlot */
		TupleTableSlot *slot = dcontext->decompressed_slot;

		/* Create a non ref-counted copy of the tuple descriptor */
		if (dcontext->decompressed_slot_scan_tdesc == NULL)
			dcontext->decompressed_slot_scan_tdesc =
				CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
		Assert(dcontext->decompressed_slot_scan_tdesc->tdrefcount == -1);

		batch_state->decompressed_scan_slot =
			MakeSingleTupleTableSlot(dcontext->decompressed_slot_scan_tdesc, slot->tts_ops);
	}
	else
	{
		Assert(batch_state->compressed_slot != NULL);
		Assert(batch_state->decompressed_scan_slot != NULL);
	}

	/* Ensure that all fields are empty. Calling ExecClearTuple is not enough
	 * because some attributes might not be populated (e.g., due to a dropped
	 * column) and these attributes need to be set to null. */
	ExecStoreAllNullTuple(batch_state->decompressed_scan_slot);
	ExecClearTuple(batch_state->decompressed_scan_slot);

	ExecCopySlot(batch_state->compressed_slot, subslot);
	Assert(!TupIsNull(batch_state->compressed_slot));

	batch_state->total_batch_rows = 0;
	batch_state->next_batch_row = 0;

	MemoryContext old_context = MemoryContextSwitchTo(batch_state->per_batch_context);
	MemoryContextReset(batch_state->per_batch_context);

	for (int i = 0; i < dcontext->num_total_columns; i++)
	{
		CompressionColumnDescription *column_description = &dcontext->template_columns[i];

		switch (column_description->type)
		{
			case COMPRESSED_COLUMN:
			{
				Assert(i < dcontext->num_compressed_columns);
				/*
				 * We decompress the compressed columns on demand, so that we can
				 * skip decompressing some columns if the entire batch doesn't pass
				 * the quals. Skip them for now.
				 */
				CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
				column_values->decompression_type = DT_Invalid;
				column_values->arrow = NULL;
				break;
			}
			case SEGMENTBY_COLUMN:
			{
				/*
				 * A segmentby column is not going to change during one batch,
				 * and our output tuples are read-only, so it's enough to only
				 * save it once per batch, which we do here.
				 */
				batch_state->decompressed_scan_slot
					->tts_values[column_description->scan_column_index] =
					slot_getattr(batch_state->compressed_slot,
								 column_description->compressed_scan_attno,
								 &batch_state->decompressed_scan_slot
									  ->tts_isnull[column_description->scan_column_index]);
				break;
			}
			case COUNT_COLUMN:
			{
				bool isnull;
				Datum value = slot_getattr(batch_state->compressed_slot,
										   column_description->compressed_scan_attno,
										   &isnull);
				/* count column should never be NULL */
				Assert(!isnull);
				int count_value = DatumGetInt32(value);
				if (count_value <= 0)
				{
					ereport(ERROR,
							(errmsg("the compressed data is corrupt: got a segment with length %d",
									count_value)));
				}

				Assert(batch_state->total_batch_rows == 0);
				CheckCompressedData(count_value <= UINT16_MAX);
				batch_state->total_batch_rows = count_value;

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

	VectorQualSummary vector_qual_summary = dcontext->vectorized_quals_constified != NIL ?
												compute_vector_quals(dcontext, batch_state) :
												AllRowsPass;
	if (vector_qual_summary == NoRowsPass && !dcontext->batch_sorted_merge)
	{
		/*
		 * The entire batch doesn't pass the vectorized quals, so we might be
		 * able to avoid reading and decompressing other columns. Scroll it to
		 * the end.
		 * Note that this optimization can't work with "batch sorted merge",
		 * because the latter always has to read the first row of the batch for
		 * its sorting needs, so it always has to read and decompress all
		 * columns. This can be improved by only decompressing the columns
		 * needed for sorting.
		 */
		batch_state->next_batch_row = batch_state->total_batch_rows;

		InstrCountTuples2(dcontext->ps, 1);
		InstrCountFiltered1(dcontext->ps, batch_state->total_batch_rows);
	}
	else
	{
		/*
		 * We have some rows in the batch that pass the vectorized filters, so
		 * we have to decompress the rest of the compressed columns.
		 */
		const int num_compressed_columns = dcontext->num_compressed_columns;
		for (int i = 0; i < num_compressed_columns; i++)
		{
			CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
			if (column_values->decompression_type == DT_Invalid)
			{
				decompress_column(dcontext, batch_state, i);
				Assert(column_values->decompression_type != DT_Invalid);
			}
		}

		/*
		 * If all rows pass, no need to test the vector qual for each row. This
		 * is a common case for time range conditions.
		 */
		if (vector_qual_summary == AllRowsPass)
		{
			batch_state->vector_qual_result = NULL;
		}
	}

	MemoryContextSwitchTo(old_context);
}

static void
store_text_datum(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	const int total_bytes = value_bytes + VARHDRSZ;
	Assert(DatumGetPointer(*column_values->output_value) != NULL);
	SET_VARSIZE(*column_values->output_value, total_bytes);
	memcpy(VARDATA(*column_values->output_value),
		   &((uint8 *) column_values->buffers[2])[start],
		   value_bytes);
}

/*
 * Construct the next tuple in the decompressed scan slot.
 * Doesn't check the quals.
 */
static void
make_next_tuple(DecompressBatchState *batch_state, uint16 arrow_row, int num_compressed_columns)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->next_batch_row < batch_state->total_batch_rows);

	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
		if (column_values->decompression_type == DT_Iterator)
		{
			DecompressionIterator *iterator = (DecompressionIterator *) column_values->buffers[0];
			DecompressResult result = iterator->try_next(iterator);

			if (result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}

			*column_values->output_isnull = result.is_null;
			*column_values->output_value = result.val;
		}
		else if (column_values->decompression_type > SIZEOF_DATUM)
		{
			/*
			 * Fixed-width by-reference type that doesn't fit into a Datum.
			 * For now this only happens for 8-byte types on 32-bit systems,
			 * but eventually we could also use it for bigger by-value types
			 * such as UUID.
			 */
			const uint8 value_bytes = column_values->decompression_type;
			const char *restrict src = column_values->buffers[1];
			*column_values->output_value = PointerGetDatum(&src[value_bytes * arrow_row]);
			*column_values->output_isnull =
				!arrow_row_is_valid(column_values->buffers[0], arrow_row);
		}
		else if (column_values->decompression_type > 0)
		{
			/*
			 * Fixed-width by-value type that fits into a Datum.
			 *
			 * The conversion of Datum to more narrow types will truncate
			 * the higher bytes, so we don't care if we read some garbage
			 * into them, and can always read 8 bytes. These are unaligned
			 * reads, so technically we have to do memcpy.
			 */
			const uint8 value_bytes = column_values->decompression_type;
			Assert(value_bytes <= SIZEOF_DATUM);
			const char *restrict src = column_values->buffers[1];
			memcpy(column_values->output_value, &src[value_bytes * arrow_row], SIZEOF_DATUM);
			*column_values->output_isnull =
				!arrow_row_is_valid(column_values->buffers[0], arrow_row);
		}
		else if (column_values->decompression_type == DT_ArrowText)
		{
			store_text_datum(column_values, arrow_row);
			*column_values->output_isnull =
				!arrow_row_is_valid(column_values->buffers[0], arrow_row);
		}
		else if (column_values->decompression_type == DT_ArrowTextDict)
		{
			const int16 index = ((int16 *) column_values->buffers[3])[arrow_row];
			store_text_datum(column_values, index);
			*column_values->output_isnull =
				!arrow_row_is_valid(column_values->buffers[0], arrow_row);
		}
		else
		{
			/* A compressed column with default value, do nothing. */
			Assert(column_values->decompression_type == DT_Default);
		}
	}

	/*
	 * It's a virtual tuple slot, so no point in clearing/storing it
	 * per each row, we can just update the values in-place. This saves
	 * some CPU. We have to store it after ExecQual returns false (the tuple
	 * didn't pass the filter), or after a new batch. The standard protocol
	 * is to clear and set the tuple slot for each row, but our output tuple
	 * slots are read-only, and the memory is owned by this node, so it is
	 * safe to violate this protocol.
	 */
	Assert(TTS_IS_VIRTUAL(decompressed_scan_slot));
	if (TTS_EMPTY(decompressed_scan_slot))
	{
		ExecStoreVirtualTuple(decompressed_scan_slot);
	}
}

static bool
vector_qual(DecompressBatchState *batch_state, uint16 arrow_row)
{
	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->next_batch_row < batch_state->total_batch_rows);

	if (!batch_state->vector_qual_result)
	{
		return true;
	}

	return arrow_row_is_valid(batch_state->vector_qual_result, arrow_row);
}

static bool
postgres_qual(DecompressContext *dcontext, DecompressBatchState *batch_state)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(!TupIsNull(decompressed_scan_slot));

	if (dcontext->ps == NULL || dcontext->ps->qual == NULL)
	{
		return true;
	}

	/* Perform the usual Postgres selection. */
	ExprContext *econtext = dcontext->ps->ps_ExprContext;
	econtext->ecxt_scantuple = decompressed_scan_slot;
	ResetExprContext(econtext);
	return ExecQual(dcontext->ps->qual, econtext);
}

/*
 * Decompress the next tuple from the batch indicated by batch state. The result is stored
 * in batch_state->decompressed_scan_slot. The slot will be empty if the batch
 * is entirely processed.
 */
void
compressed_batch_advance(DecompressContext *dcontext, DecompressBatchState *batch_state)
{
	Assert(batch_state->total_batch_rows > 0);

	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	const bool reverse = dcontext->reverse;
	const int num_compressed_columns = dcontext->num_compressed_columns;

	for (; batch_state->next_batch_row < batch_state->total_batch_rows;
		 batch_state->next_batch_row++)
	{
		const uint16 output_row = batch_state->next_batch_row;
		const uint16 arrow_row =
			unlikely(reverse) ? batch_state->total_batch_rows - 1 - output_row : output_row;

		if (!vector_qual(batch_state, arrow_row))
		{
			/*
			 * This row doesn't pass the vectorized quals. Advance the iterated
			 * compressed columns if we have any.
			 */
			for (int i = 0; i < num_compressed_columns; i++)
			{
				CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
				if (column_values->decompression_type == DT_Iterator)
				{
					DecompressionIterator *iterator =
						(DecompressionIterator *) column_values->buffers[0];
					iterator->try_next(iterator);
				}
			}

			InstrCountFiltered1(dcontext->ps, 1);
			continue;
		}

		make_next_tuple(batch_state, arrow_row, num_compressed_columns);

		if (!postgres_qual(dcontext, batch_state))
		{
			/*
			 * The tuple didn't pass the qual, fetch the next one in the next
			 * iteration.
			 */
			InstrCountFiltered1(dcontext->ps, 1);
			continue;
		}

		/* The tuple passed the qual. */
		batch_state->next_batch_row++;
		return;
	}

	/*
	 * Reached end of batch. Check that the columns that we're decompressing
	 * row-by-row have also ended.
	 */
	Assert(batch_state->next_batch_row == batch_state->total_batch_rows);
	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
		if (column_values->decompression_type == DT_Iterator)
		{
			DecompressionIterator *iterator = (DecompressionIterator *) column_values->buffers[0];
			DecompressResult result = iterator->try_next(iterator);
			if (!result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}
		}
	}

	/* Clear old slot state */
	ExecClearTuple(decompressed_scan_slot);
}

/*
 * Before loading the first matching tuple from the batch, also save the very
 * first one into the given slot, even if it doesn't pass the quals. This is
 * needed for batch sorted merge.
 */
void
compressed_batch_save_first_tuple(DecompressContext *dcontext, DecompressBatchState *batch_state,
								  TupleTableSlot *first_tuple_slot)
{
	Assert(batch_state->next_batch_row == 0);
	Assert(batch_state->total_batch_rows > 0);
	Assert(TupIsNull(batch_state->decompressed_scan_slot));

	/*
	 * Check that we have decompressed all columns even if the vector quals
	 * didn't pass for the entire batch. We need them because we're asked
	 * to save the first tuple. This doesn't actually happen yet, because the
	 * vectorized decompression is disabled with sorted merge.
	 */
#ifdef USE_ASSERT_CHECKING
	const int num_compressed_columns = dcontext->num_compressed_columns;
	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
		Assert(column_values->decompression_type != DT_Invalid);
	}
#endif

	/* Make the first tuple and save it. */
	Assert(batch_state->next_batch_row == 0);
	const uint16 arrow_row = dcontext->reverse ? batch_state->total_batch_rows - 1 : 0;
	make_next_tuple(batch_state, arrow_row, dcontext->num_compressed_columns);
	ExecCopySlot(first_tuple_slot, batch_state->decompressed_scan_slot);

	/*
	 * Check the quals and advance, so that the batch is in the correct state
	 * for the subsequent calls (matching tuple is in decompressed scan slot).
	 */
	const bool qual_passed =
		vector_qual(batch_state, arrow_row) && postgres_qual(dcontext, batch_state);
	batch_state->next_batch_row++;

	if (!qual_passed)
	{
		InstrCountFiltered1(dcontext->ps, 1);
		compressed_batch_advance(dcontext, batch_state);
	}
}
