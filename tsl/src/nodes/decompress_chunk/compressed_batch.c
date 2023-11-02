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
#include "nodes/decompress_chunk/exec.h"
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

static void
translate_from_dictionary(const ArrowArray *arrow, uint64 *restrict dict_result,
	uint64 *restrict final_result)
{
	Assert(arrow->dictionary != NULL);

	/* Translate dictionary results to per-value results. */
	const size_t n = arrow->length;
	int16 *restrict indices = (int16 *) arrow->buffers[1];
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const size_t row = outer * 64 + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const int16 index = indices[row];                                                              \
	const bool valid = arrow_row_is_valid(dict_result, index);                                     \
	word |= ((uint64) valid) << bit_index;

			INNER_LOOP

			//			fprintf(stderr, "dict-coded row %ld: index %d, valid %d\n", row, index,
			// valid);
		}
		final_result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 word = 0;
		for (size_t row = (n / 64) * 64; row < n; row++)
		{
			const size_t bit_index = row % 64;

			INNER_LOOP
		}
		final_result[n / 64] &= word;
	}
#undef INNER_LOOP
}

static inline void
vector_predicate_saop_impl(VectorPredicate *vector_const_predicate, bool is_or, const ArrowArray *vector, Datum array,
						 uint64 *restrict final_result)
{
	const size_t result_bits = vector->length;
	const size_t result_words = (result_bits + 63) / 64;

	uint64 *restrict array_result;
	/*
	 * For OR, we need an intermediate storage to accumulate the results
	 * from all elements.
	 * For AND, we can apply predicate for each element to the final result.
	 */
	uint64 array_result_storage[(GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64];
	if (is_or)
	{
		array_result = array_result_storage;
		for (size_t i = 0; i < result_words; i++)
		{
			array_result_storage[i] = 0;
		}
	}
	else
	{
		array_result = final_result;
	}

	ArrayType *arr = DatumGetArrayTypeP(array);

	int16 typlen;
	bool typbyval;
	char typalign;
	get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);

	const char *s = (const char *) ARR_DATA_PTR(arr);
	Ensure(ARR_NULLBITMAP(arr) == NULL, "vectorized scalar array ops do not support nullable arrays");

	const int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

	for (int i = 0; i < nitems; i++)
	{
		Datum constvalue = fetch_att(s, typbyval, typlen);
		s = att_addlength_pointer(s, typlen, s);
		s = (char *) att_align_nominal(s, typalign);

		/*
		 * For OR, we also need an intermediate storage for predicate result
		 * for each array element, since the predicates AND their result.
		 *
		 * For AND, we can and apply predicate for each array element to the
		 * final result.
		 */
		uint64 single_result_storage[(GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64];
		uint64 *restrict single_result;
		if (is_or)
		{
			single_result = single_result_storage;
			for (size_t outer = 0; outer < result_words; outer++)
			{
				single_result[outer] = -1;
			}
		}
		else
		{
			single_result = final_result;
		}

		vector_const_predicate(vector, constvalue, single_result);

		if (is_or)
		{
			for (size_t outer = 0; outer < result_words; outer++)
			{
				array_result[outer] |= single_result[outer];
			}
		}
	}

	if (is_or)
	{
		for (size_t outer = 0; outer < result_words; outer++)
		{
			/*
			 * The tail bits corresponding to past-the-end rows when n % 64 != 0
			 * should be already zeroed out in the final_result.
			 */
			final_result[outer] &= array_result[outer];
		}
	}
}

static void
vector_predicate_saop_and(VectorPredicate *scalar_predicate, const ArrowArray *vector, Datum array,
						  uint64 *restrict result)
{
	vector_predicate_saop_impl(scalar_predicate, /* is_or = */ false,
		vector, array, result);
}

static void
vector_predicate_saop_or(VectorPredicate *scalar_predicate, const ArrowArray *vector, Datum array,
						  uint64 *restrict result)
{
	vector_predicate_saop_impl(scalar_predicate, /* is_or = */ true,
		vector, array, result);
}

static void
apply_vector_quals(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	if (!chunk_state->vectorized_quals_constified)
	{
		return;
	}

	/*
	 * Allocate the bitmap that will hold the vectorized qual results. We will
	 * initialize it to all ones and AND the individual quals to it.
	 */
	const int bitmap_bytes = sizeof(uint64) * ((batch_state->total_batch_rows + 63) / 64);
	batch_state->vector_qual_result = palloc(bitmap_bytes);
	memset(batch_state->vector_qual_result, 0xFF, bitmap_bytes);
	if (batch_state->total_batch_rows % 64 != 0)
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
	ListCell *lc;
	foreach (lc, chunk_state->vectorized_quals_constified)
	{
		/*
		 * For now we support "Var ? Const" predicates and
		 * ScalarArrayOperations.
		 */
		List *args = NULL;
		RegProcedure vector_const_opcode = InvalidOid;
		ScalarArrayOpExpr *saop = NULL;
		OpExpr *opexpr = NULL;
		if (IsA(lfirst(lc), ScalarArrayOpExpr))
		{
			saop = castNode(ScalarArrayOpExpr, lfirst(lc));
			args = saop->args;
			vector_const_opcode = get_opcode(saop->opno);
		}
		else
		{
			opexpr = castNode(OpExpr, lfirst(lc));
			args = opexpr->args;
			vector_const_opcode = get_opcode(opexpr->opno);
		}

		/*
		 * Find the vector_const predicate.
		 */
		VectorPredicate *vector_const_predicate = get_vector_const_predicate(vector_const_opcode);
		Ensure(vector_const_predicate != NULL,
			   "vectorized predicate not found for postgres predicate %d",
			   vector_const_opcode);

		/*
		 * Find the compressed column referred to by the Var.
		 */
		Var *var = castNode(Var, linitial(args));
		DecompressChunkColumnDescription *column_description = NULL;
		int column_index = 0;
		for (; column_index < chunk_state->num_total_columns; column_index++)
		{
			column_description = &chunk_state->template_columns[column_index];
			if (column_description->output_attno == var->varattno)
			{
				break;
			}
		}
		Ensure(column_index < chunk_state->num_total_columns,
			   "decompressed column %d not found in batch",
			   var->varattno);
		Assert(column_description != NULL);
		Assert(column_description->typid == var->vartype);
		Ensure(column_description->type == COMPRESSED_COLUMN,
			   "only compressed columns are supported in vectorized quals");
		Assert(column_index < chunk_state->num_compressed_columns);
		CompressedColumnValues *column_values = &batch_state->compressed_columns[column_index];
		Ensure(column_values->iterator == NULL,
			   "only arrow columns are supported in vectorized quals");

		/*
		 * Prepare to compute the vector predicate. We have to handle the
		 * default values in a special way because they don't produce the usual
		 * decompressed ArrowArrays.
		 */
		uint64 default_value_predicate_result;
		uint64 *predicate_result = batch_state->vector_qual_result;
		const ArrowArray *vector = column_values->arrow;
		if (column_values->arrow == NULL)
		{
			/*
			 * The compressed column had a default value. We can't fall back to
			 * the non-vectorized quals now, so build a single-value ArrowArray
			 * with this default value, check if it passes the predicate, and apply
			 * it to the entire batch.
			 */
			AttrNumber attr = AttrNumberGetAttrOffset(column_description->output_attno);

			Ensure(column_values->iterator == NULL,
				   "ArrowArray expected for column %s",
				   NameStr(
					   TupleDescAttr(batch_state->decompressed_scan_slot->tts_tupleDescriptor, attr)
						   ->attname));

			/*
			 * We saved the actual default value into the decompressed scan slot
			 * above, so pull it from there.
			 */
			vector = make_single_value_arrow(column_description->typid,
											 batch_state->decompressed_scan_slot->tts_values[attr],
											 batch_state->decompressed_scan_slot->tts_isnull[attr]);

			/*
			 * We start from an all-valid bitmap, because the predicate is
			 * AND-ed to it.
			 */
			default_value_predicate_result = 1;
			predicate_result = &default_value_predicate_result;
		}

		/*
		 * The vectorizable predicates should be STRICT, so we shouldn't see null
		 * constants here.
		 */
		Const *constnode = castNode(Const, lsecond(args));
		Ensure(!constnode->constisnull, "vectorized predicate called for a null value");

		/*
		 * If the data is dictionary-encoded, we are going to compute the
		 * predicate on dictionary and then translate the results.
		 */
		const ArrowArray *vector_nodict = NULL;
		uint64 *restrict predicate_result_nodict = NULL;
		uint64 dict_result[(GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64];
		if (vector->dictionary)
		{
			const size_t dict_rows = vector->dictionary->length;
			const size_t dict_result_words = (dict_rows + 63) / 64;
			memset(dict_result, 0xFF, dict_result_words * 8);
			predicate_result_nodict = dict_result;
			vector_nodict = vector->dictionary;
		}
		else
		{
			predicate_result_nodict = predicate_result;
			vector_nodict = vector;
		}

		/*
		 * At last, compute the predicate.
		 */
		if (saop)
		{
			if (saop->useOr)
			{
				vector_predicate_saop_or(vector_const_predicate,
										 vector_nodict,
										 constnode->constvalue,
										 predicate_result_nodict);
			}
			else
			{
				vector_predicate_saop_and(vector_const_predicate,
										  vector_nodict,
										  constnode->constvalue,
										  predicate_result_nodict);
			}
		}
		else
		{
			vector_const_predicate(vector_nodict, constnode->constvalue, predicate_result_nodict);
		}

		/*
		 * If the vector is dictionary-encoded, we have just computed the
		 * predicate for dictionary and now have to translate it.
		 */
		if (vector->dictionary)
		{
			fprintf(stderr, "dictionary\n");
			translate_from_dictionary(vector, predicate_result_nodict, predicate_result);
		}
		else
		{
			fprintf(stderr, "normal\n");
		}

		/* Account for nulls which shouldn't pass the predicate. */
		const size_t n = vector->length;
		const size_t n_words = (n + 63) / 64;
		const uint64 *restrict validity = (uint64 *restrict) vector->buffers[0];
		for (size_t i = 0; i < n_words; i++)
		{
			predicate_result[i] &= validity[i];
		}

		/* Process the result. */
		if (column_values->arrow == NULL)
		{
			/* The column had a default value. */
			Assert(column_values->iterator == NULL);

			if (!(default_value_predicate_result & 1))
			{
				/*
				 * We had a default value for the compressed column, and it
				 * didn't pass the predicate, so the entire batch didn't pass.
				 */
				for (int i = 0; i < bitmap_bytes / 8; i++)
				{
					batch_state->vector_qual_result[i] = 0;
				}
			}
		}
	}
}

static int
get_max_element_bytes(ArrowArray *text_array)
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

/*
 * Initialize the bulk decompression memory context
 */
void
init_bulk_decompression_mctx(DecompressChunkState *chunk_state, MemoryContext parent_ctx)
{
	Assert(chunk_state != NULL);
	Assert(parent_ctx != NULL);
	Assert(chunk_state->bulk_decompression_context == NULL);

	chunk_state->bulk_decompression_context = AllocSetContextCreate(parent_ctx,
																	"bulk decompression",
																	/* minContextSize = */ 0,
																	/* initBlockSize = */ 64 * 1024,
																	/* maxBlockSize = */ 64 * 1024);
}

/*
 * Initialize the batch memory context
 *
 * We use custom size for the batch memory context page, calculated to
 * fit the typical result of bulk decompression (if we use it).
 * This allows us to save on expensive malloc/free calls, because the
 * Postgres memory contexts reallocate all pages except the first one
 * after each reset.
 */
void
init_per_batch_mctx(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	Assert(chunk_state != NULL);
	Assert(batch_state != NULL);
	Assert(batch_state->per_batch_context == NULL);

	batch_state->per_batch_context = AllocSetContextCreate(CurrentMemoryContext,
														   "DecompressChunk per_batch",
														   0,
														   chunk_state->batch_memory_context_bytes,
														   chunk_state->batch_memory_context_bytes);
}

/*
 * Initialize the batch decompression state with the new compressed  tuple.
 */
void
compressed_batch_set_compressed_tuple(DecompressChunkState *chunk_state,
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
		init_per_batch_mctx(chunk_state, batch_state);
		Assert(batch_state->per_batch_context != NULL);

		Assert(batch_state->compressed_slot == NULL);

		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->compressed_slot_tdesc == NULL)
			chunk_state->compressed_slot_tdesc =
				CreateTupleDescCopyConstr(subslot->tts_tupleDescriptor);
		Assert(chunk_state->compressed_slot_tdesc->tdrefcount == -1);

		batch_state->compressed_slot =
			MakeSingleTupleTableSlot(chunk_state->compressed_slot_tdesc, subslot->tts_ops);

		Assert(batch_state->decompressed_scan_slot == NULL);

		/* Get a reference the the output TupleTableSlot */
		TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;

		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->decompressed_slot_scan_tdesc == NULL)
			chunk_state->decompressed_slot_scan_tdesc =
				CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
		Assert(chunk_state->decompressed_slot_scan_tdesc->tdrefcount == -1);

		batch_state->decompressed_scan_slot =
			MakeSingleTupleTableSlot(chunk_state->decompressed_slot_scan_tdesc, &TTSOpsVirtual);
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

	for (int i = 0; i < chunk_state->num_total_columns; i++)
	{
		DecompressChunkColumnDescription *column_description = &chunk_state->template_columns[i];

		switch (column_description->type)
		{
			case COMPRESSED_COLUMN:
			{
				Assert(i < chunk_state->num_compressed_columns);
				CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
				column_values->iterator = NULL;
				column_values->arrow = NULL;
				column_values->value_bytes = -1;
				column_values->arrow_values = NULL;
				column_values->arrow_validity = NULL;
				column_values->output_attno = column_description->output_attno;
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
					column_values->iterator = NULL;
					AttrNumber attr = AttrNumberGetAttrOffset(column_description->output_attno);

					batch_state->decompressed_scan_slot->tts_values[attr] =
						getmissingattr(batch_state->decompressed_scan_slot->tts_tupleDescriptor,
									   attr + 1,
									   &batch_state->decompressed_scan_slot->tts_isnull[attr]);
					break;
				}

				/* Decompress the entire batch if it is supported. */
				CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);
				ArrowArray *arrow = NULL;
				if (chunk_state->enable_bulk_decompression &&
					column_description->bulk_decompression_supported)
				{
					if (chunk_state->bulk_decompression_context == NULL)
						init_bulk_decompression_mctx(chunk_state,
													 MemoryContextGetParent(
														 batch_state->per_batch_context));

					DecompressAllFunction decompress_all =
						tsl_get_decompress_all_function(header->compression_algorithm,
														column_description->typid);
					Assert(decompress_all != NULL);

					MemoryContext context_before_decompression =
						MemoryContextSwitchTo(chunk_state->bulk_decompression_context);

					arrow = decompress_all(PointerGetDatum(header),
										   column_description->typid,
										   batch_state->per_batch_context);

					MemoryContextReset(chunk_state->bulk_decompression_context);

					MemoryContextSwitchTo(context_before_decompression);
				}

				if (arrow)
				{
					if (batch_state->total_batch_rows == 0)
					{
						batch_state->total_batch_rows = arrow->length;
					}
					else if (batch_state->total_batch_rows != arrow->length)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}

					column_values->arrow = arrow;
					column_values->arrow_values = arrow->buffers[1];
					column_values->arrow_validity = arrow->buffers[0];

					column_values->value_bytes = get_typlen(column_description->typid);

					if (column_values->value_bytes == -1)
					{
						const int maxbytes =
							VARHDRSZ +
							(column_values->arrow->dictionary ?
								 get_max_element_bytes(column_values->arrow->dictionary) :
								 get_max_element_bytes(column_values->arrow));

						const AttrNumber attr =
							AttrNumberGetAttrOffset(column_values->output_attno);
						batch_state->decompressed_scan_slot->tts_values[attr] = PointerGetDatum(
							MemoryContextAlloc(batch_state->per_batch_context, maxbytes));
					}

					break;
				}

				/* As a fallback, decompress row-by-row. */
				column_values->iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														chunk_state
															->reverse)(PointerGetDatum(header),
																	   column_description->typid);

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				/*
				 * A segmentby column is not going to change during one batch,
				 * and our output tuples are read-only, so it's enough to only
				 * save it once per batch, which we do here.
				 */
				AttrNumber attr = AttrNumberGetAttrOffset(column_description->output_attno);
				batch_state->decompressed_scan_slot->tts_values[attr] =
					slot_getattr(batch_state->compressed_slot,
								 column_description->compressed_scan_attno,
								 &batch_state->decompressed_scan_slot->tts_isnull[attr]);
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

				if (batch_state->total_batch_rows == 0)
				{
					batch_state->total_batch_rows = count_value;
				}
				else if (batch_state->total_batch_rows != count_value)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

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

	apply_vector_quals(chunk_state, batch_state);

	MemoryContextSwitchTo(old_context);
}

static void
store_text_datum(ArrowArray *arrow, int arrow_row, Datum *dest)
{
	Assert(arrow->dictionary == NULL);
	const uint32 start = ((uint32 *) arrow->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) arrow->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	const int total_bytes = value_bytes + VARHDRSZ;
	Assert(DatumGetPointer(*dest) != NULL);
	SET_VARSIZE(*dest, total_bytes);
	memcpy(VARDATA(*dest), &((uint8 *) arrow->buffers[2])[start], value_bytes);
}

/*
 * Construct the next tuple in the decompressed scan slot.
 * Doesn't check the quals.
 */
static void
compressed_batch_make_next_tuple(DecompressChunkState *chunk_state,
								 DecompressBatchState *batch_state)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->next_batch_row < batch_state->total_batch_rows);

	const int output_row = batch_state->next_batch_row;
	const size_t arrow_row = unlikely(chunk_state->reverse) ?
								 batch_state->total_batch_rows - 1 - output_row :
								 output_row;

	const int num_compressed_columns = chunk_state->num_compressed_columns;
	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues column_values = batch_state->compressed_columns[i];

		if (column_values.iterator != NULL)
		{
			DecompressResult result = column_values.iterator->try_next(column_values.iterator);

			if (result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}

			const AttrNumber attr = AttrNumberGetAttrOffset(column_values.output_attno);
			decompressed_scan_slot->tts_isnull[attr] = result.is_null;
			decompressed_scan_slot->tts_values[attr] = result.val;
		}
		else if (column_values.arrow_values != NULL)
		{
			const AttrNumber attr = AttrNumberGetAttrOffset(column_values.output_attno);
			if (column_values.value_bytes == -1)
			{
				if (column_values.arrow->dictionary == NULL)
				{
					store_text_datum(column_values.arrow,
									 arrow_row,
									 &decompressed_scan_slot->tts_values[attr]);
				}
				else
				{
					const int16 index = ((int16 *) column_values.arrow->buffers[1])[arrow_row];
					store_text_datum(column_values.arrow->dictionary,
									 index,
									 &decompressed_scan_slot->tts_values[attr]);
				}

				decompressed_scan_slot->tts_isnull[attr] =
					!arrow_row_is_valid(column_values.arrow->buffers[0], arrow_row);
			}
			else
			{
				Assert(column_values.value_bytes > 0);
				const char *restrict src = column_values.arrow_values;

				/*
				 * The conversion of Datum to more narrow types will truncate
				 * the higher bytes, so we don't care if we read some garbage
				 * into them, and can always read 8 bytes. These are unaligned
				 * reads, so technically we have to do memcpy.
				 */
				uint64 value;
				memcpy(&value, &src[column_values.value_bytes * arrow_row], 8);

#ifdef USE_FLOAT8_BYVAL
				Datum datum = Int64GetDatum(value);
#else
				/*
				 * On 32-bit systems, the data larger than 4 bytes go by
				 * reference, so we have to jump through these hoops.
				 */
				Datum datum;
				if (column_values.value_bytes <= 4)
				{
					datum = Int32GetDatum((uint32) value);
				}
				else
				{
					datum = Int64GetDatum(value);
				}
#endif
				decompressed_scan_slot->tts_values[attr] = datum;
				decompressed_scan_slot->tts_isnull[attr] =
					!arrow_row_is_valid(column_values.arrow_validity, arrow_row);
			}
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
compressed_batch_vector_qual(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->next_batch_row < batch_state->total_batch_rows);

	const int output_row = batch_state->next_batch_row;
	const size_t arrow_row =
		chunk_state->reverse ? batch_state->total_batch_rows - 1 - output_row : output_row;

	if (!batch_state->vector_qual_result)
	{
		return true;
	}

	return arrow_row_is_valid(batch_state->vector_qual_result, arrow_row);
}

static bool
compressed_batch_postgres_qual(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(!TupIsNull(decompressed_scan_slot));

	if (!chunk_state->csstate.ss.ps.qual)
	{
		return true;
	}

	/* Perform the usual Postgres selection. */
	ExprContext *econtext = chunk_state->csstate.ss.ps.ps_ExprContext;
	econtext->ecxt_scantuple = decompressed_scan_slot;
	ResetExprContext(econtext);
	return ExecQual(chunk_state->csstate.ss.ps.qual, econtext);
}

/*
 * Decompress the next tuple from the batch indicated by batch state. The result is stored
 * in batch_state->decompressed_scan_slot. The slot will be empty if the batch
 * is entirely processed.
 */
void
compressed_batch_advance(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	Assert(batch_state->total_batch_rows > 0);

	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	const int num_compressed_columns = chunk_state->num_compressed_columns;

	for (; batch_state->next_batch_row < batch_state->total_batch_rows;
		 batch_state->next_batch_row++)
	{
		if (!compressed_batch_vector_qual(chunk_state, batch_state))
		{
			/*
			 * This row doesn't pass the vectorized quals. Advance the iterated
			 * compressed columns if we have any.
			 */
			for (int i = 0; i < num_compressed_columns; i++)
			{
				CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
				if (column_values->iterator)
				{
					column_values->iterator->try_next(column_values->iterator);
				}
			}
			InstrCountFiltered1(&chunk_state->csstate, 1);
			continue;
		}

		compressed_batch_make_next_tuple(chunk_state, batch_state);

		if (!compressed_batch_postgres_qual(chunk_state, batch_state))
		{
			/*
			 * The tuple didn't pass the qual, fetch the next one in the next
			 * iteration.
			 */
			InstrCountFiltered1(&chunk_state->csstate, 1);
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
		if (column_values->iterator)
		{
			DecompressResult result = column_values->iterator->try_next(column_values->iterator);
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
compressed_batch_save_first_tuple(DecompressChunkState *chunk_state,
								  DecompressBatchState *batch_state,
								  TupleTableSlot *first_tuple_slot)
{
	Assert(batch_state->next_batch_row == 0);
	Assert(batch_state->total_batch_rows > 0);
	Assert(TupIsNull(batch_state->decompressed_scan_slot));

	compressed_batch_make_next_tuple(chunk_state, batch_state);
	ExecCopySlot(first_tuple_slot, batch_state->decompressed_scan_slot);

	const bool qual_passed = compressed_batch_vector_qual(chunk_state, batch_state) &&
							 compressed_batch_postgres_qual(chunk_state, batch_state);
	batch_state->next_batch_row++;

	if (!qual_passed)
	{
		InstrCountFiltered1(&chunk_state->csstate, 1);
		compressed_batch_advance(chunk_state, batch_state);
	}
}
