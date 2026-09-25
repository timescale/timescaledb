/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/htup_details.h>
#include <utils/lsyscache.h>

#include "compression/batch_decompressor.h"
#include "compression/compression.h"

/*
 * Scalar setup for a column whose value is constant for the whole batch
 * (batch default or all-NULL batch). Mirrors decompress_scalar_column in
 * compressed_batch.c.
 */
static void
batch_decompressor_scalar(CompressedColumnValues *column_values, Datum value, bool isnull)
{
	column_values->decompression_type = DT_Scalar;
	column_values->buffers[0] = DatumGetPointer(BoolGetDatum(isnull));
	column_values->buffers[1] = DatumGetPointer(value);

	*column_values->output_isnull = isnull;
	*column_values->output_value = value;
}

/*
 * Wrap a freshly decompressed ArrowArray into the CompressedColumnValues,
 * mirroring the fixed-width / bool / text / dictionary cases of
 * decompress_column (compressed_batch.c). For text columns the output
 * varlena is pre-allocated through the caller-wired output pointer, which
 * is why output pointers must be bound before decoding.
 */
static void
batch_decompressor_fill_arrow(CompressedColumnValues *column_values, Oid typid, ArrowArray *arrow,
						MemoryContext per_batch_context)
{
	const int value_bytes = get_typlen(typid);
	Assert(value_bytes != 0);

	column_values->arrow = arrow;

	if (value_bytes > 0)
	{
		/* Fixed-width column. */
		column_values->decompression_type = value_bytes;
		column_values->buffers[0] = arrow->buffers[0];
		column_values->buffers[1] = arrow->buffers[1];
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;

		if (typid == BOOLOID)
		{
			/* The bool columns have a dedicated storage format. */
			column_values->decompression_type = DT_ArrowBits;
		}
	}
	else
	{
		/*
		 * Text column. Pre-allocate memory for its text Datum in the
		 * per-batch context through the output pointer. We can't put direct
		 * references to Arrow memory there, because it doesn't have the
		 * varlena headers that Postgres expects for text.
		 */
		const int maxbytes = get_max_varlena_bytes(arrow);
		*column_values->output_value =
			PointerGetDatum(MemoryContextAlloc(per_batch_context, maxbytes));

		if (arrow->dictionary == NULL)
		{
			column_values->decompression_type = DT_ArrowText;
			column_values->buffers[0] = arrow->buffers[0];
			column_values->buffers[1] = arrow->buffers[1];
			column_values->buffers[2] = arrow->buffers[2];
			column_values->buffers[3] = NULL;
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

void
batch_decompressor_init(BatchDecompressor *state, int num_columns, const BatchDecompressorColumn *columns,
				  CompressedColumnValues *values, TupleDesc uncompressed_tdesc,
				  Detoaster *detoaster, MemoryContext per_batch_context,
				  MemoryContext *bulk_scratch, bool enable_bulk_decompression, bool reverse)
{
	state->num_columns = num_columns;
	state->columns = columns;
	state->values = values;
	state->uncompressed_tdesc = uncompressed_tdesc;
	state->detoaster = detoaster;
	state->per_batch_context = per_batch_context;
	state->bulk_scratch = bulk_scratch;
	state->enable_bulk_decompression = enable_bulk_decompression;
	state->reverse = reverse;
	state->total_batch_rows = 0;
}

void
batch_decompressor_begin(BatchDecompressor *state, int rows, int max_rows)
{
	/*
	 * Validate the count as an integer before narrowing it to uint16, so a
	 * corrupt count can never silently truncate.
	 */
	CheckCompressedData(rows > 0);
	CheckCompressedData(rows <= max_rows);
	CheckCompressedData(max_rows <= UINT16_MAX);

	state->total_batch_rows = (uint16) rows;

	for (int i = 0; i < state->num_columns; i++)
	{
		CompressedColumnValues *column_values = &state->values[i];
		column_values->decompression_type = DT_Invalid;
		column_values->arrow = NULL;
		column_values->buffers[0] = NULL;
		column_values->buffers[1] = NULL;
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;
	}
}

BatchDecompressorMetrics
batch_decompressor_column(BatchDecompressor *state, int column_index, Datum compressed,
					bool input_isnull)
{
	BatchDecompressorMetrics metrics = {
		.decompressed = false,
		.compressed_bytes = 0,
	};
	CompressedColumnValues *column_values = &state->values[column_index];
	const BatchDecompressorColumn *column = &state->columns[column_index];

	/* Lazy: decompress each column at most once per batch. */
	if (column_values->decompression_type != DT_Invalid)
		return metrics;

	if (input_isnull)
	{
		/*
		 * The column has a default value for the entire batch, set it now.
		 * The default comes from the uncompressed chunk tuple descriptor,
		 * because a custom scan tuple might not carry it.
		 */
		bool isnull;
		Datum defval = getmissingattr(state->uncompressed_tdesc,
									  column->uncompressed_attno,
									  &isnull);
		batch_decompressor_scalar(column_values, defval, isnull);
		return metrics;
	}

	/* Detoast the compressed datum into the per-batch context. */
	Datum value = PointerGetDatum(detoaster_detoast_attr_copy((struct varlena *) DatumGetPointer(compressed),
															  state->detoaster,
															  state->per_batch_context));
	CompressedDataHeader *header = (CompressedDataHeader *) value;

	if (header->compression_algorithm == COMPRESSION_ALGORITHM_NULL)
	{
		/* All-NULL batch. */
		batch_decompressor_scalar(column_values, (Datum) 0, true);
		return metrics;
	}

	metrics.decompressed = true;
	metrics.compressed_bytes = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	ArrowArray *arrow = NULL;
	if (state->enable_bulk_decompression && column->bulk_decompression_supported)
	{
		if (*state->bulk_scratch == NULL)
		{
			*state->bulk_scratch =
				create_bulk_decompression_mctx(MemoryContextGetParent(state->per_batch_context));
		}

		DecompressAllFunction decompress_all =
			tsl_get_decompress_all_function(header->compression_algorithm, column->typid);
		Assert(decompress_all != NULL);

		MemoryContext save = MemoryContextSwitchTo(*state->bulk_scratch);
		arrow = decompress_all(PointerGetDatum(header), column->typid, state->per_batch_context);
		MemoryContextSwitchTo(save);
		MemoryContextReset(*state->bulk_scratch);
	}

	if (arrow == NULL)
	{
		/*
		 * Iterator fallback: bulk is not allowed for this column, or the
		 * bulk implementation declined the input.
		 */
		MemoryContext save = MemoryContextSwitchTo(state->per_batch_context);
		column_values->decompression_type = DT_Iterator;
		column_values->buffers[0] =
			tsl_get_decompression_iterator_init(header->compression_algorithm,
												state->reverse)(PointerGetDatum(header),
																column->typid);
		MemoryContextSwitchTo(save);
		return metrics;
	}

	/* Should have been validated by batch_decompressor_begin. */
	Assert(state->total_batch_rows != 0);
	if (state->total_batch_rows != arrow->length)
	{
		elog(ERROR, "compressed column out of sync with batch counter");
	}

	batch_decompressor_fill_arrow(column_values, column->typid, arrow, state->per_batch_context);
	return metrics;
}

void
batch_decompressor_discard(BatchDecompressor *state)
{
	for (int i = 0; i < state->num_columns; i++)
	{
		CompressedColumnValues *column_values = &state->values[i];
		column_values->decompression_type = DT_Invalid;
		column_values->arrow = NULL;
		column_values->buffers[0] = NULL;
		column_values->buffers[1] = NULL;
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;
	}
}
