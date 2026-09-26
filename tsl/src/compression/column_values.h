/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"

/*
 * Unified per-column representation of decompressed batch data, shared by
 * the columnar scan, vector aggregation, and the RowDecompressor-based
 * decompression paths. The wrappers describe how to obtain the decompressed
 * datum for an individual row, independent of which consumer drives the
 * batch.
 */

/* How to obtain the decompressed datum for individual row. */
typedef enum
{
	/*
	 * The decompressed value is a boolean and is stored as a vector of bits.
	 */
	DT_ArrowBits = -5,

	DT_ArrowTextDict = -4,

	DT_ArrowText = -3,

	/*
	 * The decompressed value is already in the decompressed slot. This is used
	 * for segmentby and compressed columns with default value in batch.
	 */
	DT_Scalar = -2,

	DT_Iterator = -1,

	DT_Invalid = 0,

	/*
	 * Any positive number is also valid for the decompression type. It means
	 * arrow array of a fixed-size by-value type, with size in bytes given by
	 * the number.
	 */
} DecompressionType;

typedef struct CompressedColumnValues
{
	/* How to obtain the decompressed datum for individual row. */
	DecompressionType decompression_type;

	/* Where to put the decompressed datum. */
	Datum *output_value;
	bool *output_isnull;

	/*
	 * The flattened source buffers for getting the decompressed datum.
	 * Depending on decompression type, they are as follows:
	 * scalar:          isnull, value
	 * iterator:        iterator
	 * arrow fixed:     validity, value
	 * arrow text:      validity, uint32* offsets, void* bodies
	 * arrow dict text: validity, uint32* dict offsets, void* dict bodies, int16* indices
	 */
	const void *restrict buffers[4];

	/*
	 * The source arrow array, if any. We don't use it for building the
	 * individual rows, and use the flattened buffers instead to lessen the
	 * amount of indirections. However, it is used for vectorized filters.
	 */
	ArrowArray *arrow;
} CompressedColumnValues;

extern ArrowArray *make_single_value_arrow(Oid pgtype, Datum datum, bool isnull);
int get_max_varlena_bytes(ArrowArray *text_array);

/*
 * Initialize the bulk decompression scratch memory context.
 *
 * We use Generation context here because the AllocSet has a hardcoded threshold
 * of 8kB per allocation, after which it allocates directly through malloc. We
 * want to make the blocks as big as possible, but below the malloc's mmap
 * threshold. For small queries, these contexts are basically single-shot and
 * the page faults after an mmap slow them down significantly. The threshold
 * should be 128 kiB according to the docs, but I'm seeing 64 kiB in testing.
 */
#define create_bulk_decompression_mctx(parent_mctx)                                                \
	GenerationContextCreate(parent_mctx,                                                           \
							"DecompressBatchState bulk decompression",                             \
							0,                                                                     \
							64 * 1024,                                                             \
							64 * 1024);

inline static void
store_text_datum(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	const int total_bytes = value_bytes + VARHDRSZ;
	Assert(DatumGetPointer(*column_values->output_value) != NULL);
	SET_VARSIZE(DatumGetPointer(*column_values->output_value), total_bytes);
	memcpy(VARDATA(DatumGetPointer(*column_values->output_value)),
		   &((uint8 *) column_values->buffers[2])[start],
		   value_bytes);
}

static pg_attribute_always_inline void
compressed_columns_to_postgres_data(CompressedColumnValues *columns, int num_data_columns,
									uint16 arrow_row)
{
	for (int i = 0; i < num_data_columns; i++)
	{
		CompressedColumnValues *column_values = &columns[i];
		switch ((int) column_values->decompression_type)
		{
			case DT_Iterator:
			{
				DecompressionIterator *iterator =
					(DecompressionIterator *) column_values->buffers[0];
				DecompressResult result = iterator->try_next(iterator);

				if (result.is_done)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

				*column_values->output_isnull = result.is_null;
				*column_values->output_value = result.val;
				break;
			}
#ifndef USE_FLOAT8_BYVAL
			case 8:
#endif
			case 16:
			{
				/*
				 * Fixed-width by-reference type that doesn't fit into a Datum.
				 * For now this only happens for 8-byte types on 32-bit systems,
				 * but eventually we could also use it for bigger by-value types
				 * such as UUID.
				 */
				const uint8 value_bytes = column_values->decompression_type;
				const char *src = column_values->buffers[1];
				*column_values->output_value = PointerGetDatum(&src[value_bytes * arrow_row]);
				*column_values->output_isnull =
					!arrow_row_is_valid(column_values->buffers[0], arrow_row);
				break;
			}
			case DT_ArrowBits:
			{
				/*
				 * The DT_ArrowBits type is a special case, because the value is
				 * stored as an Array of bits.
				 */
				*column_values->output_value =
					BoolGetDatum(arrow_row_is_valid(column_values->buffers[1], arrow_row));
				*column_values->output_isnull =
					!arrow_row_is_valid(column_values->buffers[0], arrow_row);
				break;
			}
			case 2:
			case 4:
#ifdef USE_FLOAT8_BYVAL
			case 8:
#endif
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
				const char *src = column_values->buffers[1];
				memcpy(column_values->output_value, &src[value_bytes * arrow_row], SIZEOF_DATUM);
				*column_values->output_isnull =
					!arrow_row_is_valid(column_values->buffers[0], arrow_row);
				break;
			}
			case DT_ArrowText:
			{
				store_text_datum(column_values, arrow_row);
				*column_values->output_isnull =
					!arrow_row_is_valid(column_values->buffers[0], arrow_row);
				break;
			}
			case DT_ArrowTextDict:
			{
				const int16 index = ((int16 *) column_values->buffers[3])[arrow_row];
				store_text_datum(column_values, index);
				*column_values->output_isnull =
					!arrow_row_is_valid(column_values->buffers[0], arrow_row);
				break;
			}
			default:
			{
				/* A compressed column with default value, do nothing. */
				Assert(column_values->decompression_type == DT_Scalar);
			}
		}
	}
}
