/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <libpq/pqformat.h>

#include "compression.h"

#include "compression_test.h"

#include "arrow_c_data_interface.h"

static uint32
arrow_get_str(ArrowArray *arrow, int arrow_row, const char **str)
{
	if (!arrow->dictionary)
	{
		const uint32 *offsets = (uint32 *) arrow->buffers[1];
		const char *values = (char *) arrow->buffers[2];

		const uint32 start = offsets[arrow_row];
		const uint32 end = offsets[arrow_row + 1];
		const uint32 arrow_len = end - start;

		*str = &values[start];
		return arrow_len;
	}

	const int16 dict_row = ((int16 *) arrow->buffers[1])[arrow_row];
	return arrow_get_str(arrow->dictionary, dict_row, str);
}

static void
decompress_generic_text_check_arrow(ArrowArray *arrow, int errorlevel, DecompressResult *results,
									int n)
{
	/* Check that both ways of decompression match. */
	if (n != arrow->length)
	{
		ereport(errorlevel,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the bulk decompression result does not match"),
				 errdetail("Expected %d elements, got %d.", n, (int) arrow->length)));
	}

	for (int i = 0; i < n; i++)
	{
		const bool arrow_isnull = !arrow_row_is_valid(arrow->buffers[0], i);
		if (arrow_isnull != results[i].is_null)
		{
			ereport(errorlevel,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the bulk decompression result does not match"),
					 errdetail("Expected null %d, got %d at row %d.",
							   results[i].is_null,
							   arrow_isnull,
							   i)));
		}

		if (!results[i].is_null)
		{
			const char *arrow_cstring;
			size_t arrow_len = arrow_get_str(arrow, i, &arrow_cstring);

			const Datum rowbyrow_varlena = results[i].val;
			const size_t rowbyrow_len = VARSIZE_ANY_EXHDR(rowbyrow_varlena);
			const char *rowbyrow_cstring = VARDATA_ANY(rowbyrow_varlena);

			if (rowbyrow_len != arrow_len)
			{
				ereport(errorlevel,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the bulk decompression result does not match"),
						 errdetail("At row %d\n", i)));
			}

			if (strncmp(arrow_cstring, rowbyrow_cstring, rowbyrow_len))
			{
				ereport(errorlevel,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the bulk decompression result does not match"),
						 errdetail("At row %d\n", i)));
			}
		}
	}
}

/*
 * Try to decompress the given compressed data.
 */
static int
decompress_generic_text(const uint8 *Data, size_t Size, bool bulk, int requested_algo)
{
	StringInfoData si = { .data = (char *) Data, .len = Size };

	const int data_algo = pq_getmsgbyte(&si);

	CheckCompressedData(data_algo > 0 && data_algo < _END_COMPRESSION_ALGORITHMS);

	if (data_algo != requested_algo)
	{
		/*
		 * It's convenient to fuzz only one algorithm at a time. We specialize
		 * the fuzz target for one algorithm, so that the fuzzer doesn't waste
		 * time discovering others from scratch.
		 */
		return -1;
	}
	const CompressionAlgorithmDefinition *def = algorithm_definition(data_algo);
	Datum compressed_data = def->compressed_data_recv(&si);
	DecompressAllFunction decompress_all = tsl_get_decompress_all_function(data_algo, TEXTOID);

	ArrowArray *arrow = NULL;
	if (bulk)
	{
		/*
		 * Check that the arrow decompression works. Have to do this before the
		 * row-by-row decompression so that it doesn't hide the possible errors.
		 */
		arrow = decompress_all(compressed_data, TEXTOID, CurrentMemoryContext);
	}

	/*
	 * Test row-by-row decompression.
	 */
	DecompressionIterator *iter = def->iterator_init_forward(compressed_data, TEXTOID);
	DecompressResult results[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	int n = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
		if (n >= GLOBAL_MAX_ROWS_PER_COMPRESSION)
		{
			elog(ERROR, "too many compressed rows");
		}

		results[n++] = r;
	}

	if (bulk)
	{
		/*
		 * Check that the arrow decompression result matches.
		 */
		decompress_generic_text_check_arrow(arrow, ERROR, results, n);
		return n;
	}

	/*
	 * For row-by-row decompression, check that the result is still the same
	 * after we compress and decompress back.
	 * Don't perform this check for other types of tests.
	 *
	 * 1) Compress.
	 */
	Compressor *compressor = def->compressor_for_type(TEXTOID);

	for (int i = 0; i < n; i++)
	{
		if (results[i].is_null)
		{
			compressor->append_null(compressor);
		}
		else
		{
			compressor->append_val(compressor, results[i].val);
		}
	}

	compressed_data = (Datum) compressor->finish(compressor);
	if (compressed_data == 0)
	{
		/* Some compressors return NULL when all rows are null. */
		return n;
	}

	/*
	 * 2) Decompress and check that it's the same.
	 */
	iter = def->iterator_init_forward(compressed_data, TEXTOID);
	int nn = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
		if (nn >= n)
		{
			elog(ERROR, "the repeated recompression result doesn't match");
		}

		if (r.is_null != results[nn].is_null)
		{
			elog(ERROR, "the repeated decompression result doesn't match");
		}

		if (!r.is_null)
		{
			const Datum old_value = results[nn].val;
			const Datum new_value = r.val;

			/*
			 * Floats can also be NaN/infinite and the comparison doesn't
			 * work in that case.
			 */
			if (VARSIZE_ANY_EXHDR(old_value) != VARSIZE_ANY_EXHDR(new_value))
			{
				elog(ERROR, "the repeated decompression result doesn't match");
			}

			if (strncmp(VARDATA_ANY(old_value),
						VARDATA_ANY(new_value),
						VARSIZE_ANY_EXHDR(new_value)) != 0)
			{
				elog(ERROR, "the repeated decompression result doesn't match");
			}
		}

		nn++;
	}

	/*
	 * 3) The bulk decompression must absolutely work on the correct compressed
	 * data we've just generated.
	 */
	arrow = decompress_all(compressed_data, TEXTOID, CurrentMemoryContext);
	decompress_generic_text_check_arrow(arrow, PANIC, results, n);

	return n;
}

int
decompress_ARRAY_TEXT(const uint8 *Data, size_t Size, bool bulk)
{
	return decompress_generic_text(Data, Size, bulk, COMPRESSION_ALGORITHM_ARRAY);
}

int
decompress_DICTIONARY_TEXT(const uint8 *Data, size_t Size, bool bulk)
{
	return decompress_generic_text(Data, Size, bulk, COMPRESSION_ALGORITHM_DICTIONARY);
}
