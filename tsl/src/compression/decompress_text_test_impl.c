/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <libpq/pqformat.h>

#include "compression.h"

#include "compression_test.h"

/*
 * Try to decompress the given compressed data. Used for fuzzing and for checking
 * the examples found by fuzzing. For fuzzing we do less checks to keep it
 * faster and the coverage space smaller. This is a generic implementation
 * for arithmetic types.
 */
static int
decompress_generic_text(const uint8 *Data, size_t Size, DecompressionTestType test_type,
						int requested_algo)
{
	if (!(test_type == DTT_RowByRow))
	{
		elog(ERROR, "decompression test type %d not supported for text", test_type);
	}

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

	/*
	 * For row-by-row decompression, check that the result is still the same
	 * after we compress and decompress back.
	 * Don't perform this check for other types of tests.
	 */
	if (test_type != DTT_RowByRow)
	{
		return n;
	}

	/*
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
		/* The gorilla compressor returns NULL for all-null input sets. */
		return n;
	};

	/*
	 * 2) Decompress and check that it's the same.
	 */
	iter = def->iterator_init_forward(compressed_data, TEXTOID);
	int nn = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
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
						VARSIZE_ANY_EXHDR(new_value)))
			{
				elog(ERROR, "the repeated decompression result doesn't match");
			}
		}

		nn++;

		if (nn > n)
		{
			elog(ERROR, "the repeated recompression result doesn't match");
		}
	}

	return n;
}

int
decompress_ARRAY_TEXT(const uint8 *Data, size_t Size, DecompressionTestType test_type)
{
	return decompress_generic_text(Data, Size, test_type, COMPRESSION_ALGORITHM_ARRAY);
}

int
decompress_DICTIONARY_TEXT(const uint8 *Data, size_t Size, DecompressionTestType test_type)
{
	return decompress_generic_text(Data, Size, test_type, COMPRESSION_ALGORITHM_DICTIONARY);
}
