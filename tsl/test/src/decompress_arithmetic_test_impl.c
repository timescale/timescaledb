/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/algorithms/uuid_compress.h"
#define FUNCTION_NAME_HELPER3(X, Y, Z) X##_##Y##_##Z
#define FUNCTION_NAME3(X, Y, Z) FUNCTION_NAME_HELPER3(X, Y, Z)
#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME2(X, Y) FUNCTION_NAME_HELPER2(X, Y)

#define PG_TYPE_OID_HELPER(X) X##OID
#define PG_TYPE_OID_HELPER2(X) PG_TYPE_OID_HELPER(X)
#define PG_TYPE_OID PG_TYPE_OID_HELPER2(PG_TYPE_PREFIX)

#ifndef IS_NOT_EQUAL
#define IS_NOT_EQUAL(X, Y) ((X) != (Y))
#endif

#ifndef IS_FINITE
#define IS_FINITE(X) true
#endif

#ifndef ARROW_GET_VALUE
#define ARROW_GET_VALUE(A, I) ((CTYPE *) (A)->buffers[1])[I]
#endif

static void
FUNCTION_NAME3(compare_results, CTYPE, ALGO)(DecompressResult *fwd, DecompressResult *rev, int n)
{
	for (int i = 0; i < n; i++)
	{
		if (fwd[i].is_null != rev[i].is_null)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the forward and reverse decompression results do not match"),
					 errdetail("Forwards is_null %d, reverse is_null %d at row %d.",
							   fwd[i].is_null,
							   rev[i].is_null,
							   i)));
		}

		if (!fwd[i].is_null && IS_NOT_EQUAL(DATUM_TO_CTYPE(fwd[i].val), DATUM_TO_CTYPE(rev[i].val)))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the forward and reverse decompression results do not match"),
					 errdetail("At row %d\n", i)));
		}
	}
}

static void
FUNCTION_NAME3(check_arrow, CTYPE, ALGO)(ArrowArray *arrow, int error_type,
										 DecompressResult *results, int n)
{
	if (n != arrow->length)
	{
		ereport(error_type,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the bulk decompression result does not match"),
				 errdetail("Expected %d elements, got %d.", n, (int) arrow->length)));
	}

	for (int i = 0; i < n; i++)
	{
		const bool arrow_isnull = !arrow_row_is_valid(arrow->buffers[0], i);
		if (arrow_isnull != results[i].is_null)
		{
			ereport(error_type,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the bulk decompression result does not match"),
					 errdetail("Expected null %d, got %d at row %d.",
							   results[i].is_null,
							   arrow_isnull,
							   i)));
		}

		if (!results[i].is_null)
		{
			const CTYPE arrow_value = ARROW_GET_VALUE(arrow, i);
			const CTYPE rowbyrow_value = DATUM_TO_CTYPE(results[i].val);

			/*
			 * Floats can also be NaN/infinite and the comparison doesn't
			 * work in that case.
			 */
			if (IS_FINITE(arrow_value) != IS_FINITE(rowbyrow_value))
			{
				ereport(error_type,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the bulk decompression result does not match"),
						 errdetail("At row %d\n", i)));
			}

			if (IS_FINITE(arrow_value) && IS_NOT_EQUAL(arrow_value, rowbyrow_value))
			{
				ereport(error_type,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the bulk decompression result does not match"),
						 errdetail("At row %d\n", i)));
			}
		}
	}
}

/*
 * Try to decompress the given compressed data and compare the results
 * between the bulk, forward and reverse iterator. This is a generic
 * implementation for arithmetic types.
 */
static int
FUNCTION_NAME3(decompress, ALGO, PG_TYPE_PREFIX)(const uint8 *Data, size_t Size, bool bulk)
{
	StringInfoData si = { .data = (char *) Data, .len = Size };

	const int data_algo = pq_getmsgbyte(&si);

	CheckCompressedData(data_algo > 0 && data_algo < _END_COMPRESSION_ALGORITHMS);

	if (data_algo != FUNCTION_NAME2(COMPRESSION_ALGORITHM, ALGO))
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
	DecompressAllFunction decompress_all = tsl_get_decompress_all_function(data_algo, PG_TYPE_OID);

	ArrowArray *arrow = NULL;
	if (bulk)
	{
		/*
		 * Test bulk decompression. Have to do this before row-by-row decompression
		 * so that the latter doesn't hide the errors.
		 */
		arrow = decompress_all(compressed_data, PG_TYPE_OID, CurrentMemoryContext);
	}

	/*
	 * Test row-by-row decompression.
	 */
	DecompressionIterator *iter = def->iterator_init_forward(compressed_data, PG_TYPE_OID);
	DecompressResult results[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	memset(results, 0xFE, sizeof(results));

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
	 * Check that the reverse iterator also works and gives the same result
	 */
	DecompressionIterator *rev_iter = def->iterator_init_reverse(compressed_data, PG_TYPE_OID);
	DecompressResult results_rev[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	memset(results_rev, 0xEF, sizeof(results_rev));
	int rn = n - 1;

	for (DecompressResult r = rev_iter->try_next(rev_iter); !r.is_done;
		 r = rev_iter->try_next(rev_iter))
	{
		results_rev[rn--] = r;
	}

	/* Check that the iterator based decompression matches both ways. */
	if (bulk)
	{
		FUNCTION_NAME3(check_arrow, CTYPE, ALGO)(arrow, ERROR, results, n);
		FUNCTION_NAME3(check_arrow, CTYPE, ALGO)(arrow, ERROR, results_rev, n);
		return n;
	}
	else
	{
		FUNCTION_NAME3(compare_results, CTYPE, ALGO)(results, results_rev, n);
	}

	/*
	 * For row-by-row decompression, check that the result is still the same
	 * after we compress and decompress back.
	 *
	 * 1) Compress.
	 */
	Compressor *compressor = def->compressor_for_type(PG_TYPE_OID);

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
	iter = def->iterator_init_forward(compressed_data, PG_TYPE_OID);
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
			CTYPE old_value = DATUM_TO_CTYPE(results[nn].val);
			CTYPE new_value = DATUM_TO_CTYPE(r.val);
			/*
			 * Floats can also be NaN/infinite and the comparison doesn't
			 * work in that case.
			 */
			if (IS_FINITE(old_value) != IS_FINITE(new_value))
			{
				elog(ERROR, "the repeated decompression result doesn't match");
			}

			if (IS_FINITE(old_value) && IS_NOT_EQUAL(old_value, new_value))
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
	PG_TRY();
	{
		arrow = decompress_all(compressed_data, PG_TYPE_OID, CurrentMemoryContext);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		elog(PANIC, "bulk decompression failed for data that we've just compressed");
	}
	PG_END_TRY();

	FUNCTION_NAME3(check_arrow, CTYPE, ALGO)(arrow, PANIC, results, n);

	return n;
}

#undef FUNCTION_NAME3
#undef FUNCTION_NAME_HELPER3
#undef FUNCTION_NAME2
#undef FUNCTION_NAME_HELPER2

#undef PG_TYPE_OID
#undef PG_TYPE_OID_HELPER
#undef PG_TYPE_OID_HELPER2

/* These are defined by the caller */
#undef ALGO
#undef CTYPE
#undef PG_TYPE_PREFIX
#undef DATUM_TO_CTYPE
#undef IS_FINITE
#undef IS_NOT_EQUAL
#undef ARROW_GET_VALUE
