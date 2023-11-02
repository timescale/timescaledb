/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER3(X, Y, Z) X##_##Y##_##Z
#define FUNCTION_NAME3(X, Y, Z) FUNCTION_NAME_HELPER3(X, Y, Z)
#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME2(X, Y) FUNCTION_NAME_HELPER2(X, Y)

#define TOSTRING_HELPER(x) #x
#define TOSTRING(x) TOSTRING_HELPER(x)

static void
FUNCTION_NAME2(check_arrow, CTYPE)(ArrowArray *arrow, int error_type, DecompressResult *results,
								   int n)
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
			const CTYPE arrow_value = ((CTYPE *) arrow->buffers[1])[i];
			const CTYPE rowbyrow_value = DATUM_TO_CTYPE(results[i].val);

			/*
			 * Floats can also be NaN/infinite and the comparison doesn't
			 * work in that case.
			 */
			if (isfinite((double) arrow_value) != isfinite((double) rowbyrow_value))
			{
				ereport(error_type,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the bulk decompression result does not match"),
						 errdetail("At row %d\n", i)));
			}

			if (isfinite((double) arrow_value) && arrow_value != rowbyrow_value)
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
 * Try to decompress the given compressed data. Used for fuzzing and for checking
 * the examples found by fuzzing. For fuzzing we do less checks to keep it
 * faster and the coverage space smaller. This is a generic implementation
 * for arithmetic types.
 */
static int
FUNCTION_NAME3(decompress, ALGO, CTYPE)(const uint8 *Data, size_t Size,
										DecompressionTestType test_type)
{
	StringInfoData si = { .data = (char *) Data, .len = Size };

	const int algo = pq_getmsgbyte(&si);

	CheckCompressedData(algo > 0 && algo < _END_COMPRESSION_ALGORITHMS);

	if (algo != get_compression_algorithm(TOSTRING(ALGO)))
	{
		/*
		 * It's convenient to fuzz only one algorithm at a time. We specialize
		 * the fuzz target for one algorithm, so that the fuzzer doesn't waste
		 * time discovering others from scratch.
		 */
		return -1;
	}

	Datum compressed_data = definitions[algo].compressed_data_recv(&si);

	DecompressAllFunction decompress_all = tsl_get_decompress_all_function(algo, PGTYPE);

	if (test_type == DTT_Fuzzing)
	{
		/*
		 * For routine fuzzing, we only run bulk decompression to make it faster
		 * and the coverage space smaller.
		 */
		decompress_all(compressed_data, PGTYPE, CurrentMemoryContext);
		return 0;
	}

	ArrowArray *arrow = NULL;
	if (test_type == DTT_Bulk)
	{
		/*
		 * Test bulk decompression. Have to do this before row-by-row decompression
		 * so that the latter doesn't hide the errors.
		 */
		arrow = decompress_all(compressed_data, PGTYPE, CurrentMemoryContext);
	}

	/*
	 * Test row-by-row decompression.
	 */
	DecompressionIterator *iter = definitions[algo].iterator_init_forward(compressed_data, PGTYPE);
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

	/* Check that both ways of decompression match. */
	if (test_type == DTT_Bulk)
	{
		FUNCTION_NAME2(check_arrow, CTYPE)(arrow, ERROR, results, n);
		return n;
	}

	/*
	 * For row-by-row decompression, check that the result is still the same
	 * after we compress and decompress back.
	 *
	 * 1) Compress.
	 */
	Compressor *compressor = definitions[algo].compressor_for_type(PGTYPE);

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
	iter = definitions[algo].iterator_init_forward(compressed_data, PGTYPE);
	int nn = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
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
			if (isfinite((double) old_value) != isfinite((double) new_value))
			{
				elog(ERROR, "the repeated decompression result doesn't match");
			}

			if (isfinite((double) old_value) && old_value != new_value)
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

	/*
	 * 3) The bulk decompression must absolutely work on the correct compressed
	 * data we've just generated.
	 */
	arrow = decompress_all(compressed_data, PGTYPE, CurrentMemoryContext);
	FUNCTION_NAME2(check_arrow, CTYPE)(arrow, PANIC, results, n);

	return n;
}

#undef TOSTRING
#undef TOSTRING_HELPER

#undef FUNCTION_NAME3
#undef FUNCTION_NAME_HELPER3
#undef FUNCTION_NAME2
#undef FUNCTION_NAME_HELPER2
