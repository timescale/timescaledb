/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X, Y) decompress_##X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

#define TOSTRING_HELPER(x) #x
#define TOSTRING(x) TOSTRING_HELPER(x)

/*
 * Try to decompress the given compressed data. Used for fuzzing and for checking
 * the examples found by fuzzing. For fuzzing we do less checks to keep it
 * faster and the coverage space smaller.
 */
static int
FUNCTION_NAME(ALGO, CTYPE)(const uint8 *Data, size_t Size, bool extra_checks)
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

	if (!extra_checks)
	{
		/*
		 * For routine fuzzing, we only run bulk decompression to make it faster
		 * and the coverage space smaller.
		 */
		DecompressAllFunction decompress_all = tsl_get_decompress_all_function(algo);
		decompress_all(compressed_data, PGTYPE, CurrentMemoryContext);
		return 0;
	}

	/*
	 * Test bulk decompression. This might hide some errors in the row-by-row
	 * decompression, but testing both is significantly more complicated, and
	 * the row-by-row is old and stable.
	 */
	ArrowArray *arrow = NULL;
	DecompressAllFunction decompress_all = tsl_get_decompress_all_function(algo);
	if (decompress_all)
	{
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
	if (arrow)
	{
		if (n != arrow->length)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the bulk decompression result does not match"),
					 errdetail("Expected %d elements, got %d.", n, (int) arrow->length)));
		}

		for (int i = 0; i < n; i++)
		{
			const bool arrow_isnull = !arrow_row_is_valid(arrow->buffers[0], i);
			if (arrow_isnull != results[i].is_null)
			{
				ereport(ERROR,
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
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("the bulk decompression result does not match"),
							 errdetail("At row %d\n", i)));
				}

				if (isfinite((double) arrow_value) && arrow_value != rowbyrow_value)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("the bulk decompression result does not match"),
							 errdetail("At row %d\n", i)));
				}
			}
		}
	}

	/*
	 * Check that the result is still the same after we compress and decompress
	 * back.
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

	return n;
}

#undef TOSTRING
#undef TOSTRING_HELPER

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
