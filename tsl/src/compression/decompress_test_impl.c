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
 * the examples found by fuzzing. For fuzzing we don't check that the
 * recompression result is the same.
 */
static int
FUNCTION_NAME(ALGO, CTYPE)(const uint8 *Data, size_t Size, bool check_compression)
{
	StringInfoData si = { .data = (char *) Data, .len = Size };

	int algo = pq_getmsgbyte(&si);

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

	Compressor *compressor = NULL;
	if (check_compression)
	{
		compressor = definitions[algo].compressor_for_type(PGTYPE);
	}

	Datum compressed_data = definitions[algo].compressed_data_recv(&si);
	DecompressionIterator *iter = definitions[algo].iterator_init_forward(compressed_data, PGTYPE);

	DecompressResult results[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	int n = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
		if (check_compression)
		{
			if (r.is_null)
			{
				compressor->append_null(compressor);
			}
			else
			{
				compressor->append_val(compressor, r.val);
			}
			results[n] = r;
		}

		n++;
	}

	if (!check_compression || n == 0)
	{
		return n;
	}

	compressed_data = (Datum) compressor->finish(compressor);
	if (compressed_data == 0)
	{
		/* The gorilla compressor returns NULL for all-null input sets. */
		return n;
	};

	/*
	 * Check that the result is still the same after we compress and decompress
	 * back.
	 */
	iter = definitions[algo].iterator_init_forward(compressed_data, PGTYPE);
	int nn = 0;
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
	{
		if (r.is_null != results[nn].is_null)
		{
			elog(ERROR, "the decompression result doesn't match");
		}

		if (!r.is_null && (DATUM_TO_CTYPE(r.val) != DATUM_TO_CTYPE(results[nn].val)))
		{
			elog(ERROR, "the decompression result doesn't match");
		}

		nn++;

		if (nn > n)
		{
			elog(ERROR, "the recompression result doesn't match");
		}
	}

	return n;
}

#undef TOSTRING
#undef TOSTRING_HELPER

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
