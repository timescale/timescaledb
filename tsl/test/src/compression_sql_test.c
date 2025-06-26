/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <math.h>

#include <postgres.h>

#include <funcapi.h>
#include <libpq/pqformat.h>
#include <utils/builtins.h>

#include "compression_sql_test.h"

#include "compression/arrow_c_data_interface.h"

#if !defined(NDEBUG) || defined(TS_COMPRESSION_FUZZING)

static int
get_compression_algorithm(char *name)
{
	if (pg_strcasecmp(name, "deltadelta") == 0)
	{
		return COMPRESSION_ALGORITHM_DELTADELTA;
	}
	else if (pg_strcasecmp(name, "gorilla") == 0)
	{
		return COMPRESSION_ALGORITHM_GORILLA;
	}
	else if (pg_strcasecmp(name, "array") == 0)
	{
		return COMPRESSION_ALGORITHM_ARRAY;
	}
	else if (pg_strcasecmp(name, "dictionary") == 0)
	{
		return COMPRESSION_ALGORITHM_DICTIONARY;
	}
	else if (pg_strcasecmp(name, "bool") == 0)
	{
		return COMPRESSION_ALGORITHM_BOOL;
	}
	else if (pg_strcasecmp(name, "uuid") == 0)
	{
		return COMPRESSION_ALGORITHM_UUID;
	}

	ereport(ERROR, (errmsg("unknown compression algorithm %s", name)));
	return _INVALID_COMPRESSION_ALGORITHM;
}

/*
 * Specializations of test functions for arithmetic types.
 */
#define ALGO GORILLA
#define CTYPE float8
#define PG_TYPE_PREFIX FLOAT8
#define DATUM_TO_CTYPE DatumGetFloat8
#include "decompress_arithmetic_test_impl.c"
#undef ALGO
#undef CTYPE
#undef PG_TYPE_PREFIX
#undef DATUM_TO_CTYPE

#define ALGO DELTADELTA
#define CTYPE int64
#define PG_TYPE_PREFIX INT8
#define DATUM_TO_CTYPE DatumGetInt64
#include "decompress_arithmetic_test_impl.c"
#undef ALGO
#undef CTYPE
#undef PG_TYPE_PREFIX
#undef DATUM_TO_CTYPE

/*
 * The table of the supported testing configurations. We use it to generate
 * dispatch tables and specializations of test functions.
 */
#define APPLY_FOR_TYPES(X)                                                                         \
	X(GORILLA, FLOAT8, true)                                                                       \
	X(GORILLA, FLOAT8, false)                                                                      \
	X(DELTADELTA, INT8, true)                                                                      \
	X(DELTADELTA, INT8, false)                                                                     \
	X(ARRAY, TEXT, false)                                                                          \
	X(ARRAY, TEXT, true)                                                                           \
	X(DICTIONARY, TEXT, false)                                                                     \
	X(DICTIONARY, TEXT, true)

static int (*get_decompress_fn(int algo, Oid type))(const uint8 *Data, size_t Size, bool bulk)
{
#define DISPATCH(ALGO, PGTYPE, BULK)                                                               \
	if (algo == COMPRESSION_ALGORITHM_##ALGO && type == PGTYPE##OID)                               \
	{                                                                                              \
		return decompress_##ALGO##_##PGTYPE;                                                       \
	}

	APPLY_FOR_TYPES(DISPATCH)

	elog(ERROR,
		 "no decompression function for compression algorithm %d with element type %d",
		 algo,
		 type);
	pg_unreachable();
#undef DISPATCH
}

/*
 * Read and decompress compressed data from file. Useful for debugging the
 * results of fuzzing.
 * The out parameter bytes is volatile because we want to fill it even
 * if we error out later.
 */
static void
read_compressed_data_file_impl(int algo, Oid type, const char *path, bool bulk, volatile int *bytes,
							   int *rows)
{
	FILE *f = fopen(path, "r");

	if (!f)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE), errmsg("could not open the file '%s'", path)));
	}

	fseek(f, 0, SEEK_END);
	const size_t fsize = ftell(f);
	fseek(f, 0, SEEK_SET); /* same as rewind(f); */

	*rows = 0;
	*bytes = fsize;

	if (fsize == 0)
	{
		/*
		 * Skip empty data, because we'll just get "no data left in message"
		 * right away.
		 */
		fclose(f);
		return;
	}

	char *string = palloc(fsize + 1);
	size_t elements_read = fread(string, fsize, 1, f);

	if (elements_read != 1)
	{
		fclose(f);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("failed to read file '%s'", path)));
	}

	fclose(f);

	string[fsize] = 0;

	*rows = get_decompress_fn(algo, type)((const uint8 *) string, fsize, bulk);
}

TS_FUNCTION_INFO_V1(ts_read_compressed_data_file);

/* Read and decompress compressed data from file -- SQL-callable wrapper. */
Datum
ts_read_compressed_data_file(PG_FUNCTION_ARGS)
{
	int rows;
	int bytes;
	read_compressed_data_file_impl(get_compression_algorithm(PG_GETARG_CSTRING(0)),
								   PG_GETARG_OID(1),
								   PG_GETARG_CSTRING(2),
								   PG_GETARG_BOOL(3),
								   &bytes,
								   &rows);
	PG_RETURN_INT32(rows);
}

TS_FUNCTION_INFO_V1(ts_read_compressed_data_directory);

/*
 * Read and decomrpess all compressed data files from directory. Useful for
 * checking the fuzzing corpuses in the regression tests.
 */
Datum
ts_read_compressed_data_directory(PG_FUNCTION_ARGS)
{
	/* Output columns of this function. */
	enum
	{
		out_path = 0,
		out_bytes,
		out_rows,
		out_sqlstate,
		out_location,
		_out_columns
	};

	/* Cross-call context for this set-returning function. */
	struct user_context
	{
		DIR *dp;
		struct dirent *ep;
	};

	char *name = PG_GETARG_CSTRING(2);
	const int algo = get_compression_algorithm(PG_GETARG_CSTRING(0));

	FuncCallContext *funcctx;
	struct user_context *c;
	MemoryContext call_memory_context = CurrentMemoryContext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/*
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		funcctx->attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);

		funcctx->user_fctx = palloc(sizeof(struct user_context));
		c = funcctx->user_fctx;

		c->dp = opendir(name);

		if (!c->dp)
		{
			elog(ERROR, "could not open directory '%s'", name);
		}

		MemoryContextSwitchTo(call_memory_context);
	}

	funcctx = SRF_PERCALL_SETUP();
	c = (struct user_context *) funcctx->user_fctx;

	Datum values[_out_columns] = { 0 };
	bool nulls[_out_columns] = { 0 };
	for (int i = 0; i < _out_columns; i++)
	{
		nulls[i] = true;
	}

	while ((c->ep = readdir(c->dp)))
	{
		if (c->ep->d_name[0] == '.')
		{
			continue;
		}

		char *path = psprintf("%s/%s", name, c->ep->d_name);

		/* The return values are: path, ret, sqlstate, status, location. */
		values[out_path] = PointerGetDatum(cstring_to_text(path));
		nulls[out_path] = false;

		int rows;
		volatile int bytes = 0;
		PG_TRY();
		{
			read_compressed_data_file_impl(algo,
										   PG_GETARG_OID(1),
										   path,
										   PG_GETARG_BOOL(3),
										   &bytes,
										   &rows);
			values[out_rows] = Int32GetDatum(rows);
			nulls[out_rows] = false;
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(call_memory_context);

			ErrorData *error = CopyErrorData();
			FlushErrorState();

			values[out_sqlstate] =
				PointerGetDatum(cstring_to_text(unpack_sql_state(error->sqlerrcode)));
			nulls[out_sqlstate] = false;

			if (error->filename)
			{
				values[out_location] = PointerGetDatum(
					cstring_to_text(psprintf("%s:%d", error->filename, error->lineno)));
				nulls[out_location] = false;
			}
			FreeErrorData(error);
		}
		PG_END_TRY();

		values[out_bytes] = Int32GetDatum(bytes);
		nulls[out_bytes] = false;

		SRF_RETURN_NEXT(funcctx,
						HeapTupleGetDatum(heap_form_tuple(funcctx->tuple_desc, values, nulls)));
	}

	(void) closedir(c->dp);

	SRF_RETURN_DONE(funcctx);
}

#endif

#ifdef TS_COMPRESSION_FUZZING

/*
 * Fuzzing target for all supported types.
 */
static int
target_generic(const uint8 *Data, size_t Size, CompressionAlgorithm requested_algo, Oid pg_type,
			   bool bulk)
{
	StringInfoData si = { .data = (char *) Data, .len = Size };

	const CompressionAlgorithm data_algo = pq_getmsgbyte(&si);

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

	if (bulk)
	{
		DecompressAllFunction decompress_all = tsl_get_decompress_all_function(data_algo, pg_type);
		decompress_all(compressed_data, pg_type, CurrentMemoryContext);
		return 0;
	}

	DecompressionIterator *iter = def->iterator_init_forward(compressed_data, pg_type);
	for (DecompressResult r = iter->try_next(iter); !r.is_done; r = iter->try_next(iter))
		;
	return 0;
}

/*
 * This is a wrapper for fuzzing target. It will called by the libfuzzer driver.
 * It has to catch the postgres exceptions normally produced for corrupt data.
 */
static int
target_wrapper(const uint8_t *Data, size_t Size, CompressionAlgorithm requested_algo, Oid pg_type,
			   bool bulk)
{
	MemoryContextReset(CurrentMemoryContext);

	int res = 0;
	PG_TRY();
	{
		CHECK_FOR_INTERRUPTS();
		res = target_generic(Data, Size, requested_algo, pg_type, bulk);
	}
	PG_CATCH();
	{
		/* EmitErrorReport(); */
		FlushErrorState();
	}
	PG_END_TRY();

	/*
	 * -1 means "don't include it into corpus", return it if the test function
	 * says so, otherwise return 0. Some test functions also returns the number
	 * of rows for the correct data, the fuzzer doesn't understand these values.
	 */
	return res == -1 ? -1 : 0;
}

/*
 * Specializations of fuzzing targets for supported types that will be directly
 * called by the fuzzing driver.
 */
#define DECLARE_TARGET(ALGO, PGTYPE, BULK)                                                         \
	static int target_##ALGO##_##PGTYPE##_##BULK(const uint8_t *D, size_t S)                       \
	{                                                                                              \
		return target_wrapper(D, S, COMPRESSION_ALGORITHM_##ALGO, PGTYPE##OID, BULK);              \
	}

APPLY_FOR_TYPES(DECLARE_TARGET)

#undef DECLARE_TARGET

/*
 * libfuzzer fuzzing driver that we import from LLVM libraries. It will run our
 * test functions with random inputs.
 */
extern int LLVMFuzzerRunDriver(int *argc, char ***argv,
							   int (*UserCb)(const uint8_t *Data, size_t Size));

/*
 * The SQL function to perform fuzzing.
 */
TS_FUNCTION_INFO_V1(ts_fuzz_compression);

Datum
ts_fuzz_compression(PG_FUNCTION_ARGS)
{
	/*
	 * We use the memory context size larger than default here, so that all data
	 * allocated by fuzzing fit into the first chunk. The first chunk is not
	 * deallocated when the memory context is reset, so this reduces overhead
	 * caused by repeated reallocations.
	 * The particular value of 8MB is somewhat arbitrary and large. In practice,
	 * we have inputs of 1k rows max here, which decompress to 8 kB max.
	 */
	MemoryContext fuzzing_context =
		AllocSetContextCreate(CurrentMemoryContext, "fuzzing", 0, 8 * 1024 * 1024, 8 * 1024 * 1024);
	MemoryContext old_context = MemoryContextSwitchTo(fuzzing_context);

	char *argvdata[] = { "PostgresFuzzer",
						 "-timeout=1",
						 "-report_slow_units=1",
						 // "-use_value_profile=1",
						 "-reload=1",
						 //"-print_coverage=1",
						 //"-print_full_coverage=1",
						 //"-print_final_stats=1",
						 //"-help=1",
						 psprintf("-runs=%d", PG_GETARG_INT32(3)),
						 "corpus" /* in the database directory */,
						 NULL };
	char **argv = argvdata;
	int argc = sizeof(argvdata) / sizeof(*argvdata) - 1;

	int algo = get_compression_algorithm(PG_GETARG_CSTRING(0));
	Oid type = PG_GETARG_OID(1);
	bool bulk = PG_GETARG_BOOL(2);

	int (*target)(const uint8_t *, size_t) = NULL;

#define DISPATCH(ALGO, PGTYPE, BULK)                                                               \
	if (algo == COMPRESSION_ALGORITHM_##ALGO && type == PGTYPE##OID && bulk == BULK)               \
	{                                                                                              \
		target = target_##ALGO##_##PGTYPE##_##BULK;                                                \
	}

	APPLY_FOR_TYPES(DISPATCH)
#undef DISPATCH

	if (target == NULL)
	{
		elog(ERROR, "no llvm fuzz target for compression algorithm %d and type %d", algo, type);
	}

	int res = LLVMFuzzerRunDriver(&argc, &argv, target);

	MemoryContextSwitchTo(old_context);

	PG_RETURN_INT32(res);
}

#endif
