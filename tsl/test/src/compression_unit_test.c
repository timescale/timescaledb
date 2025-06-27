/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <access/heapam.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <guc.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "test_utils.h"
#include "ts_catalog/catalog.h"
#include <export.h>

#include "compression/algorithms/array.h"
#include "compression/algorithms/bool_compress.h"
#include "compression/algorithms/deltadelta.h"
#include "compression/algorithms/dictionary.h"
#include "compression/algorithms/float_utils.h"
#include "compression/algorithms/gorilla.h"
#include "compression/algorithms/null.h"
#include "compression/algorithms/simple8b_rle.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/batch_metadata_builder_minmax.h"

#define TEST_ELEMENTS 1015

TS_FUNCTION_INFO_V1(ts_test_compression);

static void
test_int_array()
{
	ArrayCompressor *compressor = array_compressor_alloc(INT4OID);
	ArrayCompressed *compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < TEST_ELEMENTS; i++)
		array_compressor_append(compressor, Int32GetDatum(i));

	compressed = array_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);

	i = 0;
	iter =
		tsl_array_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), INT4OID);
	for (DecompressResult r = array_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt32(r.val), i);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);

	iter =
		tsl_array_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), INT4OID);
	for (DecompressResult r = array_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_reverse(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt32(r.val), i - 1);
		i -= 1;
	}
	TestAssertInt64Eq(i, 0);
}

static void
test_string_array()
{
	ArrayCompressor *compressor = array_compressor_alloc(TEXTOID);
	ArrayCompressed *compressed;
	DecompressionIterator *iter;
	char *strings[5] = { "a", "foo", "bar", "gobble gobble gobble", "baz" };
	text *texts[5];
	int i;
	for (i = 0; i < 5; i++)
		texts[i] = cstring_to_text(strings[i]);

	for (i = 0; i < TEST_ELEMENTS; i++)
		array_compressor_append(compressor, PointerGetDatum(texts[i % 5]));

	compressed = array_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);

	i = 0;
	iter =
		tsl_array_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), TEXTOID);
	for (DecompressResult r = array_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[i % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);

	iter =
		tsl_array_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), TEXTOID);
	for (DecompressResult r = array_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_reverse(iter))
	{
		TestAssertTrue(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[(i - 1) % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i -= 1;
	}
	TestAssertInt64Eq(i, 0);
}

static void
test_int_dictionary()
{
	DictionaryCompressor *compressor = dictionary_compressor_alloc(INT4OID);
	DictionaryCompressed *compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < TEST_ELEMENTS; i++)
		dictionary_compressor_append(compressor, Int32GetDatum(i % 15));

	compressed = dictionary_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);

	i = 0;
	iter = tsl_dictionary_decompression_iterator_from_datum_forward(PointerGetDatum(compressed),
																	INT4OID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt32(r.val), i % 15);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);
}

static void
test_string_dictionary()
{
	DictionaryCompressor *compressor = dictionary_compressor_alloc(TEXTOID);
	DictionaryCompressed *compressed;
	DecompressionIterator *iter;
	char *strings[5] = { "a", "foo", "bar", "gobble gobble gobble", "baz" };
	text *texts[5];
	int i;
	for (i = 0; i < 5; i++)
		texts[i] = cstring_to_text(strings[i]);

	for (i = 0; i < 1014; i++)
		dictionary_compressor_append(compressor, PointerGetDatum(texts[i % 5]));

	compressed = dictionary_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);

	i = 0;
	iter = tsl_dictionary_decompression_iterator_from_datum_forward(PointerGetDatum(compressed),
																	TEXTOID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[i % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i += 1;
	}

	TestAssertInt64Eq(i, 1014);
	iter = tsl_dictionary_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed),
																	TEXTOID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_reverse(iter))
	{
		TestAssertTrue(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[(i - 1) % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i -= 1;
	}
	TestAssertInt64Eq(i, 0);

	TestEnsureError(dictionary_compressor_alloc(CSTRINGOID));
}

static void
test_gorilla_int()
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;
	DecompressionIterator *iter;
	uint32 i;
	for (i = 0; i < TEST_ELEMENTS; i++)
		gorilla_compressor_append_value(compressor, i);

	compressed = gorilla_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);
	TestAssertInt64Eq(VARSIZE(compressed), 1344);

	i = 0;
	iter = gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), INT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);

	iter = gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), INT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_reverse(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt64(r.val), i - 1);
		i -= 1;
	}
	TestAssertInt64Eq(i, 0);

	{
		StringInfoData buf;
		bytea *sent;
		StringInfoData transmission;
		GorillaCompressed *compressed_recv;

		pq_begintypsend(&buf);
		gorilla_compressed_send((CompressedDataHeader *) compressed, &buf);
		sent = pq_endtypsend(&buf);

		transmission = (StringInfoData){
			.data = VARDATA(sent),
			.len = VARSIZE(sent),
			.maxlen = VARSIZE(sent),
		};

		compressed_recv =
			(GorillaCompressed *) DatumGetPointer(gorilla_compressed_recv(&transmission));
		iter = gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed_recv),
																 INT8OID);
		for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
			 r = gorilla_decompression_iterator_try_next_forward(iter))
		{
			TestAssertTrue(!r.is_null);
			TestAssertInt64Eq(DatumGetInt64(r.val), i);
			i += 1;
		}
		TestAssertInt64Eq(i, TEST_ELEMENTS);
	}
}

static void
test_gorilla_float()
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;
	DecompressionIterator *iter;
	for (int x = 0; x < TEST_ELEMENTS; x++)
		gorilla_compressor_append_value(compressor, float_get_bits((float) x));

	compressed = gorilla_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);
	TestAssertInt64Eq(VARSIZE(compressed), 1200);

	float i = 0;
	iter =
		gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), FLOAT4OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertDoubleEq(DatumGetFloat4(r.val), i);
		i += 1.0;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);

	iter =
		gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), FLOAT4OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_reverse(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetFloat4(r.val), i - 1);
		i -= 1;
	}
	TestAssertInt64Eq(i, 0);
}

static uint64
test_hash64(uint64 x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9U;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebU;
	x ^= x >> 31;
	return x;
}

static void
test_gorilla_double(bool have_nulls, bool have_random)
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;

	double values[TEST_ELEMENTS];
	bool nulls[TEST_ELEMENTS];
	for (int i = 0; i < TEST_ELEMENTS; i++)
	{
		if (have_random)
		{
			/* Also add some stretches of equal numbers. */
			int base = i;
			if (i % 37 < 3)
			{
				base = 1;
			}
			else if (i % 53 < 2)
			{
				base = 2;
			}

			values[i] = (test_hash64(base) / (double) PG_UINT64_MAX) * 100.;
		}
		else
		{
			values[i] = i;
		}

		if (have_nulls && i % 29 == 0)
		{
			nulls[i] = true;
		}
		else
		{
			nulls[i] = false;
		}

		if (nulls[i])
		{
			gorilla_compressor_append_null(compressor);
		}
		else
		{
			gorilla_compressor_append_value(compressor, double_get_bits(values[i]));
		}
	}

	compressed = gorilla_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);
	if (!have_nulls && !have_random)
	{
		TestAssertInt64Eq(VARSIZE(compressed), 1200);
	}

	/* Forward decompression. */
	DecompressionIterator *iter =
		gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), FLOAT8OID);
	ArrowArray *bulk_result =
		gorilla_decompress_all(PointerGetDatum(compressed), FLOAT8OID, CurrentMemoryContext);
	for (int i = 0; i < TEST_ELEMENTS; i++)
	{
		DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter);
		TestAssertTrue(!r.is_done);
		if (r.is_null)
		{
			TestAssertTrue(nulls[i]);
			TestAssertTrue(!arrow_row_is_valid(bulk_result->buffers[0], i));
		}
		else
		{
			TestAssertTrue(!nulls[i]);
			TestAssertTrue(arrow_row_is_valid(bulk_result->buffers[0], i));
			TestAssertTrue(values[i] == DatumGetFloat8(r.val));
			TestAssertTrue(values[i] == ((double *) bulk_result->buffers[1])[i]);
		}
	}
	DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter);
	TestAssertTrue(r.is_done);

	/* Reverse decompression. */
	iter =
		gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), FLOAT8OID);
	for (int i = TEST_ELEMENTS - 1; i >= 0; i--)
	{
		DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter);
		TestAssertTrue(!r.is_done);
		if (r.is_null)
		{
			TestAssertTrue(nulls[i]);
		}
		else
		{
			TestAssertTrue(!nulls[i]);
			TestAssertTrue(values[i] == DatumGetFloat8(r.val));
		}
	}
	r = gorilla_decompression_iterator_try_next_reverse(iter);
	TestAssertTrue(r.is_done);
}

static void
test_delta()
{
	DeltaDeltaCompressor *compressor = delta_delta_compressor_alloc();
	Datum compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < TEST_ELEMENTS; i++)
		delta_delta_compressor_append_value(compressor, i);

	compressed = DirectFunctionCall1(tsl_deltadelta_compressor_finish, PointerGetDatum(compressor));
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	TestAssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), 56);

	i = 0;
	iter = delta_delta_decompression_iterator_from_datum_forward(compressed, INT8OID);
	for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = delta_delta_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);
}

static void
test_delta2()
{
	DeltaDeltaCompressor *compressor = delta_delta_compressor_alloc();
	Datum compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < TEST_ELEMENTS; i++)
	{
		/* prevent everything from being rle'd away */
		if (i % 2 != 0)
			delta_delta_compressor_append_value(compressor, 2 * i);
		else
			delta_delta_compressor_append_value(compressor, i);
	}

	compressed = DirectFunctionCall1(tsl_deltadelta_compressor_finish, PointerGetDatum(compressor));
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	TestAssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), 1664);

	i = 0;
	iter = delta_delta_decompression_iterator_from_datum_forward(compressed, INT8OID);
	for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = delta_delta_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		if (i % 2 != 0)
			TestAssertInt64Eq(DatumGetInt64(r.val), 2 * i);
		else
			TestAssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	TestAssertInt64Eq(i, TEST_ELEMENTS);
}

static void
test_delta3(bool have_nulls, bool have_random)
{
	DeltaDeltaCompressor *compressor = delta_delta_compressor_alloc();
	Datum compressed;

	int64 values[TEST_ELEMENTS];
	bool nulls[TEST_ELEMENTS];
	for (int i = 0; i < TEST_ELEMENTS; i++)
	{
		if (have_random)
		{
			/* Also add some stretches of equal numbers. */
			int base = i;
			if (i % 37 < 4)
			{
				base = 1;
			}
			else if (i % 53 < 2)
			{
				base = 2;
			}

			values[i] = test_hash64(base);
		}
		else
		{
			values[i] = i;
		}

		if (have_nulls && i % 29 == 0)
		{
			nulls[i] = true;
		}
		else
		{
			nulls[i] = false;
		}

		if (nulls[i])
		{
			delta_delta_compressor_append_null(compressor);
		}
		else
		{
			delta_delta_compressor_append_value(compressor, values[i]);
		}
	}

	compressed = PointerGetDatum(delta_delta_compressor_finish(compressor));
	TestAssertTrue(DatumGetPointer(compressed) != NULL);

	/* Forward decompression. */
	DecompressionIterator *iter =
		delta_delta_decompression_iterator_from_datum_forward(compressed, INT8OID);
	ArrowArray *bulk_result = delta_delta_decompress_all(compressed, INT8OID, CurrentMemoryContext);
	for (int i = 0; i < TEST_ELEMENTS; i++)
	{
		DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter);
		TestAssertTrue(!r.is_done);
		if (r.is_null)
		{
			TestAssertTrue(nulls[i]);
			TestAssertTrue(!arrow_row_is_valid(bulk_result->buffers[0], i));
		}
		else
		{
			TestAssertTrue(!nulls[i]);
			TestAssertTrue(arrow_row_is_valid(bulk_result->buffers[0], i));
			TestAssertTrue(values[i] == DatumGetInt64(r.val));
			TestAssertTrue(values[i] == ((int64 *) bulk_result->buffers[1])[i]);
		}
	}
	DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter);
	TestAssertTrue(r.is_done);

	/* Reverse decompression. */
	iter = delta_delta_decompression_iterator_from_datum_reverse(compressed, INT8OID);
	for (int i = TEST_ELEMENTS - 1; i >= 0; i--)
	{
		DecompressResult r = delta_delta_decompression_iterator_try_next_reverse(iter);
		TestAssertTrue(!r.is_done);
		if (r.is_null)
		{
			TestAssertTrue(nulls[i]);
		}
		else
		{
			TestAssertTrue(!nulls[i]);
			TestAssertTrue(values[i] == DatumGetInt64(r.val));
		}
	}
	r = delta_delta_decompression_iterator_try_next_reverse(iter);
	TestAssertTrue(r.is_done);
}

static int32 test_delta4_case1[] = { -603979776, 1462059044 };

static int32 test_delta4_case2[] = {
	0x7979fd07, 0x79797979, 0x79797979, 0x79797979, 0x79797979, 0x79797979, 0x79797979,
	0x79797979, 0x79797979, 0x79797979, 0x79797979, 0x79797979, 0x79797979, 0x79797979,
	0x79797979, 0x50505050, 0xc4c4c4c4, 0xc4c4c4c4, 0x50505050, 0x50505050, 0xc4c4c4c4,
};

static void
test_delta4(const int32 *values, int n)
{
	Compressor *compressor = delta_delta_compressor_for_type(INT4OID);
	for (int i = 0; i < n; i++)
	{
		compressor->append_val(compressor, Int32GetDatum(values[i]));
	}
	Datum compressed = (Datum) compressor->finish(compressor);

	ArrowArray *arrow = delta_delta_decompress_all(compressed, INT4OID, CurrentMemoryContext);
	DecompressionIterator *iter =
		delta_delta_decompression_iterator_from_datum_forward(compressed, INT4OID);
	int i = 0;
	for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = delta_delta_decompression_iterator_try_next_forward(iter))
	{
		TestAssertTrue(!r.is_null);
		TestAssertTrue(i < arrow->length);
		TestAssertTrue(((int32 *) arrow->buffers[1])[i] == DatumGetInt32(r.val));
		TestAssertTrue(arrow_row_is_valid(arrow->buffers[0], i));
		TestAssertTrue(values[i] == DatumGetInt32(r.val));
		i++;
	}
	TestAssertTrue(i == arrow->length);
	TestAssertTrue(i == n);
}

static void
test_bool_rle(bool nulls, int run_length, int expected_size)
{
	Compressor *compressor = bool_compressor_for_type(BOOLOID);
	int rlen = run_length;
	bool val = true;
	int64 compressed_null_count = 0;
	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		if (rlen == 0)
		{
			if (nulls)
			{
				compressor->append_null(compressor);
				++compressed_null_count;
			}
			else
				compressor->append_val(compressor, BoolGetDatum(val));
			rlen = run_length;
			val = !val;
		}
		else
		{
			compressor->append_val(compressor, BoolGetDatum(val));
			--rlen;
		}
	}

	Datum compressed = (Datum) compressor->finish(compressor);
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	TestAssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), expected_size);

	rlen = run_length;
	val = true;
	DecompressionIterator *iter =
		bool_decompression_iterator_from_datum_forward(compressed, BOOLOID);
	ArrowArray *bulk_result = bool_decompress_all(compressed, BOOLOID, CurrentMemoryContext);
	const uint64 *bulk_data = bulk_result->buffers[1];
	int64 decompressed_null_count = 0;

	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		DecompressResult r = bool_decompression_iterator_try_next_forward(iter);
		TestAssertTrue(!r.is_done);
		if (rlen == 0)
		{
			if (nulls)
			{
				TestAssertTrue(!arrow_row_is_valid(bulk_result->buffers[0], i));
				TestAssertTrue(r.is_null);
				++decompressed_null_count;
			}
			else
			{
				TestAssertTrue(arrow_row_is_valid(bulk_result->buffers[0], i));
				TestAssertTrue(DatumGetBool(r.val) == val);
				const int16 block = i / 64;
				const int16 offset = i % 64;
				TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			}
			rlen = run_length;
			val = !val;
		}
		else
		{
			TestAssertTrue(arrow_row_is_valid(bulk_result->buffers[0], i));
			TestAssertTrue(r.is_null == false);
			TestAssertTrue(DatumGetBool(r.val) == val);
			const int16 block = i / 64;
			const int16 offset = i % 64;
			TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			--rlen;
		}
	}

	TestAssertInt64Eq(decompressed_null_count, compressed_null_count);
	TestAssertInt64Eq(bulk_result->null_count, compressed_null_count);

	DecompressResult r = bool_decompression_iterator_try_next_forward(iter);
	TestAssertTrue(r.is_done);
}

static void
test_bool_array(bool nulls, int run_length, int expected_size)
{
	Compressor *compressor = array_compressor_for_type(BOOLOID);
	int rlen = run_length;
	bool val = true;
	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		if (rlen == 0)
		{
			if (nulls)
				compressor->append_null(compressor);
			else
				compressor->append_val(compressor, BoolGetDatum(val));
			rlen = run_length;
			val = !val;
		}
		else
		{
			compressor->append_val(compressor, BoolGetDatum(val));
			--rlen;
		}
	}

	Datum compressed = (Datum) compressor->finish(compressor);
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	TestAssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), expected_size);

	rlen = run_length;
	val = true;
	DecompressionIterator *iter =
		tsl_array_decompression_iterator_from_datum_forward(compressed, BOOLOID);

	ArrowArray *bulk_result = tsl_array_decompress_all(compressed, BOOLOID, CurrentMemoryContext);
	const uint64 *bulk_data = bulk_result->buffers[1];

	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		DecompressResult r = array_decompression_iterator_try_next_forward(iter);
		TestAssertTrue(!r.is_done);
		if (rlen == 0)
		{
			if (nulls)
			{
				TestAssertTrue(!arrow_row_is_valid(bulk_result->buffers[0], i));
				TestAssertTrue(r.is_null);
			}
			else
			{
				TestAssertTrue(DatumGetBool(r.val) == val);
				const int16 block = i / 64;
				const int16 offset = i % 64;
				TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			}
			rlen = run_length;
			val = !val;
		}
		else
		{
			TestAssertTrue(r.is_null == false);
			TestAssertTrue(DatumGetBool(r.val) == val);
			const int16 block = i / 64;
			const int16 offset = i % 64;
			TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			--rlen;
		}
	}

	DecompressResult r = array_decompression_iterator_try_next_forward(iter);
	TestAssertTrue(r.is_done);
}

static void
test_bool_dictionary(bool nulls, int run_length, int expected_size)
{
	Compressor *compressor = dictionary_compressor_for_type(BOOLOID);
	int rlen = run_length;
	bool val = true;
	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		if (rlen == 0)
		{
			if (nulls)
				compressor->append_null(compressor);
			else
				compressor->append_val(compressor, BoolGetDatum(val));
			rlen = run_length;
			val = !val;
		}
		else
		{
			compressor->append_val(compressor, BoolGetDatum(val));
			--rlen;
		}
	}

	Datum compressed = (Datum) compressor->finish(compressor);
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	TestAssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), expected_size);

	rlen = run_length;
	val = true;
	DecompressionIterator *iter =
		tsl_dictionary_decompression_iterator_from_datum_forward(compressed, BOOLOID);

	ArrowArray *bulk_result =
		tsl_dictionary_decompress_all(compressed, BOOLOID, CurrentMemoryContext);
	const uint64 *bulk_data = bulk_result->buffers[1];

	for (int i = 0; i < TEST_ELEMENTS; ++i)
	{
		DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter);
		TestAssertTrue(!r.is_done);
		if (rlen == 0)
		{
			if (nulls)
			{
				TestAssertTrue(!arrow_row_is_valid(bulk_result->buffers[0], i));
				TestAssertTrue(r.is_null);
			}
			else
			{
				TestAssertTrue(DatumGetBool(r.val) == val);
				const int16 block = i / 64;
				const int16 offset = i % 64;
				TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			}
			rlen = run_length;
			val = !val;
		}
		else
		{
			TestAssertTrue(r.is_null == false);
			TestAssertTrue(DatumGetBool(r.val) == val);
			const int16 block = i / 64;
			const int16 offset = i % 64;
			TestAssertTrue(((bulk_data[block] >> offset) & 1UL) == (int) val);
			--rlen;
		}
	}

	DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter);
	TestAssertTrue(r.is_done);
}

static void
test_empty_bool_compressor()
{
	/* This returns an ExtendedCompressor from bool_compress.c */
	Compressor *compressor = bool_compressor_for_type(BOOLOID);
	Datum compressed = (Datum) compressor->finish(compressor);
	TestAssertTrue(DatumGetPointer(compressed) == NULL);

	/* further abusing finish: */
	compressed = (Datum) compressor->finish(NULL);
	TestAssertTrue(DatumGetPointer(compressed) == NULL);

	/* make codecov happy */
	TestAssertTrue(bool_compressor_finish(NULL) == NULL);

	/* Passing a NULL pointer returns NULL. */
	TestEnsureError(DirectFunctionCall1(tsl_bool_compressor_finish, PointerGetDatum(NULL)));

	TestEnsureError(bool_compressor_for_type(FLOAT4OID));

	bool old_val = ts_guc_enable_bool_compression;
	ts_guc_enable_bool_compression = true;
	TestAssertTrue(compression_get_default_algorithm(BOOLOID) == COMPRESSION_ALGORITHM_BOOL);
	ts_guc_enable_bool_compression = false;
	TestAssertTrue(compression_get_default_algorithm(BOOLOID) == COMPRESSION_ALGORITHM_ARRAY);
	ts_guc_enable_bool_compression = old_val;
}

static void
test_bool_compressor_extended()
{
	Compressor *compressor = bool_compressor_for_type(BOOLOID);
	void *finished = compressor->finish(compressor);
	TestAssertTrue(finished == NULL);

	/* adding a null value should reinitialize the compressor */
	compressor->append_null(compressor);
	finished = compressor->finish(compressor);
	TestAssertTrue(finished == NULL);

	/* finishing a finished compressor should return NULL */
	finished = compressor->finish(compressor);
	TestAssertTrue(finished == NULL && "finishing a finished compressor should return NULL");

	/* adding a non-null value should reinitialize the compressor */
	compressor->append_val(compressor, BoolGetDatum(true));
	finished = compressor->finish(compressor);
	TestAssertTrue(finished != NULL);
}

static uint32
bool_compressed_size(int num_values, int flip_nth)
{
	Compressor *compressor = bool_compressor_for_type(BOOLOID);
	for (int i = 1; i < (num_values + 1); ++i)
	{
		if (i % flip_nth == 0)
			compressor->append_val(compressor, BoolGetDatum(false));
		else
			compressor->append_val(compressor, BoolGetDatum(true));
	}

	Datum compressed = (Datum) compressor->finish(compressor);
	TestAssertTrue(DatumGetPointer(compressed) != NULL);
	return VARSIZE(DatumGetPointer(compressed));
}

static void
test_bool()
{
	/* code covareage and simple tests */
	test_empty_bool_compressor();
	test_bool_compressor_extended();

	/* testing a few RLE configurations with or without nulls: */
	test_bool_rle(/* nulls = */ false, /* run_length = */ 1, /* expected_size = */ 152);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 5, /* expected_size = */ 152);
	test_bool_rle(/* nulls = */ true, /* run_length = */ 19, /* expected_size = */ 296);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 27, /* expected_size = */ 152);
	test_bool_rle(/* nulls = */ true, /* run_length = */ 43, /* expected_size = */ 296);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 61, /* expected_size = */ 152);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 65, /* expected_size = */ 152);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 100, /* expected_size = */ 112);
	test_bool_rle(/* nulls = */ true, /* run_length = */ 97, /* expected_size = */ 256);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 191, /* expected_size = */ 72);
	test_bool_rle(/* nulls = */ true, /* run_length = */ 237, /* expected_size = */ 144);
	test_bool_rle(/* nulls = */ false, /* run_length = */ 600, /* expected_size = */ 40);
	test_bool_rle(/* nulls = */ true, /* run_length = */ 720, /* expected_size = */ 80);
	test_bool_rle(/* nulls = */ false,
				  /* run_length = */ TEST_ELEMENTS + 1,
				  /* expected_size = */ 32);
	/* few select cases for comparison against bool compression: */
	test_bool_array(/* nulls = */ false, /* run_length = */ 1, /* expected_size = */ 1055);
	test_bool_array(/* nulls = */ true, /* run_length = */ 19, /* expected_size = */ 1149);
	test_bool_array(/* nulls = */ false, /* run_length = */ 600, /* expected_size = */ 1055);
	test_bool_array(/* nulls = */ true, /* run_length = */ 720, /* expected_size = */ 1094);
	test_bool_array(/* nulls = */ false,
					/* run_length = */ TEST_ELEMENTS + 1,
					/* expected_size = */ 1055);

	/* few select cases for comparison against bool compression: */
	test_bool_dictionary(/* nulls = */ false, /* run_length = */ 1, /* expected_size = */ 186);
	test_bool_dictionary(/* nulls = */ true, /* run_length = */ 19, /* expected_size = */ 330);
	test_bool_dictionary(/* nulls = */ false, /* run_length = */ 600, /* expected_size = */ 74);
	test_bool_dictionary(/* nulls = */ true, /* run_length = */ 720, /* expected_size = */ 114);
	test_bool_dictionary(/* nulls = */ false,
						 /* run_length = */ TEST_ELEMENTS + 1,
						 /* expected_size = */ 65);

	int baseline = bool_compressed_size(1, 1);
	int no_rle = bool_compressed_size(64, 2);
	/* verify that we can pack 64 bits into the same size */
	TestAssertTrue(no_rle == baseline);
	int rle_size = bool_compressed_size(65, 66);
	/* verify that we can RLE 65 bits into the same size */
	TestAssertTrue(rle_size == baseline);
}

static void
test_null()
{
	/* pointless tests to make codecov happy */
	StringInfoData buffer = (StringInfoData){
		.data = NULL,
		.len = 0,
		.maxlen = 0,
	};
	TestEnsureError(null_decompression_iterator_from_datum_forward((Datum) 0, INT2OID));
	TestEnsureError(null_decompression_iterator_from_datum_reverse((Datum) 0, BOOLOID));
	TestEnsureError(null_compressed_send(NULL, &buffer));
	TestEnsureError(null_compressed_recv(&buffer));
	TestEnsureError(null_compressor_for_type(BOOLOID));

	{
		StringInfoData buffer;

		void *compressed = null_compressor_get_dummy_block();
		Datum sent_datum = DirectFunctionCall2(tsl_compressed_data_send,
											   PointerGetDatum(compressed),
											   PointerGetDatum(&buffer));

		bytea *sent = (bytea *) DatumGetPointer(sent_datum);
		StringInfoData transmission = (StringInfoData){
			.data = VARDATA(sent),
			.len = VARSIZE(sent),
			.maxlen = VARSIZE(sent),
		};

		TestAssertTrue(transmission.len > 0);
		TestAssertTrue(transmission.data != NULL);

		Datum res = DirectFunctionCall1(tsl_compressed_data_recv, PointerGetDatum(&transmission));
		TestAssertTrue(DatumGetPointer(res) != NULL);
	}
	{
		void *compressed = null_compressor_get_dummy_block();
		Datum has_nulls =
			DirectFunctionCall1(tsl_compressed_data_has_nulls, PointerGetDatum(compressed));
		TestAssertTrue(DatumGetBool(has_nulls));
	}
}

static void
test_simple8b_rle_compressed_size(uint64 *elements, int num_elements)
{
	Simple8bRleCompressor compressor;
	simple8brle_compressor_init(&compressor);

	for (int i = 0; i < num_elements; i++)
	{
		simple8brle_compressor_append(&compressor, elements[i]);
	}

	size_t compressed_size = simple8brle_compressor_compressed_const_size(&compressor);
	if (num_elements == 0)
	{
		TestAssertInt64Eq(compressed_size, 0);
		return;
	}

	Simple8bRleSerialized *serialized = simple8brle_compressor_finish(&compressor);
	size_t serialized_size = simple8brle_serialized_total_size(serialized);
	TestAssertInt64Eq(compressed_size, serialized_size);

	/* Check the const size function after the compressor is finished,
	 * this may happen accidentally, not on purpose.
	 */
	compressed_size = simple8brle_compressor_compressed_const_size(&compressor);
	TestAssertInt64Eq(compressed_size, serialized_size);

	pfree(serialized);
}

static void
test_simple8b_rle()
{
	/* clang-format off */
	/* clang would place all the elements on a single line otherwise */
	uint64 elements[] = {
		1, 2, 4, 8, 16, 7, 3, 32, 64, 63, 31, 15, 7, 3, 1, 0,
		128, 127, 63, 31, 15, 7, 3, 1, 0, 256, 255, 127, 63, 31, 15, 7,
		3, 1, 0, 512, 511, 126, 63, 31, 15, 7, 3, 1, 0, 1024, 1023, 511,
		255, 127, 63, 31, 15, 7, 3, 1, 0, 2048, 2047, 1023, 511, 255, 127,
		63, 31, 15, 7, 3, 1, 0, 4096, 4095, 2047, 1023, 511, 255, 127, 63,
		31, 15, 7, 3, 1, 0, 8192, 8191, 4095, 2047, 1023, 511, 255, 127,
		63, 31, 15, 7, 3, 1, 0, 16384, 16383, 8191, 4095, 2047, 1023, 511,
		255, 127, 63, 31, 15, 7, 3, 1, 0, 32768, 32767, 16383, 8191, 4095,
		2047, 1023, 511, 255, 127, 63, 31, 15, 7, 3, 1, 0, 65536, 65535,
		32767, 16383, 8191, 4095, 2047, 1023, 511, 255, 127, 63, 31, 15,
		131072, 131071, 65535, 16777216, 16777211, 16777215, 4294967296ULL,
		12884901888ULL
	};
	/* clang-format on */

	int n = sizeof(elements) / sizeof(*elements);
	for (int i = 0; i < n; i++)
	{
		test_simple8b_rle_compressed_size(elements, i);
	}
}

Datum
ts_test_compression(PG_FUNCTION_ARGS)
{
	test_int_array();
	test_string_array();
	test_int_dictionary();
	test_string_dictionary();
	test_gorilla_int();
	test_gorilla_float();
	test_gorilla_double(/* have_nulls = */ false, /* have_random = */ false);
	test_gorilla_double(/* have_nulls = */ false, /* have_random = */ true);
	test_gorilla_double(/* have_nulls = */ true, /* have_random = */ false);
	test_gorilla_double(/* have_nulls = */ true, /* have_random = */ true);
	test_delta();
	test_delta2();
	test_delta3(/* have_nulls = */ false, /* have_random = */ false);
	test_delta3(/* have_nulls = */ false, /* have_random = */ true);
	test_delta3(/* have_nulls = */ true, /* have_random = */ false);
	test_delta3(/* have_nulls = */ true, /* have_random = */ true);
	test_bool();
	test_null();
	test_simple8b_rle();

	/* Some tests for zig-zag encoding overflowing the original element width. */
	test_delta4(test_delta4_case1, sizeof(test_delta4_case1) / sizeof(*test_delta4_case1));
	test_delta4(test_delta4_case2, sizeof(test_delta4_case2) / sizeof(*test_delta4_case2));

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_append);

Datum
ts_segment_meta_min_max_append(PG_FUNCTION_ARGS)
{
	BatchMetadataBuilder *builder =
		(BatchMetadataBuilder *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_segment_meta_min_max_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (builder == NULL)
	{
		Oid type_to_compress = get_fn_expr_argtype(fcinfo->flinfo, 1);
		builder =
			batch_metadata_builder_minmax_create(type_to_compress, fcinfo->fncollation, -1, -1);
	}
	if (PG_ARGISNULL(1))
		builder->update_null(builder);
	else
		builder->update_val(builder, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(builder);
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_finish_max);
Datum
ts_segment_meta_min_max_finish_max(PG_FUNCTION_ARGS)
{
	BatchMetadataBuilderMinMax *builder =
		(BatchMetadataBuilderMinMax *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (builder == NULL || batch_metadata_builder_minmax_empty(builder))
		PG_RETURN_NULL();

	PG_RETURN_DATUM(batch_metadata_builder_minmax_max(builder));
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_finish_min);
Datum
ts_segment_meta_min_max_finish_min(PG_FUNCTION_ARGS)
{
	BatchMetadataBuilderMinMax *builder =
		(BatchMetadataBuilderMinMax *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (builder == NULL || batch_metadata_builder_minmax_empty(builder))
		PG_RETURN_NULL();

	PG_RETURN_DATUM(batch_metadata_builder_minmax_min(builder));
}

TS_FUNCTION_INFO_V1(ts_compression_custom_type_in);
TS_FUNCTION_INFO_V1(ts_compression_custom_type_out);
TS_FUNCTION_INFO_V1(ts_compression_custom_type_eq);

/* basically int2in but returns by reference */
Datum
ts_compression_custom_type_in(PG_FUNCTION_ARGS)
{
	char *num = PG_GETARG_CSTRING(0);
	int16 *val = palloc(sizeof(*val));
	*val = pg_strtoint16(num);

	PG_RETURN_POINTER(val);
}

/* like int2out but takes values by ref */
Datum
ts_compression_custom_type_out(PG_FUNCTION_ARGS)
{
	int16 *arg = (int16 *) PG_GETARG_POINTER(0);
	char *result = (char *) palloc(7); /* sign, 5 digits, '\0' */

	pg_itoa(*arg, result);
	PG_RETURN_CSTRING(result);
}

/* like int2eq but takes values by ref */
Datum
ts_compression_custom_type_eq(PG_FUNCTION_ARGS)
{
	int16 *arg1 = (int16 *) PG_GETARG_POINTER(0);
	int16 *arg2 = (int16 *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(*arg1 == *arg2);
}
