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
#include <libpq/pqformat.h>
#include <lib/stringinfo.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>
#include <fmgr.h>

#include "ts_catalog/catalog.h"
#include <export.h>
#include "test_utils.h"

#include "compression/array.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/deltadelta.h"
#include "compression/float_utils.h"
#include "compression/segment_meta.h"

#define VEC_PREFIX compression_info
#define VEC_ELEMENT_TYPE Form_hypertable_compression
#define VEC_DECLARE 1
#define VEC_DEFINE 1
#define VEC_SCOPE static inline
#include <adts/vec.h>

#define TEST_ELEMENTS 1015

TS_FUNCTION_INFO_V1(ts_test_compression);
TS_FUNCTION_INFO_V1(ts_compress_table);
TS_FUNCTION_INFO_V1(ts_decompress_table);

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
		StringInfoData transmition;
		GorillaCompressed *compressed_recv;

		pq_begintypsend(&buf);
		gorilla_compressed_send((CompressedDataHeader *) compressed, &buf);
		sent = pq_endtypsend(&buf);

		transmition = (StringInfoData){
			.data = VARDATA(sent),
			.len = VARSIZE(sent),
			.maxlen = VARSIZE(sent),
		};

		compressed_recv =
			(GorillaCompressed *) DatumGetPointer(gorilla_compressed_recv(&transmition));
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
	float i;
	for (i = 0.0; i < TEST_ELEMENTS; i++)
		gorilla_compressor_append_value(compressor, float_get_bits(i));

	compressed = gorilla_compressor_finish(compressor);
	TestAssertTrue(compressed != NULL);
	TestAssertInt64Eq(VARSIZE(compressed), 1200);

	i = 0;
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
		delta_delta_decompression_iterator_from_datum_forward(PointerGetDatum((void *) compressed),
															  INT8OID);
	ArrowArray *bulk_result = delta_delta_decompress_all(PointerGetDatum((void *) compressed),
														 INT8OID,
														 CurrentMemoryContext);
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
	iter =
		delta_delta_decompression_iterator_from_datum_reverse(PointerGetDatum((void *) compressed),
															  INT8OID);
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

	/* Some tests for zig-zag encoding overflowing the original element width. */
	test_delta4(test_delta4_case1, sizeof(test_delta4_case1) / sizeof(*test_delta4_case1));
	test_delta4(test_delta4_case2, sizeof(test_delta4_case2) / sizeof(*test_delta4_case2));

	PG_RETURN_VOID();
}

static compression_info_vec *
compression_info_from_array(ArrayType *compression_info_arr, Oid form_oid)
{
	ArrayMetaState compression_info_arr_meta = {
		.element_type = form_oid,
	};
	ArrayIterator compression_info_iter;
	Datum compression_info_datum;
	bool is_null;
	compression_info_vec *compression_info = compression_info_vec_create(CurrentMemoryContext, 0);
	TupleDesc form_desc = NULL;

	get_typlenbyvalalign(compression_info_arr_meta.element_type,
						 &compression_info_arr_meta.typlen,
						 &compression_info_arr_meta.typbyval,
						 &compression_info_arr_meta.typalign);

	compression_info_iter =
		array_create_iterator(compression_info_arr, 0, &compression_info_arr_meta);

	while (array_iterate(compression_info_iter, &compression_info_datum, &is_null))
	{
		HeapTupleHeader form;
		HeapTupleData tmptup;

		TestAssertTrue(!is_null);
		form = DatumGetHeapTupleHeaderCopy(compression_info_datum);
		TestAssertTrue(HeapTupleHeaderGetTypeId(form) == form_oid);
		if (form_desc == NULL)
		{
			int32 formTypmod = HeapTupleHeaderGetTypMod(form);
			form_desc = lookup_rowtype_tupdesc(form_oid, formTypmod);
		}

		tmptup.t_len = HeapTupleHeaderGetDatumLength(form);
		tmptup.t_data = form;
		compression_info_vec_append(compression_info, (void *) GETSTRUCT(&tmptup));
	}
	if (form_desc != NULL)
		ReleaseTupleDesc(form_desc);
	return compression_info;
}

Datum
ts_compress_table(PG_FUNCTION_ARGS)
{
	Oid in_table = PG_GETARG_OID(0);
	Oid out_table = PG_GETARG_OID(1);
	ArrayType *compression_info_array = DatumGetArrayTypeP(PG_GETARG_DATUM(2));
	compression_info_vec *compression_info =
		compression_info_from_array(compression_info_array, compression_info_array->elemtype);

	compress_chunk(in_table,
				   out_table,
				   (const ColumnCompressionInfo **) compression_info->data,
				   compression_info->num_elements);

	PG_RETURN_VOID();
}

Datum
ts_decompress_table(PG_FUNCTION_ARGS)
{
	Oid in_table = PG_GETARG_OID(0);
	Oid out_table = PG_GETARG_OID(1);

	decompress_chunk(in_table, out_table);

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_append);

Datum
ts_segment_meta_min_max_append(PG_FUNCTION_ARGS)
{
	SegmentMetaMinMaxBuilder *builder =
		(SegmentMetaMinMaxBuilder *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
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
		builder = segment_meta_min_max_builder_create(type_to_compress, fcinfo->fncollation);
	}
	if (PG_ARGISNULL(1))
		segment_meta_min_max_builder_update_null(builder);
	else
		segment_meta_min_max_builder_update_val(builder, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(builder);
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_finish_max);
Datum
ts_segment_meta_min_max_finish_max(PG_FUNCTION_ARGS)
{
	SegmentMetaMinMaxBuilder *builder =
		(SegmentMetaMinMaxBuilder *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (builder == NULL || segment_meta_min_max_builder_empty(builder))
		PG_RETURN_NULL();

	PG_RETURN_DATUM(segment_meta_min_max_builder_max(builder));
}

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_finish_min);
Datum
ts_segment_meta_min_max_finish_min(PG_FUNCTION_ARGS)
{
	SegmentMetaMinMaxBuilder *builder =
		(SegmentMetaMinMaxBuilder *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (builder == NULL || segment_meta_min_max_builder_empty(builder))
		PG_RETURN_NULL();

	PG_RETURN_DATUM(segment_meta_min_max_builder_min(builder));
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
