/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <libpq/pqformat.h>
#include <fmgr.h>

#include <export.h>

#include "compression/array.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/deltadelta.h"
#include "compression/utils.h"

TS_FUNCTION_INFO_V1(ts_test_compression);

#define AssertInt64Eq(a, b)                                                                        \
	do                                                                                             \
	{                                                                                              \
		int64 a_i = (a);                                                                           \
		int64 b_i = (b);                                                                           \
		if (a_i != b_i)                                                                            \
		{                                                                                          \
			elog(ERROR, INT64_FORMAT " != " INT64_FORMAT " @ line %d", a_i, b_i, __LINE__);        \
		}                                                                                          \
	} while (0)

#define AssertDoubleEq(a, b)                                                                       \
	do                                                                                             \
	{                                                                                              \
		double a_i = (a);                                                                          \
		double b_i = (b);                                                                          \
		if (a_i != b_i)                                                                            \
		{                                                                                          \
			elog(ERROR, "%f != %f @ line %d", a_i, b_i, __LINE__);                                 \
		}                                                                                          \
	} while (0)

#define EnsureError(a)                                                                             \
	do                                                                                             \
	{                                                                                              \
		volatile bool this_has_panicked = false;                                                   \
		PG_TRY();                                                                                  \
		{                                                                                          \
			(a);                                                                                   \
		}                                                                                          \
		PG_CATCH();                                                                                \
		{                                                                                          \
			this_has_panicked = true;                                                              \
			FlushErrorState();                                                                     \
		}                                                                                          \
		PG_END_TRY();                                                                              \
		if (!this_has_panicked)                                                                    \
		{                                                                                          \
			elog(ERROR, "failed to panic @ line %d", __LINE__);                                    \
		}                                                                                          \
	} while (0)

static void
test_int_array()
{
	ArrayCompressor *compressor = array_compressor_alloc(INT4OID);
	ArrayCompressed *compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < 1015; i++)
		array_compressor_append(compressor, Int32GetDatum(i));

	compressed = array_compressor_finish(compressor);
	Assert(compressed != NULL);

	i = 0;
	iter =
		tsl_array_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), INT4OID);
	for (DecompressResult r = array_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt32(r.val), i);
		i += 1;
	}
	AssertInt64Eq(i, 1015);

	iter =
		tsl_array_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), INT4OID);
	for (DecompressResult r = array_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt32(r.val), i - 1);
		i -= 1;
	}
	AssertInt64Eq(i, 0);
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

	for (i = 0; i < 1015; i++)
		array_compressor_append(compressor, PointerGetDatum(texts[i % 5]));

	compressed = array_compressor_finish(compressor);
	Assert(compressed != NULL);

	i = 0;
	iter =
		tsl_array_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), TEXTOID);
	for (DecompressResult r = array_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[i % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i += 1;
	}
	AssertInt64Eq(i, 1015);

	iter =
		tsl_array_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), TEXTOID);
	for (DecompressResult r = array_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = array_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[(i - 1) % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i -= 1;
	}
	AssertInt64Eq(i, 0);
}

static void
test_int_dictionary()
{
	DictionaryCompressor *compressor = dictionary_compressor_alloc(INT4OID);
	DictionaryCompressed *compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < 1015; i++)
		dictionary_compressor_append(compressor, Int32GetDatum(i % 15));

	compressed = dictionary_compressor_finish(compressor);
	Assert(compressed != NULL);

	i = 0;
	iter = tsl_dictionary_decompression_iterator_from_datum_forward(PointerGetDatum(compressed),
																	INT4OID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt32(r.val), i % 15);
		i += 1;
	}
	AssertInt64Eq(i, 1015);
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
	Assert(compressed != NULL);

	i = 0;
	iter = tsl_dictionary_decompression_iterator_from_datum_forward(PointerGetDatum(compressed),
																	TEXTOID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[i % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i += 1;
	}

	AssertInt64Eq(i, 1014);
	iter = tsl_dictionary_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed),
																	TEXTOID);
	for (DecompressResult r = dictionary_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = dictionary_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		if (strcmp(TextDatumGetCString(r.val), strings[(i - 1) % 5]) != 0)
			elog(ERROR,
				 "%4d \"%s\" != \"%s\" @ %d",
				 i,
				 TextDatumGetCString(r.val),
				 strings[i % 5],
				 __LINE__);
		i -= 1;
	}
	AssertInt64Eq(i, 0);

	EnsureError(dictionary_compressor_alloc(CSTRINGOID));
}

static void
test_gorilla_int()
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;
	DecompressionIterator *iter;
	uint32 i;
	for (i = 0; i < 1015; i++)
		gorilla_compressor_append_value(compressor, i);

	compressed = gorilla_compressor_finish(compressor);
	Assert(compressed != NULL);
	AssertInt64Eq(VARSIZE(compressed), 1304);

	i = 0;
	iter = gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), INT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	AssertInt64Eq(i, 1015);

	iter = gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), INT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt64(r.val), i - 1);
		i -= 1;
	}
	AssertInt64Eq(i, 0);

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
			Assert(!r.is_null);
			AssertInt64Eq(DatumGetInt64(r.val), i);
			i += 1;
		}
		AssertInt64Eq(i, 1015);
	}
}

static void
test_gorilla_float()
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;
	DecompressionIterator *iter;
	float i;
	for (i = 0.0; i < 1015.0; i++)
		gorilla_compressor_append_value(compressor, float_get_bits(i));

	compressed = gorilla_compressor_finish(compressor);
	Assert(compressed != NULL);
	AssertInt64Eq(VARSIZE(compressed), 1168);

	i = 0;
	iter =
		gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), FLOAT4OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertDoubleEq(DatumGetFloat4(r.val), i);
		i += 1.0;
	}
	AssertInt64Eq(i, 1015);

	iter =
		gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), FLOAT4OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetFloat4(r.val), i - 1);
		i -= 1;
	}
	AssertInt64Eq(i, 0);
}

static void
test_gorilla_double()
{
	GorillaCompressor *compressor = gorilla_compressor_alloc();
	GorillaCompressed *compressed;
	DecompressionIterator *iter;
	double i;
	for (i = 0.0; i < 1015.0; i++)
		gorilla_compressor_append_value(compressor, double_get_bits(i));

	compressed = gorilla_compressor_finish(compressor);
	Assert(compressed != NULL);
	AssertInt64Eq(VARSIZE(compressed), 1176);

	i = 0;
	iter =
		gorilla_decompression_iterator_from_datum_forward(PointerGetDatum(compressed), FLOAT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertDoubleEq(DatumGetFloat8(r.val), i);
		i += 1.0;
	}
	AssertInt64Eq(i, 1015);

	iter =
		gorilla_decompression_iterator_from_datum_reverse(PointerGetDatum(compressed), FLOAT8OID);
	for (DecompressResult r = gorilla_decompression_iterator_try_next_reverse(iter); !r.is_done;
		 r = gorilla_decompression_iterator_try_next_reverse(iter))
	{
		Assert(!r.is_null);
		AssertDoubleEq(DatumGetFloat8(r.val), i - 1);
		i -= 1;
	}
	AssertInt64Eq(i, 0);
}

static void
test_delta()
{
	DeltaDeltaCompressor *compressor = delta_delta_compressor_alloc();
	Datum compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < 1015; i++)
		delta_delta_compressor_append_value(compressor, i);

	compressed = DirectFunctionCall1(tsl_deltadelta_compressor_finish, PointerGetDatum(compressor));
	Assert(DatumGetPointer(compressed) != NULL);
	AssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), 64);

	i = 0;
	iter = delta_delta_decompression_iterator_from_datum_forward(compressed, INT8OID);
	for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = delta_delta_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		AssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	AssertInt64Eq(i, 1015);
}

static void
test_delta2()
{
	DeltaDeltaCompressor *compressor = delta_delta_compressor_alloc();
	Datum compressed;
	DecompressionIterator *iter;
	int i;
	for (i = 0; i < 1015; i++)
	{
		/* prevent everything from being rle'd away */
		if (i % 2 != 0)
			delta_delta_compressor_append_value(compressor, 2 * i);
		else
			delta_delta_compressor_append_value(compressor, i);
	}

	compressed = DirectFunctionCall1(tsl_deltadelta_compressor_finish, PointerGetDatum(compressor));
	Assert(DatumGetPointer(compressed) != NULL);
	AssertInt64Eq(VARSIZE(DatumGetPointer(compressed)), 1672);

	i = 0;
	iter = delta_delta_decompression_iterator_from_datum_forward(compressed, INT8OID);
	for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter); !r.is_done;
		 r = delta_delta_decompression_iterator_try_next_forward(iter))
	{
		Assert(!r.is_null);
		if (i % 2 != 0)
			AssertInt64Eq(DatumGetInt64(r.val), 2 * i);
		else
			AssertInt64Eq(DatumGetInt64(r.val), i);
		i += 1;
	}
	AssertInt64Eq(i, 1015);
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
	test_gorilla_double();
	test_delta();
	test_delta2();
	PG_RETURN_VOID();
}
