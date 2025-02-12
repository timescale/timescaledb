/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "bool_compress.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "simple8b_rle.h"
#include "simple8b_rle_bitmap.h"

typedef struct BoolCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls; /* 1 if this has a NULLs bitmap after the values, 0 otherwise */
	uint8 padding[2];
	char values[FLEXIBLE_ARRAY_MEMBER];
} BoolCompressed;

typedef struct BoolDecompressionIterator
{
	DecompressionIterator base;
	Simple8bRleBitmap values;
	Simple8bRleBitmap nulls;
	int32 num_elements;
	int32 value_position;
	int32 element_position;
} BoolDecompressionIterator;

typedef struct BoolCompressor
{
	Simple8bRleCompressor values;
	Simple8bRleCompressor nulls;
	bool has_nulls;
} BoolCompressor;

typedef struct ExtendedCompressor
{
	Compressor base;
	BoolCompressor *internal;
} ExtendedCompressor;

/*
 * Local helpers
 */
static void bool_compressor_append_bool(Compressor *compressor, Datum val);

static void bool_compressor_append_null_value(Compressor *compressor);

static void *bool_compressor_finish_and_reset(Compressor *compressor);

const Compressor bool_compressor_initializer = {
	.append_val = bool_compressor_append_bool,
	.append_null = bool_compressor_append_null_value,
	.finish = bool_compressor_finish_and_reset,
};

static BoolCompressed *bool_compressed_from_parts(Simple8bRleSerialized *values,
												  Simple8bRleSerialized *nulls);

static void decompression_iterator_init(BoolDecompressionIterator *iter, void *compressed,
										Oid element_type, bool forward);

static DecompressResultInternal
bool_decompression_iterator_try_next_forward_internal(BoolDecompressionIterator *iter);

static DecompressResultInternal
bool_decompression_iterator_try_next_reverse_internal(BoolDecompressionIterator *iter);

static inline DecompressResult convert_from_internal(DecompressResultInternal res_internal);

/*
 * Compressor framework functions and definitions for the bool_compress algorithm.
 */

extern BoolCompressor *
bool_compressor_alloc(void)
{
	BoolCompressor *compressor = palloc0(sizeof(*compressor));
	simple8brle_compressor_init(&compressor->values);
	simple8brle_compressor_init(&compressor->nulls);
	return compressor;
}

extern void
bool_compressor_append_null(BoolCompressor *compressor)
{
	compressor->has_nulls = true;
	simple8brle_compressor_append(&compressor->nulls, 1);
}

extern void
bool_compressor_append_value(BoolCompressor *compressor, bool next_val)
{
	simple8brle_compressor_append(&compressor->values, next_val);
	simple8brle_compressor_append(&compressor->nulls, 0);
}

extern void *
bool_compressor_finish(BoolCompressor *compressor)
{
	Simple8bRleSerialized *values = simple8brle_compressor_finish(&compressor->values);
	Simple8bRleSerialized *nulls = simple8brle_compressor_finish(&compressor->nulls);
	BoolCompressed *compressed;

	if (values == NULL)
		return NULL;

	compressed = bool_compressed_from_parts(values, compressor->has_nulls ? nulls : NULL);

	Assert(compressed->compression_algorithm == COMPRESSION_ALGORITHM_BOOL_COMPRESS);
	return compressed;
}

extern bool
bool_compressed_has_nulls(const CompressedDataHeader *header)
{
	const BoolCompressed *ddc = (const BoolCompressed *) header;
	return ddc->has_nulls;
}

extern DecompressResult
bool_decompression_iterator_try_next_forward(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_BOOL_COMPRESS && iter->forward);
	Assert(iter->element_type == BOOLOID);
	return convert_from_internal(
		bool_decompression_iterator_try_next_forward_internal((BoolDecompressionIterator *) iter));
}

extern DecompressionIterator *
bool_decompression_iterator_from_datum_forward(Datum bool_compressed, Oid element_type)
{
	BoolDecompressionIterator *iterator = palloc(sizeof(*iterator));
	decompression_iterator_init(iterator,
								(void *) PG_DETOAST_DATUM(bool_compressed),
								element_type,
								true);
	return &iterator->base;
}

extern DecompressResult
bool_decompression_iterator_try_next_reverse(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_BOOL_COMPRESS && !iter->forward);
	Assert(iter->element_type == BOOLOID);
	return convert_from_internal(
		bool_decompression_iterator_try_next_reverse_internal((BoolDecompressionIterator *) iter));
}

extern DecompressionIterator *
bool_decompression_iterator_from_datum_reverse(Datum bool_compressed, Oid element_type)
{
	BoolDecompressionIterator *iterator = palloc(sizeof(*iterator));
	decompression_iterator_init(iterator,
								(void *) PG_DETOAST_DATUM(bool_compressed),
								element_type,
								false);
	return &iterator->base;
}

extern void
bool_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const BoolCompressed *data = (BoolCompressed *) header;
	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_BOOL_COMPRESS);
	pq_sendbyte(buffer, data->has_nulls);
	simple8brle_serialized_send(buffer, (Simple8bRleSerialized *) data->values);
	if (data->has_nulls)
	{
		Simple8bRleSerialized *nulls =
			(Simple8bRleSerialized *) (((char *) data->values) +
									   simple8brle_serialized_total_size(
										   (Simple8bRleSerialized *) data->values));
		simple8brle_serialized_send(buffer, nulls);
	}
}

extern Datum
bool_compressed_recv(StringInfo buffer)
{
	uint8 has_nulls;
	Simple8bRleSerialized *values;
	Simple8bRleSerialized *nulls = NULL;
	BoolCompressed *compressed;

	has_nulls = pq_getmsgbyte(buffer);
	CheckCompressedData(has_nulls == 0 || has_nulls == 1);

	values = simple8brle_serialized_recv(buffer);
	if (has_nulls)
		nulls = simple8brle_serialized_recv(buffer);

	compressed = bool_compressed_from_parts(values, nulls);

	PG_RETURN_POINTER(compressed);
}

extern Compressor *
bool_compressor_for_type(Oid element_type)
{
	ExtendedCompressor *compressor = palloc(sizeof(*compressor));
	switch (element_type)
	{
		case BOOLOID:
			*compressor = (ExtendedCompressor){ .base = bool_compressor_initializer };
			return &compressor->base;
		default:
			elog(ERROR, "invalid type for bool compressor \"%s\"", format_type_be(element_type));
	}

	pg_unreachable();
}

/*
 * Cross-module functions for the bool_compress algorithm.
 */

extern Datum
tsl_bool_compressor_append(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	MemoryContext agg_context;
	BoolCompressor *compressor = (BoolCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_bool_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		compressor = bool_compressor_alloc();
		if (PG_NARGS() > 2)
			elog(ERROR, "append expects two arguments");
	}

	if (PG_ARGISNULL(1))
		bool_compressor_append_null(compressor);
	else
	{
		bool next_val = PG_GETARG_BOOL(1);
		bool_compressor_append_value(compressor, next_val);
	}

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

extern Datum
tsl_bool_compressor_finish(PG_FUNCTION_ARGS)
{
	BoolCompressor *compressor = PG_ARGISNULL(0) ? NULL : (BoolCompressor *) PG_GETARG_POINTER(0);
	void *compressed;
	if (compressor == NULL)
		PG_RETURN_NULL();

	compressed = bool_compressor_finish(compressor);
	if (compressed == NULL)
		PG_RETURN_NULL();
	PG_RETURN_POINTER(compressed);
}

/*
 * Local helpers
 */
static void
bool_compressor_append_bool(Compressor *compressor, Datum val)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = bool_compressor_alloc();

	bool_compressor_append_value(extended->internal, DatumGetBool(val) ? true : false);
}

static void
bool_compressor_append_null_value(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = bool_compressor_alloc();

	bool_compressor_append_null(extended->internal);
}

static void *
bool_compressor_finish_and_reset(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	void *compressed = bool_compressor_finish(extended->internal);
	pfree(extended->internal);
	extended->internal = NULL;
	return compressed;
}

static BoolCompressed *
bool_compressed_from_parts(Simple8bRleSerialized *values, Simple8bRleSerialized *nulls)
{
	uint32 nulls_size = 0;
	Size compressed_size;
	char *compressed_data;
	BoolCompressed *compressed;

	if (nulls != NULL)
		nulls_size = simple8brle_serialized_total_size(nulls);

	compressed_size =
		sizeof(BoolCompressed) + simple8brle_serialized_total_size(values) + nulls_size;

	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	compressed_data = palloc(compressed_size);
	compressed = (BoolCompressed *) compressed_data;
	SET_VARSIZE(&compressed->vl_len_, compressed_size);

	compressed->compression_algorithm = COMPRESSION_ALGORITHM_BOOL_COMPRESS;
	compressed->has_nulls = nulls_size != 0 ? 1 : 0;

	compressed_data += sizeof(*compressed);
	compressed_data =
		bytes_serialize_simple8b_and_advance(compressed_data,
											 simple8brle_serialized_total_size(values),
											 values);

	if (compressed->has_nulls == 1 && nulls != NULL)
	{
		CheckCompressedData(nulls->num_elements > values->num_elements);
		bytes_serialize_simple8b_and_advance(compressed_data, nulls_size, nulls);
	}

	return compressed;
}

static inline DecompressResult
convert_from_internal(DecompressResultInternal res_internal)
{
	if (res_internal.is_done || res_internal.is_null)
	{
		return (DecompressResult){
			.is_done = res_internal.is_done,
			.is_null = res_internal.is_null,
		};
	}

	return (DecompressResult){
		.val = BoolGetDatum(res_internal.val),
	};
}

static void
decompression_iterator_init(BoolDecompressionIterator *iter, void *compressed, Oid element_type,
							bool forward)
{
	StringInfoData si = { .data = compressed, .len = VARSIZE(compressed) };
	BoolCompressed *header = consumeCompressedData(&si, sizeof(BoolCompressed));
	Simple8bRleSerialized *values = bytes_deserialize_simple8b_and_advance(&si);

	Assert(header->has_nulls == 0 || header->has_nulls == 1);
	Assert(element_type == BOOLOID);

	const bool has_nulls = header->has_nulls == 1;

	CheckCompressedData(has_nulls == 0 || has_nulls == 1);

	*iter = (BoolDecompressionIterator){
		.base = { .compression_algorithm = COMPRESSION_ALGORITHM_BOOL_COMPRESS,
				  .forward = forward,
				  .element_type = element_type,
				  .try_next = (forward ? bool_decompression_iterator_try_next_forward :
										 bool_decompression_iterator_try_next_reverse) },
		.values = { 0 },
		.nulls = { 0 },
		.num_elements = 0,
		.value_position = 0,
		.element_position = 0,
	};

	iter->values = simple8brle_bitmap_decompress(values);
	iter->num_elements = iter->values.num_elements;

	if (has_nulls)
	{
		Simple8bRleSerialized *nulls = bytes_deserialize_simple8b_and_advance(&si);
		iter->nulls = simple8brle_bitmap_decompress(nulls);
		iter->num_elements = iter->nulls.num_elements;
	}

	if (!forward)
	{
		iter->element_position = iter->num_elements - 1;
		iter->value_position = iter->values.num_elements - 1;
	}
}

static DecompressResultInternal
bool_decompression_iterator_try_next_forward_internal(BoolDecompressionIterator *iter)
{
	if (iter->element_position >= iter->num_elements)
		return (DecompressResultInternal){
			.is_done = true,
		};

	/* check nulls */
	if (iter->nulls.num_elements > 0)
	{
		if (simple8brle_bitmap_get_at(&iter->nulls, iter->element_position))
		{
			iter->element_position++;
			return (DecompressResultInternal){
				.is_null = true,
			};
		}
	}

	bool val = simple8brle_bitmap_get_at(&iter->values, iter->value_position);
	iter->element_position++;
	iter->value_position++;

	return (DecompressResultInternal){
		.val = val,
	};
}

static DecompressResultInternal
bool_decompression_iterator_try_next_reverse_internal(BoolDecompressionIterator *iter)
{
	if (iter->element_position < 0)
		return (DecompressResultInternal){
			.is_done = true,
		};

	/* check nulls */
	if (iter->nulls.num_elements > 0)
	{
		if (simple8brle_bitmap_get_at(&iter->nulls, iter->element_position))
		{
			iter->element_position--;
			return (DecompressResultInternal){
				.is_null = true,
			};
		}
	}

	bool val = simple8brle_bitmap_get_at(&iter->values, iter->value_position);
	iter->element_position--;
	iter->value_position--;

	return (DecompressResultInternal){
		.val = val,
	};
}
