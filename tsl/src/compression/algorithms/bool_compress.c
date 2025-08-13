/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "bool_compress.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "simple8b_rle.h"
#include "simple8b_rle_bitarray.h"
#include "simple8b_rle_bitmap.h"

typedef struct BoolCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls;  /* 1 if this has a NULLs bitmap after the values, 0 otherwise */
	uint8 padding[2]; /* padding added because of Simple8bRleSerialized format */
	char values[FLEXIBLE_ARRAY_MEMBER];
} BoolCompressed;

typedef struct BoolDecompressionIterator
{
	DecompressionIterator base;
	Simple8bRleBitmap values;
	Simple8bRleBitmap validity_bitmap;
	int32 position;
} BoolDecompressionIterator;

typedef struct BoolCompressor
{
	Simple8bRleCompressor values;
	Simple8bRleCompressor validity_bitmap;
	bool has_nulls;
	bool last_value;
	uint32 num_nulls;
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
	.is_full = NULL,
	.finish = bool_compressor_finish_and_reset,
};

static BoolCompressed *bool_compressed_from_parts(Simple8bRleSerialized *values,
												  Simple8bRleSerialized *validity_bitmap);

static void decompression_iterator_init(BoolDecompressionIterator *iter, void *compressed,
										Oid element_type, bool forward);

/*
 * Compressor framework functions and definitions for the bool_compress algorithm.
 */

extern BoolCompressor *
bool_compressor_alloc(void)
{
	BoolCompressor *compressor = palloc0(sizeof(*compressor));
	simple8brle_compressor_init(&compressor->values);
	simple8brle_compressor_init(&compressor->validity_bitmap);
	return compressor;
}

extern void
bool_compressor_append_null(BoolCompressor *compressor)
{
	/*
	 * We use parallel bitmaps of same size for validity and values, to support
	 * zero-copy decompression into ArrowArray. When an element is null,
	 * the particular value that goes into the values bitmap doesn't matter, so
	 * we add the last seen value, not to break the RLE sequences.
	 */
	compressor->has_nulls = true;
	simple8brle_compressor_append(&compressor->values, compressor->last_value);
	simple8brle_compressor_append(&compressor->validity_bitmap, 0);
	compressor->num_nulls++;
}

extern void
bool_compressor_append_value(BoolCompressor *compressor, bool next_val)
{
	compressor->last_value = next_val;
	simple8brle_compressor_append(&compressor->values, next_val);
	simple8brle_compressor_append(&compressor->validity_bitmap, 1);
}

extern void *
bool_compressor_finish(BoolCompressor *compressor)
{
	if (compressor == NULL)
		return NULL;

	Simple8bRleSerialized *values = simple8brle_compressor_finish(&compressor->values);
	if (values == NULL)
		return NULL;

	if (compressor->num_nulls == compressor->values.num_elements)
		return NULL;

	Simple8bRleSerialized *validity_bitmap =
		simple8brle_compressor_finish(&compressor->validity_bitmap);
	BoolCompressed *compressed;

	compressed = bool_compressed_from_parts(values, compressor->has_nulls ? validity_bitmap : NULL);
	/* When only nulls are present, we can return NULL */
	Assert(compressed == NULL || compressed->compression_algorithm == COMPRESSION_ALGORITHM_BOOL);
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
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_BOOL && iter->forward);
	Assert(iter->element_type == BOOLOID);

	BoolDecompressionIterator *bool_iter = (BoolDecompressionIterator *) iter;

	if (bool_iter->position >= bool_iter->values.num_elements)
		return (DecompressResult){
			.is_done = true,
		};

	/* check nulls */
	if (bool_iter->validity_bitmap.num_elements > 0)
	{
		bool is_null = !simple8brle_bitmap_get_at(&bool_iter->validity_bitmap, bool_iter->position);
		if (is_null)
		{
			bool_iter->position++;
			return (DecompressResult){
				.is_null = true,
			};
		}
	}

	bool val = simple8brle_bitmap_get_at(&bool_iter->values, bool_iter->position);
	bool_iter->position++;

	return (DecompressResult){
		.val = BoolGetDatum(val),
	};
}

extern DecompressionIterator *
bool_decompression_iterator_from_datum_forward(Datum bool_compressed, Oid element_type)
{
	BoolDecompressionIterator *iterator = palloc(sizeof(*iterator));
	CheckCompressedData(DatumGetPointer(bool_compressed) != NULL);
	decompression_iterator_init(iterator,
								(void *) PG_DETOAST_DATUM(bool_compressed),
								element_type,
								true);
	return &iterator->base;
}

extern DecompressResult
bool_decompression_iterator_try_next_reverse(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_BOOL && !iter->forward);
	Assert(iter->element_type == BOOLOID);

	BoolDecompressionIterator *bool_iter = (BoolDecompressionIterator *) iter;

	if (bool_iter->position < 0)
		return (DecompressResult){
			.is_done = true,
		};

	/* check nulls */
	if (bool_iter->validity_bitmap.num_elements > 0)
	{
		bool is_null = !simple8brle_bitmap_get_at(&bool_iter->validity_bitmap, bool_iter->position);
		if (is_null)
		{
			bool_iter->position--;
			return (DecompressResult){
				.is_null = true,
			};
		}
	}

	bool val = simple8brle_bitmap_get_at(&bool_iter->values, bool_iter->position);
	bool_iter->position--;

	return (DecompressResult){
		.val = BoolGetDatum(val),
	};
}

extern DecompressionIterator *
bool_decompression_iterator_from_datum_reverse(Datum bool_compressed, Oid element_type)
{
	BoolDecompressionIterator *iterator = palloc(sizeof(*iterator));
	CheckCompressedData(DatumGetPointer(bool_compressed) != NULL);
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
	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_BOOL);
	pq_sendbyte(buffer, data->has_nulls);
	simple8brle_serialized_send(buffer, (Simple8bRleSerialized *) data->values);
	if (data->has_nulls)
	{
		Simple8bRleSerialized *validity_bitmap =
			(Simple8bRleSerialized *) (((char *) data->values) +
									   simple8brle_serialized_total_size(
										   (Simple8bRleSerialized *) data->values));
		simple8brle_serialized_send(buffer, validity_bitmap);
	}
}

extern Datum
bool_compressed_recv(StringInfo buffer)
{
	uint8 has_nulls;
	Simple8bRleSerialized *values;
	Simple8bRleSerialized *validity_bitmap = NULL;
	BoolCompressed *compressed;

	has_nulls = pq_getmsgbyte(buffer);
	CheckCompressedData(has_nulls == 0 || has_nulls == 1);

	values = simple8brle_serialized_recv(buffer);
	if (has_nulls)
		validity_bitmap = simple8brle_serialized_recv(buffer);

	compressed = bool_compressed_from_parts(values, validity_bitmap);

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

extern ArrowArray *
bool_decompress_all(Datum compressed, Oid element_type, MemoryContext dest_mctx)
{
	MemoryContext old_context;
	Simple8bRleBitArray value_bits;
	Simple8bRleBitArray validity_bits;

	Simple8bRleSerialized *serialized_values = NULL;
	Simple8bRleSerialized *serialized_validity_bitmap = NULL;

	ArrowArray *result = NULL;
	uint64 *validity_bitmap = NULL;
	uint64 *decompressed_values = NULL;

	CheckCompressedData(DatumGetPointer(compressed) != NULL);

	void *detoasted = PG_DETOAST_DATUM(compressed);
	StringInfoData si = { .data = detoasted, .len = VARSIZE(compressed) };
	BoolCompressed *header = consumeCompressedData(&si, sizeof(BoolCompressed));

	Assert(header->has_nulls == 0 || header->has_nulls == 1);
	Assert(element_type == BOOLOID);

	serialized_values = bytes_deserialize_simple8b_and_advance(&si);
	const bool has_nulls = header->has_nulls == 1;

	if (has_nulls)
	{
		serialized_validity_bitmap = bytes_deserialize_simple8b_and_advance(&si);
	}

	/* Decompress the values directly to bit arrays */
	old_context = MemoryContextSwitchTo(dest_mctx);
	value_bits = simple8brle_bitarray_decompress(serialized_values, /* inverted*/ false);
	decompressed_values = value_bits.data;
	validity_bits =
		simple8brle_bitarray_decompress(serialized_validity_bitmap, /* inverted*/ false);
	validity_bitmap = validity_bits.data;
	MemoryContextSwitchTo(old_context);

	result = MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + sizeof(void *) * 2);
	const void **buffers = (const void **) &result[1];
	buffers[0] = validity_bitmap;
	buffers[1] = decompressed_values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = value_bits.num_elements;
	result->null_count = has_nulls ? (result->length - validity_bits.num_ones) : 0;
	return result;
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
	void *compressed = NULL;
	if (extended != NULL && extended->internal != NULL)
	{
		compressed = bool_compressor_finish(extended->internal);
		pfree(extended->internal);
		extended->internal = NULL;
	}
	return compressed;
}

static BoolCompressed *
bool_compressed_from_parts(Simple8bRleSerialized *values, Simple8bRleSerialized *validity_bitmap)
{
	uint32 validity_bitmap_size = 0;
	Size compressed_size;
	char *compressed_data;
	BoolCompressed *compressed;
	uint32 num_values = values != NULL ? values->num_elements : 0;
	uint32 values_size = values != NULL ? simple8brle_serialized_total_size(values) : 0;

	if (num_values == 0)
		return NULL;

	if (validity_bitmap != NULL)
		validity_bitmap_size = simple8brle_serialized_total_size(validity_bitmap);

	compressed_size = sizeof(BoolCompressed) + values_size + validity_bitmap_size;

	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	compressed_data = palloc(compressed_size);
	compressed = (BoolCompressed *) compressed_data;
	SET_VARSIZE(&compressed->vl_len_, compressed_size);

	compressed->compression_algorithm = COMPRESSION_ALGORITHM_BOOL;
	compressed->has_nulls = validity_bitmap_size != 0 ? 1 : 0;

	compressed_data += sizeof(*compressed);
	compressed_data = bytes_serialize_simple8b_and_advance(compressed_data, values_size, values);

	if (compressed->has_nulls == 1 && validity_bitmap != NULL)
	{
		CheckCompressedData(validity_bitmap->num_elements == num_values);
		bytes_serialize_simple8b_and_advance(compressed_data,
											 validity_bitmap_size,
											 validity_bitmap);
	}

	return compressed;
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
		.base = { .compression_algorithm = COMPRESSION_ALGORITHM_BOOL,
				  .forward = forward,
				  .element_type = element_type,
				  .try_next = (forward ? bool_decompression_iterator_try_next_forward :
										 bool_decompression_iterator_try_next_reverse) },
		.values = { 0 },
		.validity_bitmap = { 0 },
		.position = 0,
	};

	iter->values = simple8brle_bitmap_decompress(values);

	if (has_nulls)
	{
		Simple8bRleSerialized *validity_bitmap = bytes_deserialize_simple8b_and_advance(&si);
		iter->validity_bitmap = simple8brle_bitmap_decompress(validity_bitmap);
		CheckCompressedData(iter->validity_bitmap.num_elements == iter->values.num_elements);
	}

	if (!forward)
	{
		iter->position = iter->values.num_elements - 1;
	}
}
