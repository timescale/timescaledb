/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/tupmacs.h>
#include <adts/char_vec.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <common/base64.h>
#include <funcapi.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "array.h"
#include "compression/compression.h"
#include "datum_serialize.h"
#include "simple8b_rle.h"
#include "simple8b_rle_bitarray.h"
#include "simple8b_rle_bitmap.h"

#include "compression/arrow_c_data_interface.h"

/* A "compressed" array
 *     uint8 has_nulls: 1 iff this has a nulls bitmap stored before the data
 *     Oid element_type: the element stored by this array
 *     simple8b_rle nulls: optional bitmap of nulls within the array
 *     simple8b_rle sizes: the sizes of each data element
 *     char data[]: the elements of the array
 */

typedef struct ArrayCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls;
	uint8 padding[6];
	Oid element_type;
	/* 8-byte alignment sentinel for the following fields */
	uint64 alignment_sentinel[FLEXIBLE_ARRAY_MEMBER];
} ArrayCompressed;

bool
array_compressed_has_nulls(const CompressedDataHeader *header)
{
	const ArrayCompressed *ac = (const ArrayCompressed *) header;
	return ac->has_nulls;
}

static void
pg_attribute_unused() assertions(void)
{
	ArrayCompressed test_val = { .vl_len_ = { 0 } };
	Simple8bRleSerialized test_simple8b = { 0 };
	/* make sure no padding bytes make it to disk */
	StaticAssertStmt(sizeof(ArrayCompressed) ==
						 sizeof(test_val.vl_len_) + sizeof(test_val.compression_algorithm) +
							 sizeof(test_val.has_nulls) + sizeof(test_val.padding) +
							 sizeof(test_val.element_type),
					 "ArrayCompressed wrong size");
	StaticAssertStmt(sizeof(ArrayCompressed) == 16, "ArrayCompressed wrong size");

	/* Note about alignment: the data[] field stores arbitrary Postgres types using store_att_byval
	 * and fetch_att. For this to work the data must be aligned according to the types alignment
	 * parameter (in CREATE TYPE; valid values are 1,2,4,8 bytes). In order to ease implementation,
	 * we simply align the start of data[] on a MAXALIGN (8-byte) boundary. Individual items in the
	 * array are then aligned as specified by the array element type. See top of array.h header in
	 * Postgres source code since it uses the same trick. Thus, we make sure that all fields
	 * before the alignment sentinel are 8-byte aligned, and also that the two Simple8bRleSerialized
	 * elements before the data element are themselves 8-byte aligned as well.
	 */

	StaticAssertStmt(offsetof(ArrayCompressed, alignment_sentinel) % MAXIMUM_ALIGNOF == 0,
					 "variable sized data must be 8-byte aligned");
	StaticAssertStmt(sizeof(Simple8bRleSerialized) % MAXIMUM_ALIGNOF == 0,
					 "Simple8bRle data must be 8-byte aligned");
	StaticAssertStmt(sizeof(test_simple8b.slots[0]) % MAXIMUM_ALIGNOF == 0,
					 "Simple8bRle variable-length slots must be 8-byte aligned");
}

typedef struct ArrayCompressedData
{
	Oid element_type;
	Simple8bRleSerialized *nulls; /* NULL if no nulls */
	Simple8bRleSerialized *sizes;
	const char *data;
	Size data_len;
} ArrayCompressedData;

typedef struct ArrayCompressor
{
	Simple8bRleCompressor nulls;
	Simple8bRleCompressor sizes;
	char_vec data;
	Oid type;
	DatumSerializer *serializer;
	bool has_nulls;
} ArrayCompressor;

typedef struct ExtendedCompressor
{
	Compressor base;
	ArrayCompressor *internal;
	Oid element_type;
} ExtendedCompressor;

typedef struct ArrayDecompressionIterator
{
	DecompressionIterator base;
	Simple8bRleDecompressionIterator nulls;
	Simple8bRleDecompressionIterator sizes;
	const char *data;
	uint32 num_data_bytes;
	uint32 data_offset;
	DatumDeserializer *deserializer;
	bool has_nulls;
} ArrayDecompressionIterator;

/******************
 *** Compressor ***
 ******************/

static void
array_compressor_append_datum(Compressor *compressor, Datum val)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = array_compressor_alloc(extended->element_type);

	array_compressor_append(extended->internal, val);
}

static void
array_compressor_append_null_value(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = array_compressor_alloc(extended->element_type);

	array_compressor_append_null(extended->internal);
}

static bool
array_compressor_is_full(Compressor *compressor, Datum val)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = array_compressor_alloc(extended->element_type);

	Size datum_size_and_align;
	ArrayCompressor *array_comp = (ArrayCompressor *) extended->internal;
	if (datum_serializer_value_may_be_toasted(array_comp->serializer))
		val = PointerGetDatum(PG_DETOAST_DATUM_PACKED(val));

	datum_size_and_align =
		datum_get_bytes_size(array_comp->serializer, array_comp->data.num_elements, val) -
		array_comp->data.num_elements;

	/* If we can't fit new datum in the max size, we are full */
	return (datum_size_and_align + array_comp->data.num_elements) > MAX_ARRAY_COMPRESSOR_SIZE_BYTES;
}

static void *
array_compressor_finish_and_reset(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	void *compressed = array_compressor_finish(extended->internal);
	pfree(extended->internal);
	extended->internal = NULL;
	return compressed;
}

const Compressor array_compressor = {
	.append_val = array_compressor_append_datum,
	.append_null = array_compressor_append_null_value,
	.is_full = array_compressor_is_full,
	.finish = array_compressor_finish_and_reset,
};

Compressor *
array_compressor_for_type(Oid element_type)
{
	ExtendedCompressor *compressor = palloc(sizeof(*compressor));
	*compressor = (ExtendedCompressor){
		.base = array_compressor,
		.element_type = element_type,
	};
	return &compressor->base;
}

ArrayCompressor *
array_compressor_alloc(Oid type_to_compress)
{
	ArrayCompressor *compressor = palloc(sizeof(*compressor));
	compressor->has_nulls = false;

	simple8brle_compressor_init(&compressor->nulls);
	simple8brle_compressor_init(&compressor->sizes);
	char_vec_init(&compressor->data, CurrentMemoryContext, 0);

	compressor->type = type_to_compress;
	compressor->serializer = create_datum_serializer(type_to_compress);
	return compressor;
}

void
array_compressor_append_null(ArrayCompressor *compressor)
{
	compressor->has_nulls = true;
	simple8brle_compressor_append(&compressor->nulls, 1);
}

void
array_compressor_append(ArrayCompressor *compressor, Datum val)
{
	Size datum_size_and_align;
	char *start_ptr;
	simple8brle_compressor_append(&compressor->nulls, 0);
	if (datum_serializer_value_may_be_toasted(compressor->serializer))
		val = PointerGetDatum(PG_DETOAST_DATUM_PACKED(val));

	datum_size_and_align =
		datum_get_bytes_size(compressor->serializer, compressor->data.num_elements, val) -
		compressor->data.num_elements;

	simple8brle_compressor_append(&compressor->sizes, datum_size_and_align);

	/* datum_to_bytes_and_advance will zero any padding bytes, so we need not do so here */
	char_vec_reserve(&compressor->data, datum_size_and_align);
	start_ptr = compressor->data.data + compressor->data.num_elements;
	compressor->data.num_elements += datum_size_and_align;

	datum_to_bytes_and_advance(compressor->serializer, start_ptr, &datum_size_and_align, val);
	Assert(datum_size_and_align == 0);
}

typedef struct ArrayCompressorSerializationInfo
{
	Simple8bRleSerialized *sizes;
	Simple8bRleSerialized *nulls;
	char_vec data;
	Size total;
} ArrayCompressorSerializationInfo;

ArrayCompressorSerializationInfo *
array_compressor_get_serialization_info(ArrayCompressor *compressor)
{
	ArrayCompressorSerializationInfo *info = palloc(sizeof(*info));
	*info = (ArrayCompressorSerializationInfo){
		.sizes = simple8brle_compressor_finish(&compressor->sizes),
		.nulls = compressor->has_nulls ? simple8brle_compressor_finish(&compressor->nulls) : NULL,
		.data = compressor->data,
		.total = 0,
	};
	if (info->nulls != NULL)
		info->total += simple8brle_serialized_total_size(info->nulls);

	if (info->sizes != NULL)
		info->total += simple8brle_serialized_total_size(info->sizes);

	info->total += compressor->data.num_elements;
	return info;
}

Size
array_compression_serialization_size(ArrayCompressorSerializationInfo *info)
{
	return info->total;
}

uint32
array_compression_serialization_num_elements(ArrayCompressorSerializationInfo *info)
{
	CheckCompressedData(info->sizes != NULL);
	return info->sizes->num_elements;
}

char *
bytes_serialize_array_compressor_and_advance(char *dst, PG_USED_FOR_ASSERTS_ONLY Size dst_size,
											 ArrayCompressorSerializationInfo *info)
{
	uint32 sizes_bytes = simple8brle_serialized_total_size(info->sizes);

	Assert(dst_size == info->total);

	if (info->nulls != NULL)
	{
		uint32 nulls_bytes = simple8brle_serialized_total_size(info->nulls);
		Assert(dst_size >= nulls_bytes);
		dst = bytes_serialize_simple8b_and_advance(dst, nulls_bytes, info->nulls);
		dst_size -= nulls_bytes;
	}

	Assert(dst_size >= sizes_bytes);
	dst = bytes_serialize_simple8b_and_advance(dst, sizes_bytes, info->sizes);
	dst_size -= sizes_bytes;

	Assert(dst_size == info->data.num_elements);
	memcpy(dst, info->data.data, info->data.num_elements);
	return dst + info->data.num_elements;
}

static ArrayCompressed *
array_compressed_from_serialization_info(ArrayCompressorSerializationInfo *info, Oid element_type)
{
	char *compressed_data;
	ArrayCompressed *compressed_array;
	Size compressed_size = sizeof(ArrayCompressed) + info->total;

	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	compressed_data = palloc0(compressed_size);
	compressed_array = (ArrayCompressed *) compressed_data;
	*compressed_array = (ArrayCompressed){
		.compression_algorithm = COMPRESSION_ALGORITHM_ARRAY,
		.has_nulls = info->nulls != NULL,
		.element_type = element_type,
	};
	SET_VARSIZE(compressed_array->vl_len_, compressed_size);
	compressed_data += sizeof(ArrayCompressed);
	compressed_size -= sizeof(ArrayCompressed);

	bytes_serialize_array_compressor_and_advance(compressed_data, compressed_size, info);
	return compressed_array;
}

void *
array_compressor_finish(ArrayCompressor *compressor)
{
	ArrayCompressorSerializationInfo *info = array_compressor_get_serialization_info(compressor);
	if (info->sizes == NULL)
		return NULL;

	return array_compressed_from_serialization_info(info, compressor->type);
}

/******************
 *** Decompress ***
 ******************/

static ArrayCompressedData
array_compressed_data_from_bytes(StringInfo serialized_data, Oid element_type, bool has_nulls)
{
	ArrayCompressedData data = { .element_type = element_type };

	if (has_nulls)
	{
		data.nulls = bytes_deserialize_simple8b_and_advance(serialized_data);
	}

	data.sizes = bytes_deserialize_simple8b_and_advance(serialized_data);

	data.data = serialized_data->data + serialized_data->cursor;
	data.data_len = serialized_data->len - serialized_data->cursor;

	return data;
}

DecompressionIterator *
array_decompression_iterator_alloc_forward(StringInfo serialized_data, Oid element_type,
										   bool has_nulls)
{
	ArrayCompressedData data =
		array_compressed_data_from_bytes(serialized_data, element_type, has_nulls);

	ArrayDecompressionIterator *iterator = palloc(sizeof(*iterator));
	iterator->base.compression_algorithm = COMPRESSION_ALGORITHM_ARRAY;
	iterator->base.forward = true;
	iterator->base.element_type = element_type;
	iterator->base.try_next = array_decompression_iterator_try_next_forward;

	iterator->has_nulls = data.nulls != NULL;
	if (iterator->has_nulls)
		simple8brle_decompression_iterator_init_forward(&iterator->nulls, data.nulls);

	simple8brle_decompression_iterator_init_forward(&iterator->sizes, data.sizes);

	iterator->data = data.data;
	iterator->num_data_bytes = data.data_len;
	iterator->data_offset = 0;
	iterator->deserializer = create_datum_deserializer(iterator->base.element_type);

	return &iterator->base;
}

DecompressionIterator *
tsl_array_decompression_iterator_from_datum_forward(Datum compressed_array, Oid element_type)
{
	void *compressed_data = (void *) PG_DETOAST_DATUM(compressed_array);
	StringInfoData si = { .data = compressed_data, .len = VARSIZE(compressed_data) };

	ArrayCompressed *compressed_array_header = consumeCompressedData(&si, sizeof(ArrayCompressed));

	Assert(compressed_array_header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);

	CheckCompressedData(element_type == compressed_array_header->element_type);

	return array_decompression_iterator_alloc_forward(&si,
													  compressed_array_header->element_type,
													  compressed_array_header->has_nulls == 1);
}

extern DecompressResult
array_decompression_iterator_try_next_forward(DecompressionIterator *general_iter)
{
	Simple8bRleDecompressResult datum_size;
	ArrayDecompressionIterator *iter;
	Datum val;
	const char *start_pointer;

	Assert(general_iter->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY &&
		   general_iter->forward);
	iter = (ArrayDecompressionIterator *) general_iter;

	if (iter->has_nulls)
	{
		Simple8bRleDecompressResult null =
			simple8brle_decompression_iterator_try_next_forward(&iter->nulls);
		if (null.is_done)
			return (DecompressResult){
				.is_done = true,
			};

		if (null.val != 0)
			return (DecompressResult){
				.is_null = true,
			};
	}

	datum_size = simple8brle_decompression_iterator_try_next_forward(&iter->sizes);
	if (datum_size.is_done)
		return (DecompressResult){
			.is_done = true,
		};

	CheckCompressedData(iter->data_offset + datum_size.val <= iter->num_data_bytes);

	start_pointer = iter->data + iter->data_offset;
	val = bytes_to_datum_and_advance(iter->deserializer, &start_pointer);
	iter->data_offset += datum_size.val;
	CheckCompressedData(iter->data + iter->data_offset == start_pointer);

	return (DecompressResult){
		.val = val,
	};
}

/**************************
 *** Decompress Reverse ***
 **************************/

DecompressionIterator *
tsl_array_decompression_iterator_from_datum_reverse(Datum compressed_array, Oid element_type)
{
	ArrayCompressed *compressed_array_header;
	ArrayCompressedData array_compressed_data;
	ArrayDecompressionIterator *iterator = palloc(sizeof(*iterator));
	iterator->base.compression_algorithm = COMPRESSION_ALGORITHM_ARRAY;
	iterator->base.forward = false;
	iterator->base.element_type = element_type;
	iterator->base.try_next = array_decompression_iterator_try_next_reverse;

	void *compressed_data = PG_DETOAST_DATUM(compressed_array);
	StringInfoData si = { .data = compressed_data, .len = VARSIZE(compressed_data) };
	compressed_array_header = consumeCompressedData(&si, sizeof(ArrayCompressed));

	Assert(compressed_array_header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	if (element_type != compressed_array_header->element_type)
		elog(ERROR, "trying to decompress the wrong type");

	array_compressed_data = array_compressed_data_from_bytes(&si,
															 compressed_array_header->element_type,
															 compressed_array_header->has_nulls);

	iterator->has_nulls = array_compressed_data.nulls != NULL;
	if (iterator->has_nulls)
		simple8brle_decompression_iterator_init_reverse(&iterator->nulls,
														array_compressed_data.nulls);

	simple8brle_decompression_iterator_init_reverse(&iterator->sizes, array_compressed_data.sizes);

	iterator->data = array_compressed_data.data;
	iterator->num_data_bytes = array_compressed_data.data_len;
	iterator->data_offset = iterator->num_data_bytes;
	iterator->deserializer = create_datum_deserializer(iterator->base.element_type);

	return &iterator->base;
}

static ArrowArray *tsl_bool_array_decompress_all(Datum compressed_array, Oid element_type,
												 MemoryContext dest_mctx);
static ArrowArray *tsl_text_array_decompress_all(Datum compressed_array, Oid element_type,
												 MemoryContext dest_mctx);
static ArrowArray *tsl_uuid_array_decompress_all(Datum compressed_array, Oid element_type,
												 MemoryContext dest_mctx);

/* Pass through to the specialized functions below for BOOL and TEXT */
ArrowArray *
tsl_array_decompress_all(Datum compressed_array, Oid element_type, MemoryContext dest_mctx)
{
	switch (element_type)
	{
		case BOOLOID:
			return tsl_bool_array_decompress_all(compressed_array, element_type, dest_mctx);
		case TEXTOID:
			return tsl_text_array_decompress_all(compressed_array, element_type, dest_mctx);
		case UUIDOID:
			return tsl_uuid_array_decompress_all(compressed_array, element_type, dest_mctx);
		default:
			elog(ERROR, "unsupported array type %u for bulk decompression", element_type);
			break;
	}
	return NULL;
}

static ArrowArray *
tsl_bool_array_decompress_all(Datum compressed_array, Oid element_type, MemoryContext dest_mctx)
{
	Assert(element_type == BOOLOID);
	void *compressed_data = PG_DETOAST_DATUM(compressed_array);
	StringInfoData si = { .data = compressed_data, .len = VARSIZE(compressed_data) };
	ArrayCompressed *header = consumeCompressedData(&si, sizeof(ArrayCompressed));

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	CheckCompressedData(header->element_type == BOOLOID);

	Simple8bRleSerialized *nulls_serialized = NULL;
	if (header->has_nulls)
	{
		nulls_serialized = bytes_deserialize_simple8b_and_advance(&si);
	}

	Simple8bRleSerialized *sizes_serialized = bytes_deserialize_simple8b_and_advance(&si);

	const uint32 n_notnull = sizes_serialized->num_elements;
	const uint32 n_total = header->has_nulls ? nulls_serialized->num_elements : n_notnull;
	const uint32 n_padded_bits = n_total + 63;
	const uint32 n_padded_bytes = n_padded_bits / 8;

	uint64 *validity_bitmap = NULL;
	uint64 *values = MemoryContextAllocZero(dest_mctx, n_padded_bytes);

	MemoryContext old_context = MemoryContextSwitchTo(dest_mctx);
	/* Decompress the nulls */
	Simple8bRleBitArray validity_bits =
		simple8brle_bitarray_decompress(nulls_serialized, /* inverted*/ true);
	validity_bitmap = validity_bits.data;
	MemoryContextSwitchTo(old_context);

	/* Decompress the values using the iterator based decompressor */
	{
		int position = 0;
		DecompressionIterator *iter =
			tsl_array_decompression_iterator_from_datum_forward(PointerGetDatum(compressed_data),
																BOOLOID);
		for (DecompressResult r = array_decompression_iterator_try_next_forward(iter); !r.is_done;
			 r = array_decompression_iterator_try_next_forward(iter))
		{
			if (!r.is_null)
			{
				bool data = DatumGetBool(r.val) == true;
				if (data)
				{
					arrow_set_row_validity(values, position, true);
				}
			}
			++position;
		}
	}

	ArrowArray *result =
		MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + (sizeof(void *) * 2));
	const void **buffers = (const void **) &result[1];
	buffers[0] = validity_bitmap;
	buffers[1] = values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = n_total - n_notnull;
	return result;
}

static ArrowArray *
tsl_uuid_array_decompress_all(Datum compressed_array, Oid element_type, MemoryContext dest_mctx)
{
	Assert(element_type == UUIDOID);
	void *compressed_data = PG_DETOAST_DATUM(compressed_array);
	StringInfoData si = { .data = compressed_data, .len = VARSIZE_ANY(compressed_data) };
	ArrayCompressed *header = consumeCompressedData(&si, sizeof(ArrayCompressed));

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	CheckCompressedData(header->element_type == UUIDOID);

	Simple8bRleSerialized *nulls_serialized = NULL;
	if (header->has_nulls)
	{
		nulls_serialized = bytes_deserialize_simple8b_and_advance(&si);
	}

	Simple8bRleSerialized *sizes_serialized = bytes_deserialize_simple8b_and_advance(&si);

	const uint32 n_notnull = sizes_serialized->num_elements;
	/* the nulls_serialized test shouldn't be necessary, but clang needs this to pass without
	 * warnings */
	const uint32 n_total = (header->has_nulls && nulls_serialized != NULL) ?
							   nulls_serialized->num_elements :
							   n_notnull;
	const uint32 n_bytes = n_total * 16;

	const uint64 *restrict compressed_non_null_values = (const uint64 *) (si.data + si.cursor);
	CheckCompressedData(n_notnull * 16 <= (uint32) (si.len - si.cursor));

	uint64 *restrict validity_bitmap = NULL;
	uint64 *restrict values = MemoryContextAlloc(dest_mctx, n_bytes);

	MemoryContext old_context = MemoryContextSwitchTo(dest_mctx);
	/* Decompress the nulls */
	Simple8bRleBitArray validity_bits =
		simple8brle_bitarray_decompress(nulls_serialized, /* inverted*/ true);
	validity_bitmap = validity_bits.data;
	MemoryContextSwitchTo(old_context);

	/* Check the alignment of compressed_non_null_values */
	if (((uintptr_t) compressed_non_null_values % 8) == 0)
	{
		int position = 0;
		for (uint32 i = 0; i < n_total; i++)
		{
			if (arrow_row_is_valid(validity_bitmap, i))
			{
				/* Copy the 16 bytes of the UUID with simple assignment, because we know it is
				 * aligned */
				values[i * 2] = compressed_non_null_values[position * 2];
				values[i * 2 + 1] = compressed_non_null_values[position * 2 + 1];
				position++;
			}
		}
	}
	else
	{
		int position = 0;
		for (uint32 i = 0; i < n_total; i++)
		{
			if (arrow_row_is_valid(validity_bitmap, i))
			{
				/* Copy the 16 bytes of the UUID with memcpy */
				memcpy(&values[i * 2], &compressed_non_null_values[position * 2], 16);
				position++;
			}
		}
	}

	ArrowArray *result =
		MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + (sizeof(void *) * 2));
	const void **buffers = (const void **) &result[1];
	buffers[0] = validity_bitmap;
	buffers[1] = values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = n_total - n_notnull;
	return result;
}

#define ELEMENT_TYPE uint32
#include "simple8b_rle_decompress_all.h"
#undef ELEMENT_TYPE

static ArrowArray *
tsl_text_array_decompress_all(Datum compressed_array, Oid element_type, MemoryContext dest_mctx)
{
	Assert(element_type == TEXTOID);
	void *compressed_data = PG_DETOAST_DATUM(compressed_array);
	StringInfoData si = { .data = compressed_data, .len = VARSIZE(compressed_data) };
	ArrayCompressed *header = consumeCompressedData(&si, sizeof(ArrayCompressed));

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	CheckCompressedData(header->element_type == TEXTOID);

	return text_array_decompress_all_serialized_no_header(&si, header->has_nulls, dest_mctx);
}

ArrowArray *
text_array_decompress_all_serialized_no_header(StringInfo si, bool has_nulls,
											   MemoryContext dest_mctx)
{
	Simple8bRleSerialized *nulls_serialized = NULL;
	if (has_nulls)
	{
		nulls_serialized = bytes_deserialize_simple8b_and_advance(si);
	}

	Simple8bRleSerialized *sizes_serialized = bytes_deserialize_simple8b_and_advance(si);

	uint32 n_notnull;
	const uint32 *sizes = simple8brle_decompress_all_uint32(sizes_serialized, &n_notnull);
	const uint32 n_total = has_nulls ? nulls_serialized->num_elements : n_notnull;
	CheckCompressedData(n_total >= n_notnull);

	uint32 *offsets =
		(uint32 *) MemoryContextAlloc(dest_mctx,
									  pad_to_multiple(64, sizeof(*offsets) * (n_total + 1)));
	uint8 *arrow_bodies =
		(uint8 *) MemoryContextAlloc(dest_mctx, pad_to_multiple(64, si->len - si->cursor));

	uint32 offset = 0;
	for (uint32 i = 0; i < n_notnull; i++)
	{
		const void *unaligned = consumeCompressedData(si, sizes[i]);

		/*
		 * We start reading from the end of previous datum, but this pointer
		 * might be not aligned as required for varlena-4b struct. We have to
		 * align it here. Note that sizes[i] includes the alignment as well in
		 * addition to the varlena size.
		 *
		 * See the corresponding row-by-row code in bytes_to_datum_and_advance().
		 */
		const void *vardata =
			DatumGetPointer(att_align_pointer(unaligned, TYPALIGN_INT, -1, unaligned));

		/*
		 * Check for potentially corrupt varlena headers since we're reading them
		 * directly from compressed data.
		 */
		if (VARATT_IS_4B_U(vardata))
		{
			/*
			 * Full varsize must be larger or equal than the header size so that
			 * the calculation of size without header doesn't overflow.
			 */
			CheckCompressedData(VARSIZE_4B(vardata) >= VARHDRSZ);
		}
		else if (VARATT_IS_1B(vardata))
		{
			/* Can't have a TOAST pointer here. */
			CheckCompressedData(!VARATT_IS_1B_E(vardata));

			/*
			 * Full varsize must be larger or equal than the header size so that
			 * the calculation of size without header doesn't overflow.
			 */
			CheckCompressedData(VARSIZE_1B(vardata) >= VARHDRSZ_SHORT);
		}
		else
		{
			/*
			 * Can only have an uncompressed datum with 1-byte or 4-byte header
			 * here, no TOAST or compressed data.
			 */
			CheckCompressedData(false);
		}

		/*
		 * Size of varlena plus alignment must match the size stored in the
		 * sizes array for this element.
		 */
		const Datum alignment_bytes = PointerGetDatum(vardata) - PointerGetDatum(unaligned);
		CheckCompressedData(VARSIZE_ANY(vardata) + alignment_bytes == sizes[i]);

		const uint32 textlen = VARSIZE_ANY_EXHDR(vardata);
		memcpy(&arrow_bodies[offset], VARDATA_ANY(vardata), textlen);

		offsets[i] = offset;

		CheckCompressedData(offset <= offset + textlen); /* Check for overflow. */
		offset += textlen;
	}
	offsets[n_notnull] = offset;

	uint64 *restrict validity_bitmap = NULL;
	if (has_nulls)
	{
		const int validity_bitmap_bytes = sizeof(uint64) * (pad_to_multiple(64, n_total) / 64);
		validity_bitmap = MemoryContextAlloc(dest_mctx, validity_bitmap_bytes);

		/*
		 * First, mark all data as valid, we will fill the nulls later if needed.
		 * Note that the validity bitmap size is a multiple of 64 bits. We have to
		 * fill the tail bits with zeros, because the corresponding elements are not
		 * valid.
		 *
		 */
		memset(validity_bitmap, 0xFF, validity_bitmap_bytes);
		if (n_total % 64)
		{
			const uint64 tail_mask = ~0ULL >> (64 - n_total % 64);
			validity_bitmap[n_total / 64] &= tail_mask;
		}

		/*
		 * We have decompressed the data with nulls skipped, reshuffle it
		 * according to the nulls bitmap.
		 */
		const Simple8bRleBitmap nulls = simple8brle_bitmap_decompress(nulls_serialized);
		CheckCompressedData(n_notnull + simple8brle_bitmap_num_ones(&nulls) == n_total);

		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			/*
			 * The index of the corresponding offset is higher by one than
			 * the index of the element. The offset[0] is never affected by
			 * this shuffling and is always 0.
			 * Note that unlike the usual null reshuffling in other algorithms,
			 * for offsets, even if all elements are null, the starting offset
			 * is well-defined and we can do this assignment. This case is only
			 * accessible through fuzzing. Through SQL, all-null batches result
			 * in a null compressed value.
			 */
			Assert(current_notnull_element + 1 >= 0);
			offsets[i + 1] = offsets[current_notnull_element + 1];

			if (simple8brle_bitmap_get_at(&nulls, i))
			{
				arrow_set_row_validity(validity_bitmap, i, false);
			}
			else
			{
				Assert(current_notnull_element >= 0);
				current_notnull_element--;
			}
		}

		Assert(current_notnull_element == -1);
	}

	ArrowArray *result =
		MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + (sizeof(void *) * 3));
	const void **buffers = (const void **) &result[1];
	buffers[0] = validity_bitmap;
	buffers[1] = offsets;
	buffers[2] = arrow_bodies;
	result->n_buffers = 3;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = n_total - n_notnull;
	return result;
}

DecompressResult
array_decompression_iterator_try_next_reverse(DecompressionIterator *base_iter)
{
	Simple8bRleDecompressResult datum_size;
	ArrayDecompressionIterator *iter;
	Datum val;
	const char *start_pointer;

	Assert(base_iter->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY && !base_iter->forward);
	iter = (ArrayDecompressionIterator *) base_iter;

	if (iter->has_nulls)
	{
		Simple8bRleDecompressResult null =
			simple8brle_decompression_iterator_try_next_reverse(&iter->nulls);
		if (null.is_done)
			return (DecompressResult){
				.is_done = true,
			};

		if (null.val != 0)
			return (DecompressResult){
				.is_null = true,
			};
	}

	datum_size = simple8brle_decompression_iterator_try_next_reverse(&iter->sizes);
	if (datum_size.is_done)
		return (DecompressResult){
			.is_done = true,
		};

	Assert((int64) iter->data_offset - (int64) datum_size.val >= 0);

	iter->data_offset -= datum_size.val;
	start_pointer = iter->data + iter->data_offset;
	val = bytes_to_datum_and_advance(iter->deserializer, &start_pointer);

	return (DecompressResult){
		.val = val,
	};
}

/*********************
 ***  send / recv  ***
 *********************/

ArrayCompressorSerializationInfo *
array_compressed_data_recv(StringInfo buffer, Oid element_type)
{
	ArrayCompressor *compressor = array_compressor_alloc(element_type);
	Simple8bRleDecompressionIterator nulls;
	uint8 has_nulls;
	DatumDeserializer *deser = create_datum_deserializer(element_type);
	bool use_binary_recv;
	uint32 num_elements;
	uint32 i;

	has_nulls = pq_getmsgbyte(buffer) != 0;
	if (has_nulls)
		simple8brle_decompression_iterator_init_forward(&nulls,
														simple8brle_serialized_recv(buffer));

	use_binary_recv = pq_getmsgbyte(buffer) != 0;

	/* This is actually the number of not-null elements */
	num_elements = pq_getmsgint32(buffer);

	/* if there are nulls, use that count instead */
	if (has_nulls)
		num_elements = nulls.num_elements;

	for (i = 0; i < num_elements; i++)
	{
		Datum val;
		if (has_nulls)
		{
			Simple8bRleDecompressResult null =
				simple8brle_decompression_iterator_try_next_forward(&nulls);
			Assert(!null.is_done);
			if (null.val)
			{
				array_compressor_append_null(compressor);
				continue;
			}
		}

		val = binary_string_to_datum(deser,
									 use_binary_recv ? BINARY_ENCODING : TEXT_ENCODING,
									 buffer);

		array_compressor_append(compressor, val);
	}

	return array_compressor_get_serialization_info(compressor);
}

void
array_compressed_data_send(StringInfo buffer, const char *_serialized_data, Size _data_size,
						   Oid element_type, bool has_nulls)
{
	DecompressResult datum;
	DatumSerializer *serializer = create_datum_serializer(element_type);
	BinaryStringEncoding encoding = datum_serializer_binary_string_encoding(serializer);

	StringInfoData si = { .data = (char *) _serialized_data, .len = _data_size };
	ArrayCompressedData array_compressed_data =
		array_compressed_data_from_bytes(&si, element_type, has_nulls);

	si.cursor = 0;
	DecompressionIterator *data_iter =
		array_decompression_iterator_alloc_forward(&si, element_type, has_nulls);

	pq_sendbyte(buffer, array_compressed_data.nulls != NULL);
	if (array_compressed_data.nulls != NULL)
		simple8brle_serialized_send(buffer, array_compressed_data.nulls);

	pq_sendbyte(buffer, encoding == BINARY_ENCODING);

	/*
	 * we do not send data.sizes because the sizes need not be the same once
	 * deserialized, and we will need to recalculate them on recv. We do need
	 * to send the number of elements, which is always the same as the number
	 * of sizes.
	 */
	pq_sendint32(buffer, array_compressed_data.sizes->num_elements);
	for (datum = array_decompression_iterator_try_next_forward(data_iter); !datum.is_done;
		 datum = array_decompression_iterator_try_next_forward(data_iter))
	{
		if (datum.is_null)
			continue;

		datum_append_to_binary_string(serializer, encoding, buffer, datum.val);
	}
}

/********************
 *** SQL Bindings ***
 ********************/

Datum
array_compressed_recv(StringInfo buffer)
{
	uint8 has_nulls;
	Oid element_type;

	has_nulls = pq_getmsgbyte(buffer);
	CheckCompressedData(has_nulls == 0 || has_nulls == 1);

	element_type = binary_string_get_type(buffer);

	ArrayCompressorSerializationInfo *info = array_compressed_data_recv(buffer, element_type);

	CheckCompressedData(info->sizes != NULL);
	CheckCompressedData(has_nulls == (info->nulls != NULL));

	PG_RETURN_POINTER(array_compressed_from_serialization_info(info, element_type));
}

void
array_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const char *compressed_data = (char *) header;
	uint32 data_size;
	ArrayCompressed *compressed_array_header;

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	compressed_array_header = (ArrayCompressed *) header;

	compressed_data += sizeof(*compressed_array_header);

	data_size = VARSIZE(compressed_array_header);
	data_size -= sizeof(*compressed_array_header);

	pq_sendbyte(buffer, compressed_array_header->has_nulls == true);

	type_append_to_binary_string(compressed_array_header->element_type, buffer);

	array_compressed_data_send(buffer,
							   compressed_data,
							   data_size,
							   compressed_array_header->element_type,
							   compressed_array_header->has_nulls);
}

extern Datum
tsl_array_compressor_append(PG_FUNCTION_ARGS)
{
	ArrayCompressor *compressor =
		(ArrayCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_array_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		Oid type_to_compress = get_fn_expr_argtype(fcinfo->flinfo, 1);
		compressor = array_compressor_alloc(type_to_compress);
	}
	if (PG_ARGISNULL(1))
		array_compressor_append_null(compressor);
	else
		array_compressor_append(compressor, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

extern Datum
tsl_array_compressor_finish(PG_FUNCTION_ARGS)
{
	ArrayCompressor *compressor =
		(ArrayCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	void *compressed;
	if (compressor == NULL)
		PG_RETURN_NULL();

	compressed = array_compressor_finish(compressor);
	if (compressed == NULL)
		PG_RETURN_NULL();

	PG_RETURN_POINTER(compressed);
}
