/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "compression/array.h"

#include <access/htup_details.h>
#include <access/tupmacs.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <funcapi.h>

#include <base64_compat.h>

#include "compression/compression.h"
#include "compression/simple8b_rle.h"
#include "compat.h"
#include "datum_serialize.h"

#include <adts/char_vec.h>

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

static void
pg_attribute_unused() assertions(void)
{
	ArrayCompressed test_val = { { 0 } };
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
	 * before the alignment sentinal are 8-byte aligned, and also that the two Simple8bRleSerialized
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
	return info->sizes->num_elements;
}

char *
bytes_serialize_array_compressor_and_advance(char *dst, Size dst_size,
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
array_compressed_data_from_bytes(const char *serialized_data, Size data_size, Oid element_type,
								 bool has_nulls)
{
	ArrayCompressedData data = { .element_type = element_type };

	if (has_nulls)
	{
		Simple8bRleSerialized *nulls = bytes_deserialize_simple8b_and_advance(&serialized_data);
		data.nulls = nulls;
		data_size -= simple8brle_serialized_total_size(nulls);
	}

	data.sizes = bytes_deserialize_simple8b_and_advance(&serialized_data);
	data_size -= simple8brle_serialized_total_size(data.sizes);

	data.data = serialized_data;
	data.data_len = data_size;

	return data;
}

DecompressionIterator *
array_decompression_iterator_alloc_forward(const char *serialized_data, Size data_size,
										   Oid element_type, bool has_nulls)
{
	ArrayCompressedData data =
		array_compressed_data_from_bytes(serialized_data, data_size, element_type, has_nulls);

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
	ArrayCompressed *compressed_array_header;
	uint32 data_size;
	const char *compressed_data = (void *) PG_DETOAST_DATUM(compressed_array);

	compressed_array_header = (ArrayCompressed *) compressed_data;
	compressed_data += sizeof(*compressed_array_header);

	Assert(compressed_array_header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);

	data_size = VARSIZE(compressed_array_header);
	data_size -= sizeof(*compressed_array_header);

	if (element_type != compressed_array_header->element_type)
		elog(ERROR, "trying to decompress the wrong type");

	return array_decompression_iterator_alloc_forward(compressed_data,
													  data_size,
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

	Assert(iter->data_offset + datum_size.val <= iter->num_data_bytes);

	start_pointer = iter->data + iter->data_offset;
	val = bytes_to_datum_and_advance(iter->deserializer, &start_pointer);
	iter->data_offset += datum_size.val;
	Assert(iter->data + iter->data_offset == start_pointer);

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
	uint32 data_size;
	ArrayCompressedData array_compressed_data;
	ArrayDecompressionIterator *iterator = palloc(sizeof(*iterator));
	const char *compressed_data = (void *) PG_DETOAST_DATUM(compressed_array);
	iterator->base.compression_algorithm = COMPRESSION_ALGORITHM_ARRAY;
	iterator->base.forward = false;
	iterator->base.element_type = element_type;
	iterator->base.try_next = array_decompression_iterator_try_next_reverse;

	compressed_array_header = (ArrayCompressed *) compressed_data;
	compressed_data += sizeof(*compressed_array_header);

	Assert(compressed_array_header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	if (element_type != compressed_array_header->element_type)
		elog(ERROR, "trying to decompress the wrong type");

	data_size = VARSIZE(compressed_array_header);
	data_size -= sizeof(*compressed_array_header);

	array_compressed_data = array_compressed_data_from_bytes(compressed_data,
															 data_size,
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
	bool use_binary_recv;
	FmgrInfo recv_flinfo;
	Oid typIOParam;
	uint32 num_elements;
	uint32 i;
	Form_pg_type type_tuple;
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(element_type));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", element_type);

	type_tuple = (Form_pg_type) GETSTRUCT(tup);

	has_nulls = pq_getmsgbyte(buffer) != 0;
	if (has_nulls)
		simple8brle_decompression_iterator_init_forward(&nulls,
														simple8brle_serialized_recv(buffer));

	use_binary_recv = pq_getmsgbyte(buffer) != 0;
	if (use_binary_recv && !OidIsValid(type_tuple->typreceive))
		elog(ERROR, "could not find binary recv for type %s", NameStr(type_tuple->typname));

	if (use_binary_recv)
	{
		fmgr_info(type_tuple->typreceive, &recv_flinfo);
	}
	else
		fmgr_info(type_tuple->typinput, &recv_flinfo);

	typIOParam = getTypeIOParam(tup);

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

		if (use_binary_recv)
		{
			uint32 data_size = pq_getmsgint32(buffer);
			const char *bytes = pq_getmsgbytes(buffer, data_size);
			StringInfoData d = {
				.data = (char *) bytes,
				.len = data_size,
				.maxlen = data_size,
			};
			val = ReceiveFunctionCall(&recv_flinfo, &d, typIOParam, type_tuple->typtypmod);
		}
		else
		{
			const char *string = pq_getmsgstring(buffer);
			val =
				InputFunctionCall(&recv_flinfo, (char *) string, typIOParam, type_tuple->typtypmod);
		}

		array_compressor_append(compressor, val);
	}

	ReleaseSysCache(tup);

	return array_compressor_get_serialization_info(compressor);
}

void
array_compressed_data_send(StringInfo buffer, const char *serialized_data, Size data_size,
						   Oid element_type, bool has_nulls)
{
	ArrayCompressedData data;
	DecompressionIterator *data_iter;
	DecompressResult datum;
	FmgrInfo send_flinfo;
	bool use_binary_send;
	Form_pg_type type_tuple;
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(element_type));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", element_type);
	type_tuple = (Form_pg_type) GETSTRUCT(tup);

	data = array_compressed_data_from_bytes(serialized_data, data_size, element_type, has_nulls);

	pq_sendbyte(buffer, data.nulls != NULL);
	if (data.nulls != NULL)
		simple8brle_serialized_send(buffer, data.nulls);

	use_binary_send = OidIsValid(type_tuple->typsend);
	pq_sendbyte(buffer, use_binary_send == true);

	if (use_binary_send)
		fmgr_info(type_tuple->typsend, &send_flinfo);
	else
		fmgr_info(type_tuple->typoutput, &send_flinfo);

	/*
	 * we do not send data.sizes because the sizes need not be the same once
	 * deserialized, and we will need to recalculate them on recv. We do need
	 * to send the number of elements, which is always the same as the number
	 * of sizes.
	 */
	pq_sendint32(buffer, data.sizes->num_elements);
	data_iter = array_decompression_iterator_alloc_forward(serialized_data,
														   data_size,
														   element_type,
														   has_nulls);
	for (datum = array_decompression_iterator_try_next_forward(data_iter); !datum.is_done;
		 datum = array_decompression_iterator_try_next_forward(data_iter))
	{
		if (datum.is_null)
			continue;

		if (use_binary_send)
		{
			bytea *output = SendFunctionCall(&send_flinfo, datum.val);
			pq_sendint32(buffer, VARSIZE_ANY_EXHDR(output));
			pq_sendbytes(buffer, VARDATA(output), VARSIZE_ANY_EXHDR(output));
		}
		else
		{
			char *output = OutputFunctionCall(&send_flinfo, datum.val);
			pq_sendstring(buffer, output);
		}
	}

	ReleaseSysCache(tup);
}

/********************
 *** SQL Bindings ***
 ********************/

Datum
array_compressed_recv(StringInfo buffer)
{
	ArrayCompressorSerializationInfo *data;
	uint8 has_nulls;
	const char *element_type_namespace;
	const char *element_type_name;
	Oid namespace_oid;
	Oid element_type;

	has_nulls = pq_getmsgbyte(buffer);
	if (has_nulls != 0 && has_nulls != 1)
		elog(ERROR, "invalid recv in array: bad bool");

	element_type_namespace = pq_getmsgstring(buffer);
	element_type_name = pq_getmsgstring(buffer);

	namespace_oid = LookupExplicitNamespace(element_type_namespace, false);

	element_type = GetSysCacheOid2(TYPENAMENSP,
								   PointerGetDatum(element_type_name),
								   ObjectIdGetDatum(namespace_oid));
	if (!OidIsValid(element_type))
		elog(ERROR, "could not find type %s.%s", element_type_namespace, element_type_name);

	data = array_compressed_data_recv(buffer, element_type);

	PG_RETURN_POINTER(array_compressed_from_serialization_info(data, element_type));
}

void
array_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const char *compressed_data = (char *) header;
	uint32 data_size;
	char *namespace_name;
	Form_pg_type type_tuple;
	HeapTuple tup;
	ArrayCompressed *compressed_array_header;

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_ARRAY);
	compressed_array_header = (ArrayCompressed *) header;

	compressed_data += sizeof(*compressed_array_header);

	data_size = VARSIZE(compressed_array_header);
	data_size -= sizeof(*compressed_array_header);

	pq_sendbyte(buffer, compressed_array_header->has_nulls == true);

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(compressed_array_header->element_type));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", compressed_array_header->element_type);

	type_tuple = (Form_pg_type) GETSTRUCT(tup);

	namespace_name = get_namespace_name(type_tuple->typnamespace);

	pq_sendstring(buffer, namespace_name);
	pq_sendstring(buffer, NameStr(type_tuple->typname));

	array_compressed_data_send(buffer,
							   compressed_data,
							   data_size,
							   compressed_array_header->element_type,
							   compressed_array_header->has_nulls);
	ReleaseSysCache(tup);
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
