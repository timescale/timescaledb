
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "postgres.h"

#include "compression/dictionary.h"

#include <access/htup_details.h>
#include <access/tupmacs.h>
#include <catalog/pg_aggregate.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include <base64_compat.h>

#include "compression/compression.h"
#include "compression/simple8b_rle.h"
#include "compression/array.h"
#include "compression/dictionary_hash.h"

/*
 * A compression bitmap is stored as
 *     bool has_nulls
 *     padding
 *     Oid element_type: the element stored by this compressed dictionary
 *     uint32 num_distinct: the number of distinct values
 *     simple8b_rle dictionary indexes: array of mappings from row to index into dictionary items
 * ArrayCompressed simple8b_rle nulls (optional) ArrayCompressed dictionary items
 */
typedef struct DictionaryCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls;
	uint8 padding[2];
	Oid element_type;
	uint32 num_distinct;
	/* alignment sentinel for the following fields */
	Simple8bRleSerialized alignment_sentinel[FLEXIBLE_ARRAY_MEMBER];
} DictionaryCompressed;

static void
pg_attribute_unused() assertions(void)
{
	DictionaryCompressed test_val = {};
	/* make sure no padding bytes make it to disk */
	StaticAssertStmt(sizeof(DictionaryCompressed) ==
						 sizeof(test_val.vl_len_) + sizeof(test_val.compression_algorithm) +
							 sizeof(test_val.has_nulls) + sizeof(test_val.padding) +
							 sizeof(test_val.element_type) + sizeof(test_val.num_distinct),
					 "CompressedDictionary wrong size");
	StaticAssertStmt(sizeof(DictionaryCompressed) == 16, "CompressedDictionary wrong size");
}

//////////////////
/// Compressor ///
//////////////////

// FIXME store (index + 1), and use 0 to mean NULL

typedef struct DictionaryCompressor
{
	dictionary_hash *dictionary_items;
	uint32 next_index;
	Oid type;
	int16 typlen;
	bool typbyval;
	char typalign;
	bool has_nulls;
	Simple8bRleCompressor dictionary_indexes;
	Simple8bRleCompressor nulls;
} DictionaryCompressor;

DictionaryCompressor *
dictionary_compressor_alloc(Oid type)
{
	DictionaryCompressor *compressor = palloc(sizeof(*compressor));
	TypeCacheEntry *tentry =
		lookup_type_cache(type, TYPECACHE_EQ_OPR_FINFO | TYPECACHE_HASH_PROC_FINFO);

	compressor->next_index = 0;
	compressor->has_nulls = false;
	compressor->type = type;
	compressor->typlen = tentry->typlen;
	compressor->typbyval = tentry->typbyval;
	compressor->typalign = tentry->typalign;

	compressor->dictionary_items = dictionary_hash_alloc(tentry);

	simple8brle_compressor_init(&compressor->dictionary_indexes);
	simple8brle_compressor_init(&compressor->nulls);
	return compressor;
}

void
dictionary_compressor_append_null(DictionaryCompressor *compressor)
{
	compressor->has_nulls = true;
	simple8brle_compressor_append(&compressor->nulls, 1);
}

void
dictionary_compressor_append(DictionaryCompressor *compressor, Datum val)
{
	bool found;
	DictionaryHashItem *dict_item;

	Assert(compressor != NULL);

	dict_item = dictionary_insert(compressor->dictionary_items, val, &found);

	if (!found)
	{
		// per_val->bitmap = roaring_dictionary_create();
		dict_item->index = compressor->next_index;
		dict_item->key = datumCopy(val, compressor->typbyval, compressor->typlen);
		compressor->next_index += 1;
	}

	simple8brle_compressor_append(&compressor->dictionary_indexes, dict_item->index);
	simple8brle_compressor_append(&compressor->nulls, 0);
}

typedef struct DictionaryCompressorSerializationInfo
{
	Size bitmaps_size;
	Size nulls_size;
	Size dictionary_size;
	Size total_size;
	uint32 num_distinct;
	Simple8bRleSerialized *dictionary_compressed_indexes;
	Simple8bRleSerialized *compressed_nulls;
	ArrayCompressorSerializationInfo *dictionary_serialization_info;
} DictionaryCompressorSerializationInfo;

static DictionaryCompressorSerializationInfo
compressor_get_serialization_info(DictionaryCompressor *compressor)
{
	Simple8bRleSerialized *dict_indexes =
		simple8brle_compressor_finish(&compressor->dictionary_indexes);
	Simple8bRleSerialized *nulls = simple8brle_compressor_finish(&compressor->nulls);
	dictionary_iterator dictionary_item_iterator;
	Datum *value_array = palloc(compressor->next_index * sizeof(*value_array));

	ArrayCompressor *array_comp = array_compressor_alloc(compressor->type);

	/* the total size is header size + bitmaps size + nulls? + data sizesize */
	DictionaryCompressorSerializationInfo sizes = {
		.dictionary_compressed_indexes = dict_indexes,
		.compressed_nulls = nulls,
	};
	Size header_size = sizeof(DictionaryCompressed);

	sizes.bitmaps_size = simple8brle_serialized_total_size(dict_indexes);
	sizes.total_size = MAXALIGN(header_size) + sizes.bitmaps_size;
	if (compressor->has_nulls)
		sizes.nulls_size = simple8brle_serialized_total_size(nulls);
	sizes.total_size += sizes.nulls_size;

	dictionary_start_iterate(compressor->dictionary_items, &dictionary_item_iterator);
	sizes.num_distinct = 0;
	for (DictionaryHashItem *dict_item =
			 dictionary_iterate(compressor->dictionary_items, &dictionary_item_iterator);
		 dict_item != NULL;
		 dict_item = dictionary_iterate(compressor->dictionary_items, &dictionary_item_iterator))
	{
		value_array[dict_item->index] = dict_item->key;
		sizes.num_distinct += 1;
	}
	for (int i = 0; i < sizes.num_distinct; i++)
	{
		array_compressor_append(array_comp, value_array[i]);
	}
	sizes.dictionary_serialization_info = array_compressor_get_serialization_info(array_comp);
	sizes.dictionary_size =
		array_compression_serialization_size(sizes.dictionary_serialization_info);
	sizes.total_size += sizes.dictionary_size;

	if (!AllocSizeIsValid(sizes.total_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));
	return sizes;
}

static DictionaryCompressed *
dictionary_compressed_from_serialization_info(DictionaryCompressorSerializationInfo sizes,
											  Oid element_type)
{
	char *data = palloc0(sizes.total_size);
	DictionaryCompressed *bitmap = (DictionaryCompressed *) data;
	SET_VARSIZE(bitmap->vl_len_, sizes.total_size);

	bitmap->compression_algorithm = COMPRESSION_ALGORITHM_DICTIONARY;
	bitmap->element_type = element_type;
	bitmap->has_nulls = sizes.nulls_size > 0 ? 1 : 0;
	bitmap->num_distinct = sizes.num_distinct;

	data = data + sizeof(DictionaryCompressed);
	data = bytes_serialize_simple8b_and_advance(data,
												sizes.bitmaps_size,
												sizes.dictionary_compressed_indexes);

	if (bitmap->has_nulls)
		data = bytes_serialize_simple8b_and_advance(data, sizes.nulls_size, sizes.compressed_nulls);

	data = bytes_serialize_array_compressor_and_advance(data,
														sizes.dictionary_size,
														sizes.dictionary_serialization_info);

	Assert(data - (char *) bitmap == sizes.total_size);
	return bitmap;
}

DictionaryCompressed *
dictionary_compressor_finish(DictionaryCompressor *compressor)
{
	DictionaryCompressorSerializationInfo sizes = compressor_get_serialization_info(compressor);
	return dictionary_compressed_from_serialization_info(sizes, compressor->type);
}

////////////////////
/// Decompressor ///
////////////////////

struct DictionaryDecompressionIterator
{
	DecompressionIterator base;
	const DictionaryCompressed *compressed;
	Datum *values;
	Simple8bRleDecompressionIterator bitmap;
	Simple8bRleDecompressionIterator nulls;
	bool has_nulls;
};

static void
dictionary_decompression_iterator_init(DictionaryDecompressionIterator *iter, const char *data,
									   bool scan_forward, Oid element_type)
{
	const DictionaryCompressed *bitmap = (const DictionaryCompressed *) data;
	Size total_size = VARSIZE(bitmap);
	Size remaining_size;
	Simple8bRleSerialized *s8_bitmap;
	DecompressionIterator *dictionary_iterator;

	*iter = (DictionaryDecompressionIterator){
		.base = {
			.compression_algorithm = COMPRESSION_ALGORITHM_DICTIONARY,
			.forward = scan_forward,
			.element_type = element_type,
			.try_next = (scan_forward ? dictionary_decompression_iterator_try_next_forward : dictionary_decompression_iterator_try_next_reverse),
		},
		.compressed = bitmap,
		.values = palloc(sizeof(Datum) * bitmap->num_distinct),
		.has_nulls = bitmap->has_nulls == 1,
	};

	data += sizeof(DictionaryCompressed);
	s8_bitmap = bytes_deserialize_simple8b_and_advance(&data);

	if (scan_forward)
		simple8brle_decompression_iterator_init_forward(&iter->bitmap, s8_bitmap);
	else
		simple8brle_decompression_iterator_init_reverse(&iter->bitmap, s8_bitmap);

	if (iter->has_nulls)
	{
		Simple8bRleSerialized *s8_null = bytes_deserialize_simple8b_and_advance(&data);
		if (scan_forward)
			simple8brle_decompression_iterator_init_forward(&iter->nulls, s8_null);
		else
			simple8brle_decompression_iterator_init_reverse(&iter->nulls, s8_null);
	}

	remaining_size = total_size - (data - (char *) bitmap);

	dictionary_iterator = array_decompression_iterator_alloc_forward(data,
																	 remaining_size,
																	 bitmap->element_type,
																	 /* has_nulls */ false);

	for (int i = 0; i < bitmap->num_distinct; i++)
	{
		DecompressResult res = array_decompression_iterator_try_next_forward(dictionary_iterator);
		Assert(!res.is_null);
		Assert(!res.is_done);
		iter->values[i] = res.val;
	}
	Assert(array_decompression_iterator_try_next_forward(dictionary_iterator).is_done);
}
DecompressionIterator *
tsl_dictionary_decompression_iterator_from_datum_forward(Datum dictionary_compressed,
														 Oid element_type)
{
	DictionaryDecompressionIterator *iterator = palloc(sizeof(*iterator));
	dictionary_decompression_iterator_init(iterator,
										   (void *) PG_DETOAST_DATUM(dictionary_compressed),
										   true,
										   element_type);
	return &iterator->base;
}

DecompressionIterator *
tsl_dictionary_decompression_iterator_from_datum_reverse(Datum dictionary_compressed,
														 Oid element_type)
{
	DictionaryDecompressionIterator *iterator = palloc(sizeof(*iterator));
	dictionary_decompression_iterator_init(iterator,
										   (void *) PG_DETOAST_DATUM(dictionary_compressed),
										   false,
										   element_type);
	return &iterator->base;
}

DecompressResult
dictionary_decompression_iterator_try_next_forward(DecompressionIterator *iter_base)
{
	DictionaryDecompressionIterator *iter;
	Simple8bRleDecompressResult result;

	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_DICTIONARY &&
		   iter_base->forward);
	iter = (DictionaryDecompressionIterator *) iter_base;

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

	result = simple8brle_decompression_iterator_try_next_forward(&iter->bitmap);
	if (result.is_done)
		return (DecompressResult){
			.is_done = true,
		};

	Assert(result.val < iter->compressed->num_distinct);
	return (DecompressResult){
		.val = iter->values[result.val],
		.is_null = false,
		.is_done = false,
	};
}

DecompressResult
dictionary_decompression_iterator_try_next_reverse(DecompressionIterator *iter_base)
{
	DictionaryDecompressionIterator *iter;
	Simple8bRleDecompressResult result;

	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_DICTIONARY &&
		   !iter_base->forward);
	iter = (DictionaryDecompressionIterator *) iter_base;

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

	result = simple8brle_decompression_iterator_try_next_reverse(&iter->bitmap);
	if (result.is_done)
		return (DecompressResult){
			.is_done = true,
		};

	// FIXME 0 should be a sentinel representing null
	Assert(result.val < iter->compressed->num_distinct);
	return (DecompressResult){
		.val = iter->values[result.val],
		.is_null = false,
		.is_done = false,
	};
}

/////////////////////
/// SQL Functions ///
/////////////////////

Datum
tsl_dictionary_compressor_append(PG_FUNCTION_ARGS)
{
	DictionaryCompressor *compressor =
		(DictionaryCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_dictionary_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		Oid type_to_compress = get_fn_expr_argtype(fcinfo->flinfo, 1);
		compressor = dictionary_compressor_alloc(type_to_compress);
	}
	if (PG_ARGISNULL(1))
		dictionary_compressor_append_null(compressor);
	else
		dictionary_compressor_append(compressor, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

Datum
tsl_dictionary_compressor_finish(PG_FUNCTION_ARGS)
{
	DictionaryCompressor *compressor =
		(DictionaryCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	if (compressor == NULL)
		PG_RETURN_NULL();

	PG_RETURN_POINTER(dictionary_compressor_finish(compressor));
}

/////////////////////
/// I/O Functions ///
/////////////////////

void
dictionary_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	uint32 data_size;
	uint32 size;
	char *namespace_name;
	Form_pg_type type_tuple;
	HeapTuple tup;
	const DictionaryCompressed *compressed_header;
	const char *compressed_data;

	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_DICTIONARY);
	compressed_header = (DictionaryCompressed *) header;

	compressed_data = (char *) compressed_header;

	compressed_data += sizeof(*compressed_header);

	data_size = VARSIZE(compressed_header);
	data_size -= sizeof(*compressed_header);

	pq_sendbyte(buffer, compressed_header->has_nulls == true);

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(compressed_header->element_type));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", compressed_header->element_type);

	type_tuple = (Form_pg_type) GETSTRUCT(tup);

	namespace_name = get_namespace_name(type_tuple->typnamespace);

	pq_sendstring(buffer, namespace_name);
	pq_sendstring(buffer, NameStr(type_tuple->typname));

	size = simple8brle_serialized_total_size((void *) compressed_data);
	simple8brle_serialized_send(buffer, (void *) compressed_data);
	compressed_data += size;
	data_size -= size;

	if (compressed_header->has_nulls)
	{
		uint32 size = simple8brle_serialized_total_size((void *) compressed_data);
		simple8brle_serialized_send(buffer, (void *) compressed_data);
		compressed_data += size;
		data_size -= size;
	}

	array_compressed_data_send(buffer,
							   compressed_data,
							   data_size,
							   compressed_header->element_type,
							   false);

	ReleaseSysCache(tup);
}

Datum
dictionary_compressed_recv(StringInfo buffer)
{
	DictionaryCompressorSerializationInfo data = {};
	uint8 has_nulls;
	const char *element_type_namespace;
	const char *element_type_name;
	Oid namespace_oid;
	Oid element_type;

	has_nulls = pq_getmsgbyte(buffer);
	if (has_nulls != 0 && has_nulls != 1)
		elog(ERROR, "invalid recv in dict: bad bool");

	element_type_namespace = pq_getmsgstring(buffer);
	element_type_name = pq_getmsgstring(buffer);

	namespace_oid = LookupExplicitNamespace(element_type_namespace, false);

	element_type = GetSysCacheOid2(TYPENAMENSP,
								   PointerGetDatum(element_type_name),
								   ObjectIdGetDatum(namespace_oid));
	if (!OidIsValid(element_type))
		elog(ERROR, "could not find type %s.%s", element_type_namespace, element_type_name);

	data.dictionary_compressed_indexes = simple8brle_serialized_recv(buffer);
	data.bitmaps_size = simple8brle_serialized_total_size(data.dictionary_compressed_indexes);
	data.total_size = MAXALIGN(sizeof(DictionaryCompressed)) + data.bitmaps_size;

	if (has_nulls)
	{
		data.compressed_nulls = simple8brle_serialized_recv(buffer);
		data.nulls_size = simple8brle_serialized_total_size(data.compressed_nulls);
		data.total_size += data.nulls_size;
	}

	data.dictionary_serialization_info = array_compressed_data_recv(buffer, element_type);
	data.dictionary_size = array_compression_serialization_size(data.dictionary_serialization_info);
	data.total_size += data.dictionary_size;
	data.num_distinct =
		array_compression_serialization_num_elements(data.dictionary_serialization_info);

	if (!AllocSizeIsValid(data.total_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	return PointerGetDatum(dictionary_compressed_from_serialization_info(data, element_type));
}
