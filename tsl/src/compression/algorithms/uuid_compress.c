/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "uuid_compress.h"
#include "adts/uint64_vec.h"
#include "common/hashfn.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "deltadelta.h"
#include "dictionary.h"
#include "lib/hyperloglog.h"
#include "null.h"
#include "simple8b_rle.h"
#include "utils/palloc.h"

#ifdef TS_USE_UMASH
#include "import/umash.h"
#endif

typedef union UuidType
{
	uint64 components[2];
	pg_uuid_t uuid;
} UuidType;

typedef enum
{
	UUID_COMPRESS_SUBTYPE_DELTADELTA = 0,
	UUID_COMPRESS_SUBTYPE_COUNT = 1, /* Must be last */
} UuidCompressSubtype;

typedef struct UuidCompressed
{
	CompressedDataHeaderFields; /* this uses 5 bytes */
	uint8 subtype;
	uint16 num_nulls;
	uint32 timestamp_size;
	uint32 rand_b_and_variant_size;
	/* 8-byte alignment sentinel for the following fields */
	uint64 alignment_sentinel[FLEXIBLE_ARRAY_MEMBER];
} UuidCompressed;

static void
pg_attribute_unused() assertions(void)
{
	StaticAssertStmt(sizeof(UuidCompressed) == 16, "UuidCompressed wrong size");
	StaticAssertStmt(offsetof(UuidCompressed, alignment_sentinel) % MAXIMUM_ALIGNOF == 0,
					 "variable sized data must be 8-byte aligned");
}

typedef struct UuidDecompressionIterator
{
	DecompressionIterator base;
	int32 position;			 /* position within the total */
	int32 total_elements;	 /* total number of entries plus nulls */
	ArrowArray *uuid_buffer; /* init does a decompress_all to get this */
} UuidDecompressionIterator;

/*
 * HyperLogLog parameters.
 *
 * The bit width is set such that the error rate is acceptable and also allocate
 * little memory. 8 uses 256 bytes and the error rate is 6.5%
 */
#define HLL_BIT_WIDTH 8
#define HLL_ERROR_RATE 0.065
#define HLL_MIN_CARDINALITY 20

/*
 * The UUID compressor is using delta delta compression for the first 8
 * bytes of the UUID and stores the rest as a uint64_vec. At the same time
 * it keeps track of the cardinality of values. if the cardinality
 * indicates that we are better off with the dictionary compressor, we will
 * recompress it at the end.
 */
typedef struct UuidCompressor
{
	/* Delta-delta encoding for timestamp, version and rand_a. */
	DeltaDeltaCompressor *timestamp;

	/* We store the rand_b and variant parts together as a uint64_vec
	 * to avoid having to store two separate bitmaps.
	 */
	uint64_vec rand_b_and_variant;

	/* HLL state to estimate the cardinality. This is used to check if
	 * we are better off with recompressing the data as a dictionary.
	 */
	hyperLogLogState cardinality;

	/* Number of nulls in the data. */
	uint16 num_nulls;

	/* Number of elements in the timestamp part. */
	uint16 num_values;
} UuidCompressor;

typedef struct ExtendedCompressor
{
	Compressor base;
	UuidCompressor *internal;
} ExtendedCompressor;

/*
 * Local helpers
 */
static void uuid_compressor_append_uuid(Compressor *compressor, Datum val);
static void uuid_compressor_append_null_value(Compressor *compressor);
static void *uuid_compressor_finish_and_reset(Compressor *compressor);
static void decompression_iterator_init(UuidDecompressionIterator *iter, void *compressed,
										Oid element_type, bool forward);

const Compressor uuid_compressor_initializer = {
	.append_val = uuid_compressor_append_uuid,
	.append_null = uuid_compressor_append_null_value,
	.is_full = NULL,
	.finish = uuid_compressor_finish_and_reset,
};

/*
 * Compressor framework functions and definitions for the uuid_compress algorithm.
 */

extern UuidCompressor *
uuid_compressor_alloc(void)
{
	UuidCompressor *compressor = palloc0(sizeof(*compressor));
	compressor->timestamp = delta_delta_compressor_alloc();
	uint64_vec_init(&compressor->rand_b_and_variant,
					CurrentMemoryContext,
					TARGET_COMPRESSED_BATCH_SIZE);
	initHyperLogLog(&compressor->cardinality, HLL_BIT_WIDTH);
	return compressor;
}

extern void
uuid_compressor_append_null(UuidCompressor *compressor)
{
	delta_delta_compressor_append_null(compressor->timestamp);
	compressor->num_nulls++;
}

#ifdef TS_USE_UMASH
static inline uint32
uuid_compress_hash(pg_uuid_t *uuid)
{
	static struct umash_params params = { 0 };
	if (params.poly[0][0] == 0)
	{
		umash_params_derive(&params, 0x12345abcdef67890ULL, NULL);
		Assert(params.poly[0][0] != 0);
	}

	uint64 h = umash_full(&params,
						  /* seed = */ ~0ULL,
						  /* which = */ 0,
						  uuid->data,
						  16);

	return (uint32) (h ^ (h >> 32));
}
#else
static inline uint32
uuid_compress_hash(pg_uuid_t *uuid)
{
	return hash_bytes((unsigned char *) uuid->data, sizeof(*uuid));
}
#endif

extern void
uuid_compressor_append_value(UuidCompressor *compressor, pg_uuid_t next_val)
{
	uint64_t components[2];
	memcpy(components, next_val.data, sizeof(components));

	/* The first component is the timestamp, version and rand_a. */
	uint64_t timestamp = pg_ntoh64(components[0]);
	/* The second part is the rand_b and variant. */
	uint64_t rand_b_and_variant = components[1];

	delta_delta_compressor_append_value(compressor->timestamp, timestamp);
	uint64_vec_append(&compressor->rand_b_and_variant, rand_b_and_variant);

	uint32 h = uuid_compress_hash(&next_val);
	addHyperLogLog(&compressor->cardinality, h);
	compressor->num_values++;
}

static size_t
uuid_compressor_estimate_dictionary_storage(UuidCompressor *compressor,
											size_t nulls_compressed_size)
{
	double cardinality = (double) compressor->rand_b_and_variant.num_elements;
	double cardinality_and_error = cardinality;

	/* Don't use HLL if there are too few elements to estimate the cardinality. */
	if (cardinality > HLL_MIN_CARDINALITY)
	{
		cardinality = estimateHyperLogLog(&compressor->cardinality);
		cardinality_and_error = cardinality * (1.0 - HLL_ERROR_RATE);
	}

	int array_index_bytes = ((int) cardinality_and_error * 5 + 63) / 64 * 8;

	double estimated_dictionary_storage =
		/* 16 bytes per values in dictionary/array/values */
		cardinality_and_error * 16 +
		/* a single RLE block for the sizes in dictionary/array/sizes */
		16 +
		/* no nulls in dictionary/array/nulls */
		0 +
		/* 5 bits on average for the indexes in dictionary/array/indexes */
		array_index_bytes +
		/* storing nulls is the same as in the delta-delta compressor */
		nulls_compressed_size;

	return estimated_dictionary_storage;
}

extern void *
uuid_compressor_finish(UuidCompressor *compressor)
{
	if (compressor == NULL)
		return NULL;

	if (compressor->num_values == 0)
		return NULL;

	size_t nulls_compressed_size = 0;
	size_t timestamp_compressed_size =
		delta_delta_compressor_compressed_size(compressor->timestamp, &nulls_compressed_size);
	size_t estimated_dictionary_storage =
		uuid_compressor_estimate_dictionary_storage(compressor, nulls_compressed_size);
	size_t rand_b_and_variant_compressed_size =
		compressor->rand_b_and_variant.num_elements * sizeof(uint64_t);
	Assert(compressor->rand_b_and_variant.num_elements == compressor->num_values);
	size_t total_compressed_size =
		sizeof(UuidCompressed) + timestamp_compressed_size + rand_b_and_variant_compressed_size;

	/* TODO: this is temporary: to iterate over the delta-delta compressed data
	 * we need to finalize the compression, so even if we knew that the dictionary
	 * compression is better we still need to allocate, finish and memcpy the
	 * entries. This is clearly a waste. To solve this we will need an interface
	 * to iterate over compressed data without finalizing it.
	 */
	char *compressed_data = palloc(total_compressed_size);
	UuidCompressed *compressed = (UuidCompressed *) compressed_data;
	SET_VARSIZE(&compressed->vl_len_, total_compressed_size);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_UUID;
	compressed->num_nulls = compressor->num_nulls;
	Ensure(compressed->num_nulls == compressor->num_nulls,
		   "unexpected number of nulls, it doesn't fit into the header");
	compressed->subtype = UUID_COMPRESS_SUBTYPE_DELTADELTA;
	compressed->timestamp_size = timestamp_compressed_size;
	compressed->rand_b_and_variant_size = rand_b_and_variant_compressed_size;

	compressed_data += sizeof(*compressed);
	char *timestamp_compressed_data = compressed_data;
	compressed_data =
		delta_delta_compressor_finish_into(compressor->timestamp, timestamp_compressed_data);
	/* Make sure delta-delta took exactly the size it said it will */
	Assert(compressed_data - timestamp_compressed_data == (long) timestamp_compressed_size);
	memcpy(compressed_data,
		   compressor->rand_b_and_variant.data,
		   rand_b_and_variant_compressed_size);

	if (total_compressed_size > estimated_dictionary_storage)
	{
		/* Recompress as dictionary */
		DictionaryCompressor *dict_compressor = dictionary_compressor_alloc(UUIDOID);
		DecompressionIterator *iter =
			delta_delta_decompression_iterator_from_datum_forward(PointerGetDatum(
																	  timestamp_compressed_data),
																  INT8OID);
		uint32 value_position = 0;
		for (DecompressResult r = delta_delta_decompression_iterator_try_next_forward(iter);
			 !r.is_done;
			 r = delta_delta_decompression_iterator_try_next_forward(iter))
		{
			if (r.is_null)
			{
				dictionary_compressor_append_null(dict_compressor);
			}
			else
			{
				UuidType uuid_type;
				uuid_type.components[0] = pg_hton64(DatumGetInt64(r.val));
				uuid_type.components[1] = compressor->rand_b_and_variant.data[value_position];
				dictionary_compressor_append(dict_compressor, UUIDPGetDatum(&uuid_type.uuid));
				++value_position;
			}
		}

		void *dict_compressed = dictionary_compressor_finish(dict_compressor);
		if (VARSIZE(dict_compressed) < total_compressed_size)
		{
			/* We are better off with the dictionary compression, inline with the estimated size */
			pfree(compressed);
			compressed = dict_compressed;
		}
		else
		{
			/* We are better off with the original compression, contrary to the estimated size.
			 * This is OK, as the estimate is probabilistic.
			 */
			pfree(dict_compressed);
		}
		pfree(dict_compressor);
		pfree(iter);
	}

	return compressed;
}

extern bool
uuid_compressed_has_nulls(const CompressedDataHeader *header)
{
	const UuidCompressed *uc = (const UuidCompressed *) header;
	return uc->num_nulls > 0;
}

extern DecompressResult
uuid_decompression_iterator_try_next_forward(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_UUID && iter->forward);
	Assert(iter->element_type == UUIDOID);

	UuidDecompressionIterator *uuid_iter = (UuidDecompressionIterator *) iter;

	Assert(uuid_iter->uuid_buffer != NULL);
	Assert(uuid_iter->uuid_buffer->buffers != NULL);
	Assert(uuid_iter->uuid_buffer->buffers[1] != NULL);

	const uint64 *validity_bitmap = uuid_iter->uuid_buffer->buffers[0];
	UuidType *uuid_values = (UuidType *) uuid_iter->uuid_buffer->buffers[1];

	if (uuid_iter->position >= uuid_iter->total_elements)
		return (DecompressResult){
			.is_done = true,
		};

	/* check nulls */
	if (validity_bitmap != NULL)
	{
		if (!arrow_row_is_valid(validity_bitmap, uuid_iter->position))
		{
			uuid_iter->position++;
			return (DecompressResult){
				.is_null = true,
			};
		}
	}

	UuidType *current_uuid = &uuid_values[uuid_iter->position];
	uuid_iter->position++;

	return (DecompressResult){
		.val = PointerGetDatum(current_uuid),
	};
}

extern DecompressionIterator *
uuid_decompression_iterator_from_datum_forward(Datum uuid_compressed, Oid element_type)
{
	UuidDecompressionIterator *iterator = palloc0(sizeof(*iterator));
	decompression_iterator_init(iterator,
								(void *) PG_DETOAST_DATUM(uuid_compressed),
								element_type,
								true);
	return &iterator->base;
}

extern DecompressResult
uuid_decompression_iterator_try_next_reverse(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_UUID && !iter->forward);
	Assert(iter->element_type == UUIDOID);

	UuidDecompressionIterator *uuid_iter = (UuidDecompressionIterator *) iter;

	Assert(uuid_iter->uuid_buffer != NULL);
	Assert(uuid_iter->uuid_buffer->buffers != NULL);
	Assert(uuid_iter->uuid_buffer->buffers[1] != NULL);

	const uint64 *validity_bitmap = uuid_iter->uuid_buffer->buffers[0];
	UuidType *uuid_values = (UuidType *) uuid_iter->uuid_buffer->buffers[1];

	if (uuid_iter->position < 0)
		return (DecompressResult){
			.is_done = true,
		};

	/* check nulls */
	if (validity_bitmap != NULL)
	{
		if (!arrow_row_is_valid(validity_bitmap, uuid_iter->position))
		{
			uuid_iter->position--;
			return (DecompressResult){
				.is_null = true,
			};
		}
	}

	Assert(uuid_iter->position >= 0);
	UuidType *current_uuid = &uuid_values[uuid_iter->position];
	uuid_iter->position--;

	return (DecompressResult){
		.val = PointerGetDatum(current_uuid),
	};
}

extern DecompressionIterator *
uuid_decompression_iterator_from_datum_reverse(Datum uuid_compressed, Oid element_type)
{
	UuidDecompressionIterator *iterator = palloc(sizeof(*iterator));
	decompression_iterator_init(iterator,
								(void *) PG_DETOAST_DATUM(uuid_compressed),
								element_type,
								false);
	return &iterator->base;
}

extern void
uuid_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const UuidCompressed *data = (UuidCompressed *) header;
	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_UUID);

	pq_sendbyte(buffer, data->subtype);
	pq_sendint16(buffer, data->num_nulls);
	pq_sendint32(buffer, data->timestamp_size);
	pq_sendint32(buffer, data->rand_b_and_variant_size);

	char *ptr = (char *) data->alignment_sentinel;
	deltadelta_compressed_send((CompressedDataHeader *) ptr, buffer);
	ptr += data->timestamp_size;
	uint64 *rand_b_and_variant = (uint64 *) ptr;
	uint32 num_elements = data->rand_b_and_variant_size / sizeof(uint64);
	for (uint32 i = 0; i < num_elements; i++)
		pq_sendint64(buffer, rand_b_and_variant[i]);
}

extern Datum
uuid_compressed_recv(StringInfo buffer)
{
	size_t total_compressed_sized = 0;
	uint8 subtype = pq_getmsgbyte(buffer);
	CheckCompressedData(subtype < UUID_COMPRESS_SUBTYPE_COUNT);
	uint16 num_nulls = pq_getmsgint(buffer, 2);
	uint32 timestamp_size = pq_getmsgint32(buffer);
	uint32 rand_b_and_variant_size = pq_getmsgint32(buffer);

	total_compressed_sized = sizeof(UuidCompressed) + timestamp_size + rand_b_and_variant_size;
	CheckCompressedData(total_compressed_sized <= MaxAllocSize);

	char *result = palloc(total_compressed_sized);
	UuidCompressed *compressed = (UuidCompressed *) result;
	compressed->subtype = subtype;
	compressed->num_nulls = num_nulls;
	compressed->timestamp_size = timestamp_size;
	compressed->rand_b_and_variant_size = rand_b_and_variant_size;
	SET_VARSIZE(&compressed->vl_len_, total_compressed_sized);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_UUID;

	Datum delta_delta_compressed = deltadelta_compressed_recv(buffer);
	size_t delta_delta_compressed_size = VARSIZE(delta_delta_compressed);
	CheckCompressedData(delta_delta_compressed_size == timestamp_size);

	memcpy(result + sizeof(UuidCompressed),
		   DatumGetPointer(delta_delta_compressed),
		   delta_delta_compressed_size);
	uint64 *rand_b_and_variant =
		(uint64 *) (result + sizeof(UuidCompressed) + delta_delta_compressed_size);
	uint32 num_elements = rand_b_and_variant_size / sizeof(uint64);
	for (uint32 i = 0; i < num_elements; i++)
		rand_b_and_variant[i] = pq_getmsgint64(buffer);

	PG_RETURN_POINTER(result);
}

extern Compressor *
uuid_compressor_for_type(Oid element_type)
{
	ExtendedCompressor *compressor = palloc(sizeof(*compressor));
	switch (element_type)
	{
		case UUIDOID:
			*compressor = (ExtendedCompressor){ .base = uuid_compressor_initializer };
			return &compressor->base;
		default:
			elog(ERROR, "invalid type for uuid compressor \"%s\"", format_type_be(element_type));
	}

	pg_unreachable();
}

/*
 * Cross-module functions for the uuid_compress algorithm.
 */
extern Datum
tsl_uuid_compressor_append(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	MemoryContext agg_context;
	UuidCompressor *compressor = (UuidCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_uuid_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		compressor = uuid_compressor_alloc();
		if (PG_NARGS() > 2)
			elog(ERROR, "append expects two arguments");
	}

	if (PG_ARGISNULL(1))
		uuid_compressor_append_null(compressor);
	else
	{
		pg_uuid_t *uuid = DatumGetUUIDP(PG_GETARG_DATUM(1));
		Ensure(uuid != NULL, "invalid UUID");
		uuid_compressor_append_value(compressor, *uuid);
	}

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

extern Datum
tsl_uuid_compressor_finish(PG_FUNCTION_ARGS)
{
	UuidCompressor *compressor = PG_ARGISNULL(0) ? NULL : (UuidCompressor *) PG_GETARG_POINTER(0);
	void *compressed;
	if (compressor == NULL)
		PG_RETURN_NULL();

	compressed = uuid_compressor_finish(compressor);
	if (compressed == NULL)
		PG_RETURN_NULL();
	PG_RETURN_POINTER(compressed);
}

extern ArrowArray *
uuid_decompress_all(Datum compressed, Oid element_type, MemoryContext dest_mctx)
{
	Assert(element_type == UUIDOID);

	MemoryContext old_context;
	ArrowArray *timestamp_array = NULL;

	void *detoasted = PG_DETOAST_DATUM(compressed);
	StringInfoData si = { .data = detoasted, .len = VARSIZE(compressed) };
	UuidCompressed *header = consumeCompressedData(&si, sizeof(UuidCompressed));
	char *timestamp_compressed_data = NULL;
	char *rand_b_and_variant_compressed_data;

	CheckCompressedData(header->compression_algorithm == COMPRESSION_ALGORITHM_UUID);
	CheckCompressedData(header->subtype == UUID_COMPRESS_SUBTYPE_DELTADELTA);
	CheckCompressedData(header->timestamp_size > 0);
	CheckCompressedData(header->rand_b_and_variant_size > 0);

	timestamp_compressed_data = consumeCompressedData(&si, header->timestamp_size);
	rand_b_and_variant_compressed_data =
		consumeCompressedData(&si, header->rand_b_and_variant_size);

	int32 num_values = (int32) (header->rand_b_and_variant_size / sizeof(uint64));
	int32 total_elements = (int32) header->num_nulls + num_values;
	CheckCompressedData(num_values > 0);

	old_context = MemoryContextSwitchTo(dest_mctx);

	/* Make sure we use a 128bit aligned buffer */
	char *unaligned = (char *) palloc((total_elements * sizeof(UuidType)) + 15);
	UuidType *uuid_buffer = (UuidType *) TYPEALIGN(16, unaligned);

	/*
	 * Note : the validity bits from the delta delta compression is directly usable
	 *        in the result array, but the decompressed ingegers will be replaced
	 *        with the UUIDs
	 */
	timestamp_array = delta_delta_decompress_all(PointerGetDatum(timestamp_compressed_data),
												 INT8OID,
												 CurrentMemoryContext);

	CheckCompressedData(timestamp_array->length == total_elements);
	CheckCompressedData(timestamp_array->null_count == header->num_nulls);

	uint64 *rand_b_and_variant = (uint64 *) palloc(header->rand_b_and_variant_size);
	memcpy(rand_b_and_variant, rand_b_and_variant_compressed_data, header->rand_b_and_variant_size);

	uint64 *validity_bitmap = NULL;
	uint64 *timestamp_values = (uint64 *) timestamp_array->buffers[1];

	if (header->num_nulls > 0)
	{
		Assert(timestamp_array->buffers[0] != NULL);
		validity_bitmap = (uint64 *) timestamp_array->buffers[0];

		int value_position = 0;
		for (int i = 0; i < total_elements; i++)
		{
			if (arrow_row_is_valid(validity_bitmap, i))
			{
				Assert(value_position < num_values);
				uuid_buffer[i].components[0] = pg_ntoh64(timestamp_values[i]);
				uuid_buffer[i].components[1] = rand_b_and_variant[value_position];
				++value_position;
			}
		}
	}
	else
	{
		for (int i = 0; i < total_elements; i++)
		{
			uuid_buffer[i].components[0] = pg_ntoh64(timestamp_values[i]);
			uuid_buffer[i].components[1] = rand_b_and_variant[i];
		}
	}

	/*
	 * At this point I combined the uncompressed data in the rand_b_and_variant array
	 * and the freshly decompressed data from the delta delta bulk decompressison.
	 * I now free the temp data. Note that `timestamp_values` is the same pointer
	 * as timestamp_array->buffers[1] , the second buffer in the ArrowArray.
	 * This will be replaced with the uuid array.
	 * */
	pfree(rand_b_and_variant);
	pfree(timestamp_values);
	MemoryContextSwitchTo(old_context);

	/*
	 * This is the only fixup needed for the ArrowArray. Everything else
	 * is valid data as generated by delta_delta_decompress_all()
	 */
	timestamp_array->buffers[1] = (uint8 *) uuid_buffer;
	return timestamp_array;
}

/*
 * Local helpers
 */
static void
uuid_compressor_append_uuid(Compressor *compressor, Datum val)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = uuid_compressor_alloc();

	pg_uuid_t *uuid = DatumGetUUIDP(val);
	Ensure(uuid != NULL, "invalid UUID");
	uuid_compressor_append_value(extended->internal, *uuid);
}

static void
uuid_compressor_append_null_value(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	if (extended->internal == NULL)
		extended->internal = uuid_compressor_alloc();

	uuid_compressor_append_null(extended->internal);
}

static void *
uuid_compressor_finish_and_reset(Compressor *compressor)
{
	ExtendedCompressor *extended = (ExtendedCompressor *) compressor;
	void *compressed = NULL;
	if (extended != NULL && extended->internal != NULL)
	{
		compressed = uuid_compressor_finish(extended->internal);
		pfree(extended->internal);
		extended->internal = NULL;
	}
	return compressed;
}

static void
decompression_iterator_init(UuidDecompressionIterator *iter, void *compressed, Oid element_type,
							bool forward)
{
	Assert(element_type == UUIDOID);

	ArrowArray *arrow_array =
		uuid_decompress_all(PointerGetDatum(compressed), element_type, CurrentMemoryContext);
	int32 total_elements = arrow_array->length;

	*iter = (UuidDecompressionIterator){
		.base = { .compression_algorithm = COMPRESSION_ALGORITHM_UUID,
				  .forward = forward,
				  .element_type = element_type,
				  .try_next = (forward ? uuid_decompression_iterator_try_next_forward :
										 uuid_decompression_iterator_try_next_reverse) },
		.position = (forward ? 0 : total_elements - 1),
		.total_elements = total_elements,
		.uuid_buffer = arrow_array,
	};
}

static void
pg_attribute_unused() silence_unused_warning(void)
{
	simple8brle_serialized_recv(NULL);
}
