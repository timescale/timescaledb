/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/deltadelta.h"

#include <access/htup_details.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <funcapi.h>
#include <lib/stringinfo.h>

#include <base64_compat.h>

#include "compression/compression.h"
#include "compression/simple8b_rle.h"

static uint64 zig_zag_encode(uint64 value);
static uint64 zig_zag_decode(uint64 value);

typedef struct DeltaDeltaCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls; /* 1 if this has a NULLs bitmap after deltas, 0 otherwise */
	uint8 padding[2];
	uint64 first_value;
	uint64 last_value;
	uint64 last_delta;
	Simple8bRleSerialized delta_deltas;
} DeltaDeltaCompressed;

static void
pg_attribute_unused() assertions(void)
{
	DeltaDeltaCompressed test_val = {};
	/* make sure no padding bytes make it to disk */
	StaticAssertStmt(sizeof(DeltaDeltaCompressed) ==
						 sizeof(test_val.vl_len_) + sizeof(test_val.compression_algorithm) +
							 sizeof(test_val.has_nulls) + sizeof(test_val.padding) +
							 sizeof(test_val.first_value) + sizeof(test_val.last_value) +
							 sizeof(test_val.last_delta) + sizeof(test_val.delta_deltas),
					 "DeltaDeltaCompressed wrong size");
	StaticAssertStmt(sizeof(DeltaDeltaCompressed) == 40, "DeltaDeltaCompressed wrong size");
}

typedef struct DeltaDeltaDecompressionIterator
{
	DecompressionIterator base;
	uint64 prev_val;
	uint64 prev_delta;
	Simple8bRleDecompressionIterator delta_deltas;
	Simple8bRleDecompressionIterator nulls;
	bool returned_first;
	bool has_nulls;
} DeltaDeltaDecompressionIterator;

typedef struct DeltaDeltaCompressor
{
	bool start_is_set;
	uint64 start;
	uint64 prev_val;
	uint64 prev_delta;
	Simple8bRleCompressor delta_delta;
	Simple8bRleCompressor nulls;
	bool has_nulls;
} DeltaDeltaCompressor;

Datum
tsl_deltadelta_compressor_append(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	MemoryContext agg_context;
	DeltaDeltaCompressor *compressor =
		(DeltaDeltaCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_deltadelta_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		compressor = delta_delta_compressor_alloc();
		if (PG_NARGS() > 2)
			elog(ERROR, "append expects two arguments");
	}

	if (PG_ARGISNULL(1))
		delta_delta_compressor_append_null(compressor);
	else
	{
		int64 next_val = PG_GETARG_INT64(1);
		delta_delta_compressor_append_value(compressor, next_val);
	}

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

DeltaDeltaCompressor *
delta_delta_compressor_alloc(void)
{
	DeltaDeltaCompressor *compressor = palloc0(sizeof(*compressor));
	simple8brle_compressor_init(&compressor->delta_delta);
	simple8brle_compressor_init(&compressor->nulls);
	return compressor;
}

static DeltaDeltaCompressed *
delta_delta_from_parts(uint64 first_value, uint64 last_value, uint64 last_delta,
					   Simple8bRleSerialized *deltas, Simple8bRleSerialized *nulls)
{
	uint32 nulls_size = 0;
	Size compressed_size;
	char *compressed_data;
	DeltaDeltaCompressed *compressed;
	if (nulls != NULL)
		nulls_size = simple8brle_serialized_total_size(nulls);
	compressed_size =
		sizeof(DeltaDeltaCompressed) + simple8brle_serialized_slot_size(deltas) + nulls_size;

	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	compressed_data = palloc(compressed_size);
	compressed = (DeltaDeltaCompressed *) compressed_data;
	SET_VARSIZE(&compressed->vl_len_, compressed_size);

	compressed->compression_algorithm = COMPRESSION_ALGORITHM_DELTADELTA;
	compressed->first_value = first_value;
	compressed->last_value = last_value;
	compressed->last_delta = last_delta;
	compressed->has_nulls = nulls_size != 0 ? 1 : 0;

	compressed_data = (char *) &compressed->delta_deltas;
	compressed_data =
		bytes_serialize_simple8b_and_advance(compressed_data,
											 simple8brle_serialized_total_size(deltas),
											 deltas);
	if (compressed->has_nulls)
	{
		Assert(nulls->num_elements > deltas->num_elements);
		bytes_serialize_simple8b_and_advance(compressed_data, nulls_size, nulls);
	}

	return compressed;
}

Datum
tsl_deltadelta_compressor_finish(PG_FUNCTION_ARGS)
{
	DeltaDeltaCompressor *compressor = (DeltaDeltaCompressor *) PG_GETARG_POINTER(0);

	Simple8bRleSerialized *deltas = simple8brle_compressor_finish(&compressor->delta_delta);
	Simple8bRleSerialized *nulls = simple8brle_compressor_finish(&compressor->nulls);
	DeltaDeltaCompressed *compressed = delta_delta_from_parts(compressor->start,
															  compressor->prev_val,
															  compressor->prev_delta,
															  deltas,
															  compressor->has_nulls ? nulls : NULL);

	Assert(compressed->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA);
	PG_RETURN_POINTER(compressed);
}

void
delta_delta_compressor_append_null(DeltaDeltaCompressor *compressor)
{
	compressor->has_nulls = true;
	simple8brle_compressor_append(&compressor->nulls, 1);
}

void
delta_delta_compressor_append_value(DeltaDeltaCompressor *compressor, int64 next_val)
{
	uint64 delta;
	uint64 delta_delta;
	uint64 encoded;

	/* step 0: the first value seen is the start val */
	if (!compressor->start_is_set)
	{
		compressor->start_is_set = true;
		compressor->start = next_val;
		compressor->prev_val = next_val;
		compressor->prev_delta = 0;

		simple8brle_compressor_append(&compressor->nulls, 0);
		return;
	}

	/*
	 * We perform all arithmetic using unsigned values due to C's overflow rules:
	 * signed integer overflow is undefined behavior, so if we have a very large delta,
	 * this code is without meaning, while unsigned overflow is 2's complement, so even
	 * very large delta work the same as any other
	 */

	/* step 1: delta of deltas */
	delta = ((uint64) next_val) - compressor->prev_val;
	delta_delta = delta - compressor->prev_delta;

	compressor->prev_val = next_val;
	compressor->prev_delta = delta;

	/* step 2: ZigZag encode */
	encoded = zig_zag_encode(delta_delta);

	/* step 3: simple8b/RTE */
	simple8brle_compressor_append(&compressor->delta_delta, encoded);
	simple8brle_compressor_append(&compressor->nulls, 0);
}

/**********************************************************************************/
/**********************************************************************************/

static void
int64_decompression_iterator_init_forward(DeltaDeltaDecompressionIterator *iter,
										  DeltaDeltaCompressed *compressed, Oid element_type)
{
	const char *data = (char *) &compressed->delta_deltas;
	Simple8bRleSerialized *deltas = bytes_deserialize_simple8b_and_advance(&data);
	Simple8bRleSerialized *nulls = NULL;
	bool has_nulls = compressed->has_nulls == 1;

	if (has_nulls)
		nulls = bytes_deserialize_simple8b_and_advance(&data);
	else
		Assert(compressed->has_nulls == 0);

	*iter = (DeltaDeltaDecompressionIterator){
		.base = {
			.compression_algorithm = COMPRESSION_ALGORITHM_DELTADELTA,
			.forward = true,
			.element_type = element_type,
			.try_next = delta_delta_decompression_iterator_try_next_forward,
		},
		.returned_first = false,
		.prev_val = compressed->first_value,
		.prev_delta = 0,
		.has_nulls = has_nulls,
	};

	simple8brle_decompression_iterator_init_forward(&iter->delta_deltas, deltas);

	if (has_nulls)
		simple8brle_decompression_iterator_init_forward(&iter->nulls, nulls);
}

static void
int64_decompression_iterator_init_reverse(DeltaDeltaDecompressionIterator *iter,
										  DeltaDeltaCompressed *compressed, Oid element_type)
{
	const char *data = (char *) &compressed->delta_deltas;
	Simple8bRleSerialized *deltas = bytes_deserialize_simple8b_and_advance(&data);
	Simple8bRleSerialized *nulls = NULL;
	bool has_nulls = compressed->has_nulls == 1;

	if (has_nulls)
		nulls = bytes_deserialize_simple8b_and_advance(&data);
	else
		Assert(compressed->has_nulls == 0);

	*iter = (DeltaDeltaDecompressionIterator){
		.base = {
			.compression_algorithm = COMPRESSION_ALGORITHM_DELTADELTA,
			.forward = false,
			.element_type = element_type,
			.try_next = delta_delta_decompression_iterator_try_next_reverse,
		},
		.returned_first = false,
		.prev_val = compressed->last_value,
		.prev_delta = compressed->last_delta,
		.has_nulls = has_nulls,
	};

	simple8brle_decompression_iterator_init_reverse(&iter->delta_deltas, deltas);

	if (has_nulls == 1)
		simple8brle_decompression_iterator_init_reverse(&iter->nulls, nulls);
}

static inline DecompressResult
convert_from_internal(DecompressResultInternal res_internal, Oid element_type)
{
	if (res_internal.is_done || res_internal.is_null)
	{
		return (DecompressResult){
			.is_done = res_internal.is_done,
			.is_null = res_internal.is_null,
		};
	}

	switch (element_type)
	{
		case INT8OID:
			return (DecompressResult){
				.val = Int64GetDatum(res_internal.val),
			};
		case INT4OID:
			return (DecompressResult){
				.val = Int32GetDatum(res_internal.val),
			};
		case INT2OID:
			return (DecompressResult){
				.val = Int16GetDatum(res_internal.val),
			};
#ifdef HAVE_INT64_TIMESTAMP
		case TIMESTAMPTZOID:
			return (DecompressResult){
				.val = TimestampTzGetDatum(res_internal.val),
			};
		case TIMESTAMPOID:
			return (DecompressResult){
				.val = TimestampGetDatum(res_internal.val),
			};
#endif
		default:
			elog(ERROR, "invalid type requested from deltadelta decompression");
	}
}

static DecompressResultInternal
delta_delta_decompression_iterator_try_next_forward_internal(DeltaDeltaDecompressionIterator *iter)
{
	Simple8bRleDecompressResult result;
	uint64 delta_delta;

	/* check for a null value */
	if (iter->has_nulls)
	{
		Simple8bRleDecompressResult result =
			simple8brle_decompression_iterator_try_next_forward(&iter->nulls);
		if (result.is_done)
			return (DecompressResultInternal){
				.is_done = true,
			};

		if (result.val != 0)
		{
			Assert(result.val == 1);
			return (DecompressResultInternal){
				.is_null = true,
			};
		}
	}

	if (!iter->returned_first)
	{
		iter->returned_first = true;
		return (DecompressResultInternal){
			.val = iter->prev_val,
			.is_null = false,
			.is_done = false,
		};
	}

	result = simple8brle_decompression_iterator_try_next_forward(&iter->delta_deltas);

	if (result.is_done)
		return (DecompressResultInternal){
			.is_done = true,
		};

	delta_delta = zig_zag_decode(result.val);

	iter->prev_delta += delta_delta;
	iter->prev_val += iter->prev_delta;

	return (DecompressResultInternal){
		.val = iter->prev_val,
		.is_null = false,
		.is_done = false,
	};
}

DecompressResult
delta_delta_decompression_iterator_try_next_forward(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA && iter->forward);
	return convert_from_internal(delta_delta_decompression_iterator_try_next_forward_internal(
									 (DeltaDeltaDecompressionIterator *) iter),
								 iter->element_type);
}

static DecompressResultInternal
delta_delta_decompression_iterator_try_next_reverse_internal(DeltaDeltaDecompressionIterator *iter)
{
	Simple8bRleDecompressResult result;
	uint64 val;
	uint64 delta_delta;
	/* check for a null value */
	if (iter->has_nulls)
	{
		Simple8bRleDecompressResult result =
			simple8brle_decompression_iterator_try_next_reverse(&iter->nulls);
		if (result.is_done)
			return (DecompressResultInternal){
				.is_done = true,
			};

		if (result.val != 0)
		{
			Assert(result.val == 1);
			return (DecompressResultInternal){
				.is_null = true,
			};
		}
	}

	result = simple8brle_decompression_iterator_try_next_reverse(&iter->delta_deltas);

	if (result.is_done)
	{
		if (iter->returned_first)
			return (DecompressResultInternal){
				.is_done = true,
			};
		iter->returned_first = true;
		return (DecompressResultInternal){
			.val = iter->prev_val,
		};
	}

	val = iter->prev_val;

	delta_delta = zig_zag_decode(result.val);
	iter->prev_val -= iter->prev_delta;
	iter->prev_delta -= delta_delta;

	return (DecompressResultInternal){
		.val = val,
	};
}

DecompressResult
delta_delta_decompression_iterator_try_next_reverse(DecompressionIterator *iter)
{
	Assert(iter->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA && !iter->forward);
	return convert_from_internal(delta_delta_decompression_iterator_try_next_reverse_internal(
									 (DeltaDeltaDecompressionIterator *) iter),
								 iter->element_type);
}

DecompressionIterator *
delta_delta_decompression_iterator_from_datum_forward(Datum deltadelta_compressed, Oid element_type)
{
	DeltaDeltaDecompressionIterator *iterator = palloc(sizeof(*iterator));
	int64_decompression_iterator_init_forward(iterator,
											  (void *) PG_DETOAST_DATUM(deltadelta_compressed),
											  element_type);
	return &iterator->base;
}

DecompressionIterator *
delta_delta_decompression_iterator_from_datum_reverse(Datum deltadelta_compressed, Oid element_type)
{
	DeltaDeltaDecompressionIterator *iterator = palloc(sizeof(*iterator));
	int64_decompression_iterator_init_reverse(iterator,
											  (void *) PG_DETOAST_DATUM(deltadelta_compressed),
											  element_type);
	return &iterator->base;
}

/**********************************************************************************/
/**********************************************************************************/
void
deltadelta_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const DeltaDeltaCompressed *data = (DeltaDeltaCompressed *) header;
	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA);
	pq_sendbyte(buffer, data->has_nulls);
	pq_sendint64(buffer, data->first_value);
	pq_sendint64(buffer, data->last_value);
	pq_sendint64(buffer, data->last_delta);
	simple8brle_serialized_send(buffer, &data->delta_deltas);
	if (data->has_nulls)
	{
		Simple8bRleSerialized *nulls =
			(Simple8bRleSerialized *) (((char *) &data->delta_deltas) +
									   simple8brle_serialized_total_size(&data->delta_deltas));
		simple8brle_serialized_send(buffer, nulls);
	}
}
Datum
deltadelta_compressed_recv(StringInfo buffer)
{
	uint8 has_nulls;
	uint64 first_value;
	uint64 last_value;
	uint64 last_delta;
	Simple8bRleSerialized *delta_deltas;
	Simple8bRleSerialized *nulls = NULL;
	DeltaDeltaCompressed *compressed;

	has_nulls = pq_getmsgbyte(buffer);
	if (has_nulls != 0 && has_nulls != 1)
		elog(ERROR, "invalid recv in deltadelta: bad bool");

	first_value = pq_getmsgint64(buffer);
	last_value = pq_getmsgint64(buffer);
	last_delta = pq_getmsgint64(buffer);
	delta_deltas = simple8brle_serialized_recv(buffer);
	if (has_nulls)
		nulls = simple8brle_serialized_recv(buffer);

	compressed = delta_delta_from_parts(first_value, last_value, last_delta, delta_deltas, nulls);

	PG_RETURN_POINTER(compressed);
}

/**********************************************************************************/
/**********************************************************************************/

static inline uint64
zig_zag_encode(uint64 value)
{
	// (((uint64)value) << 1) ^ (uint64)(value >> 63);
	/* since shift is underspecified, we use (value < 0 ? 0xFFFFFFFFFFFFFFFFull : 0)
	 * which compiles to the correct asm, and is well defined
	 */
	return (value << 1) ^ (((int64) value) < 0 ? 0xFFFFFFFFFFFFFFFFull : 0);
}

static inline uint64
zig_zag_decode(uint64 value)
{
	/* ZigZag turns negative numbers into odd ones, and positive numbers into even ones*/
	return (value >> 1) ^ (uint64) - (int64)(value & 1);
}
