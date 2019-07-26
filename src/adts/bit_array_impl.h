/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ADT_BITARRAY_IMPL_H
#define TIMESCALEDB_ADT_BITARRAY_IMPL_H

#include <postgres.h>

#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include "adts/uint64_vec.h"
#include "compat.h"

#define BITS_PER_BUCKET 64

typedef struct BitArray
{
	uint64_vec buckets;
	uint8 bits_used_in_last_bucket;
} BitArray;

typedef struct BitArrayIterator
{
	const BitArray *array;
	uint8 bits_used_in_current_bucket;
	/* note that current_bucket should be signed since it sometimes gets decremented and must be
	 * able to hold UINT32_MAX */
	int64 current_bucket;
} BitArrayIterator;

/************************
 ***  Private Helpers ***
 ************************/

static void bit_array_append_bucket(BitArray *array, uint8 bits_used, uint64 bucket);
static uint64 bit_array_low_bits_mask(uint8 bits_used);

static inline void bit_array_wrap_internal(BitArray *array, uint32 num_buckets,
										   uint8 bits_used_in_last_bucket, uint64 *buckets);

/************************
 ***  Implementation ***
 ************************/

static inline void
bit_array_init(BitArray *array)
{
	*array = (BitArray){
		.bits_used_in_last_bucket = 0,
	};
	uint64_vec_init(&array->buckets, CurrentMemoryContext, 0);
}

/* This initializes the bit array by wrapping buckets. Note, that the bit array will
 * point to buckets instead of creating a new copy. */
static inline void
bit_array_wrap_internal(BitArray *array, uint32 num_buckets, uint8 bits_used_in_last_bucket,
						uint64 *buckets)
{
	*array = (BitArray){
		.bits_used_in_last_bucket = bits_used_in_last_bucket,
		.buckets =
			(uint64_vec){
				.data = buckets,
				.num_elements = num_buckets,
				.max_elements = num_buckets,
			},
	};
}

static inline size_t
bit_array_data_bytes_used(const BitArray *array)
{
	return array->buckets.num_elements * sizeof(*array->buckets.data);
}

static inline uint32
bit_array_num_buckets(const BitArray *array)
{
	return array->buckets.num_elements;
}

static inline uint64
bit_array_num_bits(const BitArray *array)
{
	return (BITS_PER_BUCKET * (array->buckets.num_elements - 1)) + array->bits_used_in_last_bucket;
}

static inline uint64 *
bit_array_buckets(const BitArray *array)
{
	return array->buckets.data;
}

static inline BitArray
bit_array_recv(const StringInfo buffer)
{
	uint32 i;
	uint32 num_elements = pq_getmsgint32(buffer);
	uint8 bits_used_in_last_bucket = pq_getmsgbyte(buffer);
	BitArray array;
	if (num_elements >= PG_UINT32_MAX / sizeof(uint64))
		elog(ERROR, "invalid number of elements in bit array");

	if (bits_used_in_last_bucket > BITS_PER_BUCKET)
		elog(ERROR, "invalid number of bits in last bucket of bit array");

	array = (BitArray){
		.bits_used_in_last_bucket = bits_used_in_last_bucket,
		.buckets = {
			.num_elements = num_elements,
			.max_elements = num_elements,
			.ctx = CurrentMemoryContext,
			.data = palloc0(num_elements * sizeof(uint64)),
		},
	};

	for (i = 0; i < num_elements; i++)
		array.buckets.data[i] = pq_getmsgint64(buffer);

	return array;
}

static inline void
bit_array_send(StringInfo buffer, const BitArray *data)
{
	int i;
	pq_sendint32(buffer, data->buckets.num_elements);
	pq_sendbyte(buffer, data->bits_used_in_last_bucket);
	for (i = 0; i < data->buckets.num_elements; i++)
		pq_sendint64(buffer, data->buckets.data[i]);
}

static inline size_t
bit_array_output(const BitArray *array, uint64 *dst, size_t max_n_bytes, uint64 *num_bits_out)
{
	size_t size = bit_array_data_bytes_used(array);

	if (max_n_bytes < size)
		elog(ERROR, "not enough memory to serialize bit array");

	if (num_bits_out != NULL)
		*num_bits_out = bit_array_num_bits(array);

	memcpy(dst, array->buckets.data, size);
	return size;
}

static inline char *
bytes_store_bit_array_and_advance(char *dest, size_t expected_size, const BitArray *array,
								  uint32 *num_buckets_out, uint8 *bits_in_last_bucket_out)
{
	size_t size = bit_array_data_bytes_used(array);

	if (expected_size != size)
		elog(ERROR, "the size to serialize does not match the  bit array");

	*num_buckets_out = bit_array_num_buckets(array);
	*bits_in_last_bucket_out = array->bits_used_in_last_bucket;

	if (size > 0)
	{
		Assert(array->buckets.data != NULL);
		memcpy(dest, array->buckets.data, size);
	}
	return dest + size;
}

static inline void
bit_array_wrap(BitArray *dst, uint64 *data, uint64 num_bits)
{
	uint32 num_buckets = num_bits / BITS_PER_BUCKET;
	uint8 bits_used_in_last_bucket = num_bits % BITS_PER_BUCKET;
	if (bits_used_in_last_bucket == 0)
	{
		/* last bucket uses all bits */
		if (num_buckets > 0)
			bits_used_in_last_bucket = BITS_PER_BUCKET;
	}
	else
		num_buckets += 1;
	bit_array_wrap_internal(dst, num_buckets, bits_used_in_last_bucket, data);
}

static inline const char *
bytes_attach_bit_array_and_advance(BitArray *dst, const char *data, uint32 num_buckets,
								   uint8 bits_in_last_bucket)
{
	bit_array_wrap_internal(dst, num_buckets, bits_in_last_bucket, (uint64 *) data);
	return data + bit_array_data_bytes_used(dst);
}

static inline void
bit_array_append(BitArray *array, uint8 num_bits, uint64 bits)
{
	/* Fill bits from LSB to MSB */
	uint8 bits_remaining_in_last_bucket;
	uint8 num_bits_for_new_bucket;
	uint64 bits_for_new_bucket;
	Assert(num_bits <= 64);
	if (num_bits == 0)
		return;

	if (array->buckets.num_elements == 0)
		bit_array_append_bucket(array, 0, 0);

	bits &= bit_array_low_bits_mask(num_bits);

	bits_remaining_in_last_bucket = 64 - array->bits_used_in_last_bucket;
	if (bits_remaining_in_last_bucket >= num_bits)
	{
		uint64 *bucket = uint64_vec_last(&array->buckets);
		/* mask out any unused high bits, probably unneeded */
		*bucket |= bits << array->bits_used_in_last_bucket;
		array->bits_used_in_last_bucket += num_bits;
		return;
	}

	/* When splitting an interger across buckets, the low-order bits go into the first bucket and
	 * the high-order bits go into the second bucket  */
	num_bits_for_new_bucket = num_bits - bits_remaining_in_last_bucket;
	if (bits_remaining_in_last_bucket > 0)
	{
		uint64 bits_for_current_bucket =
			bits & bit_array_low_bits_mask(bits_remaining_in_last_bucket);
		uint64 *current_bucket = uint64_vec_last(&array->buckets);
		*current_bucket |= bits_for_current_bucket << array->bits_used_in_last_bucket;
		bits >>= bits_remaining_in_last_bucket;
	}

	/* We zero out the high bits of the new bucket, to ensure that unused bits are always 0 */
	bits_for_new_bucket = bits & bit_array_low_bits_mask(num_bits_for_new_bucket);
	bit_array_append_bucket(array, num_bits_for_new_bucket, bits_for_new_bucket);
}

static inline void
bit_array_iterator_init(BitArrayIterator *iter, const BitArray *array)
{
	*iter = (BitArrayIterator){
		.array = array,
	};
}

static inline uint64
bit_array_iter_next(BitArrayIterator *iter, uint8 num_bits)
{
	uint8 bits_remaining_in_current_bucket;
	uint8 num_bits_from_next_bucket;
	uint64 value = 0;
	uint64 value_from_next_bucket;
	Assert(num_bits <= 64);
	if (num_bits == 0)
		return 0;

	bits_remaining_in_current_bucket = 64 - iter->bits_used_in_current_bucket;
	if (bits_remaining_in_current_bucket >= num_bits)
	{
		value = *uint64_vec_get(&iter->array->buckets, iter->current_bucket);
		value >>= iter->bits_used_in_current_bucket;
		value &= bit_array_low_bits_mask(num_bits);
		iter->bits_used_in_current_bucket += num_bits;
		Assert(iter->current_bucket < iter->array->buckets.num_elements);
		Assert(iter->current_bucket != iter->array->buckets.num_elements - 1 ||
			   iter->bits_used_in_current_bucket <= iter->array->bits_used_in_last_bucket);
		return value;
	}

	num_bits_from_next_bucket = num_bits - bits_remaining_in_current_bucket;
	if (bits_remaining_in_current_bucket > 0)
	{
		/* The first bucket has the low-order bits */
		value = *uint64_vec_get(&iter->array->buckets, iter->current_bucket);
		value >>= iter->bits_used_in_current_bucket;
	}

	/* The second bucket has the high-order bits */
	value_from_next_bucket = *uint64_vec_get(&iter->array->buckets, iter->current_bucket + 1) &
							 bit_array_low_bits_mask(num_bits_from_next_bucket);
	value_from_next_bucket <<= bits_remaining_in_current_bucket;
	value |= value_from_next_bucket;

	iter->current_bucket += 1;
	iter->bits_used_in_current_bucket = num_bits_from_next_bucket;
	Assert(iter->current_bucket < iter->array->buckets.num_elements);
	Assert(iter->current_bucket != iter->array->buckets.num_elements - 1 ||
		   iter->bits_used_in_current_bucket <= iter->array->bits_used_in_last_bucket);
	return value;
}

static inline void
bit_array_iterator_init_rev(BitArrayIterator *iter, const BitArray *array)
{
	*iter = (BitArrayIterator){
		.array = array,
		.current_bucket = array->buckets.num_elements - 1,
		.bits_used_in_current_bucket = array->bits_used_in_last_bucket,
	};
}

static inline uint64
bit_array_iter_next_rev(BitArrayIterator *iter, uint8 num_bits)
{
	uint8 bits_remaining_in_current_bucket;
	uint8 num_bits_from_previous_bucket;
	uint64 value = 0;
	uint64 bits_from_previous;
	Assert(num_bits <= BITS_PER_BUCKET);
	if (num_bits == 0)
		return 0;

	Assert(iter->current_bucket >= 0);

	bits_remaining_in_current_bucket = iter->bits_used_in_current_bucket;
	if (bits_remaining_in_current_bucket >= num_bits)
	{
		value = *uint64_vec_get(&iter->array->buckets, iter->current_bucket);
		value >>= (iter->bits_used_in_current_bucket - num_bits);
		value &= bit_array_low_bits_mask(num_bits);
		iter->bits_used_in_current_bucket -= num_bits;
		return value;
	}

	Assert(iter->current_bucket - 1 >= 0);

	num_bits_from_previous_bucket = num_bits - bits_remaining_in_current_bucket;

	Assert(num_bits <= BITS_PER_BUCKET);
	if (bits_remaining_in_current_bucket > 0)
	{
		/* The current bucket has the high-order bits (it's the second bucket) */
		value |= *uint64_vec_get(&iter->array->buckets, iter->current_bucket) &
				 bit_array_low_bits_mask(bits_remaining_in_current_bucket);
		value <<= num_bits_from_previous_bucket;
	}

	/* The previous bucket has the low-order bits (it's the first bucket) */
	bits_from_previous = *uint64_vec_get(&iter->array->buckets, iter->current_bucket - 1);
	bits_from_previous >>= BITS_PER_BUCKET - num_bits_from_previous_bucket;
	bits_from_previous &= bit_array_low_bits_mask(num_bits_from_previous_bucket);
	value |= bits_from_previous;

	iter->current_bucket -= 1;
	iter->bits_used_in_current_bucket = BITS_PER_BUCKET - num_bits_from_previous_bucket;
	return value;
}

/************************
 ***  Private Helpers ***
 ************************/
static void
bit_array_append_bucket(BitArray *array, uint8 bits_used, uint64 bucket)
{
	uint64_vec_append(&array->buckets, bucket);
	array->bits_used_in_last_bucket = bits_used;
}

static uint64
bit_array_low_bits_mask(uint8 bits_used)
{
	if (bits_used == 64)
		return PG_UINT64_MAX;
	else
		return (UINT64CONST(1) << bits_used) - UINT64CONST(1);
}

#endif
