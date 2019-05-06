/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ADT_BITARRAY_H
#define TIMESCALEDB_ADT_BITARRAY_H

#include <postgres.h>

#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include <adts/uint64_vec.h>
#include "bit_array_impl.h"

/*******************
 ***  Public API ***
 *******************/

/* Array to hold blobs of bits of arbitrary sizes. The interface
 * expects you to read the same amount of bits, in the same order, as what
 * was written.
 */
typedef struct BitArray BitArray;
typedef struct BitArrayIterator BitArrayIterator;

/* Main Interface */
static void bit_array_init(BitArray *array);

/* Append num_bits to the array */
static void bit_array_append(BitArray *array, uint8 num_bits, uint64 bits);

static void bit_array_iterator_init(BitArrayIterator *iter, const BitArray *array);
/* return next num_bits from the iterator; must have been written as num_bits */
static uint64 bit_array_iter_next(BitArrayIterator *iter, uint8 num_bits);
static void bit_array_iterator_init_rev(BitArrayIterator *iter, const BitArray *array);
/* return last num_bits in forward-order (not reverse-order); must have been written as num_bits */
static uint64 bit_array_iter_next_rev(BitArrayIterator *iter, uint8 num_bits);

/* I/O */
static inline void bit_array_send(StringInfo buffer, const BitArray *data);
static inline BitArray bit_array_recv(const StringInfo buffer);
static char *bytes_store_bit_array_and_advance(char *dest, size_t expected_size,
											   const BitArray *array, uint32 *num_buckets,
											   uint8 *bits_in_last_bucket);
static size_t bit_array_output(const BitArray *array, uint64 *data, size_t max_n_bytes,
							   uint64 *num_bits_out);
static void bit_array_wrap(BitArray *dst, uint64 *data, uint64 num_bits);
static const char *bytes_attach_bit_array_and_advance(BitArray *dst, const char *data,
													  uint32 num_buckets,
													  uint8 bits_in_last_bucket);

/* Accessors / Info */
static uint64 bit_array_num_bits(const BitArray *array);
static uint32 bit_array_num_buckets(const BitArray *array);
static uint64 *bit_array_buckets(const BitArray *array);
static size_t bit_array_data_bytes_used(const BitArray *array);

#endif
