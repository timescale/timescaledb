/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 *  The Gorilla algorithm compresses floats and is modeled after the Facebook Gorilla paper:
 *  "Gorilla: A Fast, Scalable, In-Memory Time Series Database" by Tuomas Pelkonen et. al.
 *
 * How it works: Given a series of floats, first convert that series to a series of xors between
 * consecutive floats (as uint64), while storing the first value as well. Given the first value
 * and the series of xor values, getting the floats back is trivial. So, our goal becomes to
 * compress the series of xors.
 *
 * The logic for compressing xors is as follows:
 *
 * The compression depends on the observation that a lot of xors will be mostly 0s, and that the
 * section of the xor that is non-zero will be similar in consecutive xors. So the algorithm tries
 * to record only the section of the xors that are non-zero. And records state about which section
 * of the xor its storing rarely (reusing this information from the previous xor if possible).  The
 * state of which section of the xor is recorded is kept in two variables: leading-zeroes and
 * number_of_bits_used. Thus we record only number_of_bits_used bits of the xor (shifted
 * appropriately).
 *
 * The algorithm keeps state for the number of leading-zeroes and number_of_bits_used from xor to
 * xor. A boolean array called tag1 is used to indicate that these state variables need to change
 * for the next xor. A boolean array called Tag0 is used to indicate that the xor is 0.
 *
 * The state is as follows:
 *  * two separate series of boolean bit values called tag0, tag1. (simple8brle compressed)
 *  * a series of 6-bit bit-array values called leading-zeroes (not-compressed)
 *  * a series of 64-bit ints of num_bits_used (simple8brle compressed)
 *  * a series of variable-bit-length bits stored in an bit array called xors (not-compressed)
 *
 * Pseudocode:
 * if the xor == 0:
 *   append a 0 to tag0.
 *   You are done.
 * else: (xor != 0)
 *   append a 1 to tag0.
 *   Figure out the number of leading-zeroes and number_of_bits_used necessary to store the next
 *   xor. Then, Make a decision whether to reuse the leading-zero and number of bits from the
 *   current state. (the decision is based on whether it's possible to fit the next xor in as well
 *   as a heuristic for whether it's cheaper to switch to use less number_of_bits_used)
 *
 *   if (reusing previous state)
 *     append 0 to tag1.
 *     append number_of_bits_used bits to the xors bit array. These bits consist
 *     of the next xor shifted by the appropriate amount.
 *     You are done.
 *   else: (you are changing state)
 *     append 1 to tag1
 *     append the new number of leading-zeroes to the leading-zeroes array
 *     append the new number for number_of_bits_used to the num_bits used array
 *     append new number_of_bits_used bits to the xors bit array.
 *     These bits consist of the next xor shifted by the appropriate amount.
 *     you are done.
 *
 */
#ifndef TIMESCALEDB_TSL_FLOAT_COMPRESSION_H
#define TIMESCALEDB_TSL_FLOAT_COMPRESSION_H

#include <postgres.h>
#include <c.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"
#include "export.h"

typedef struct GorillaCompressor GorillaCompressor;
typedef struct GorillaCompressed GorillaCompressed;
typedef struct GorillaDecompressionIterator GorillaDecompressionIterator;

extern Compressor *gorilla_compressor_for_type(Oid element_type);

extern GorillaCompressor *gorilla_compressor_alloc(void);
extern void gorilla_compressor_append_null(GorillaCompressor *compressor);
extern void gorilla_compressor_append_value(GorillaCompressor *compressor, uint64 val);
extern void *gorilla_compressor_finish(GorillaCompressor *compressor);

extern DecompressionIterator *
gorilla_decompression_iterator_from_datum_forward(Datum dictionary_compressed, Oid element_type);
extern DecompressResult
gorilla_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern DecompressionIterator *
gorilla_decompression_iterator_from_datum_reverse(Datum gorilla_compressed, Oid element_type);
extern DecompressResult
gorilla_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

extern void gorilla_compressed_send(CompressedDataHeader *compressed, StringInfo buffer);
extern Datum gorilla_compressed_recv(StringInfo buf);

extern Datum tsl_gorilla_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_gorilla_compressor_finish(PG_FUNCTION_ARGS);

#define GORILLA_ALGORITHM_DEFINITION                                                               \
	{                                                                                              \
		.iterator_init_forward = gorilla_decompression_iterator_from_datum_forward,                \
		.iterator_init_reverse = gorilla_decompression_iterator_from_datum_reverse,                \
		.compressed_data_send = gorilla_compressed_send,                                           \
		.compressed_data_recv = gorilla_compressed_recv,                                           \
		.compressor_for_type = gorilla_compressor_for_type,                                        \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}

#endif
