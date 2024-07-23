/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * Deltadelta is used to encode integers or integer-like objects (e.g. timestamps). It's input is a
 * series of integers. first convert that series to a series of delta-of-deltas between
 * consecutive integers, while storing the first value as well. Given the first value
 * and the series of delta-of-delta values, it is easy to reconstruct the original series of
 * integers (assume first delta is 0).
 *
 * We now describe how to compress the delta-of-deltas:
 * First we zigzag encodes the delta-of-deltas
 * Second, we simple8b_rle encode the zig-zag encoding
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"

typedef struct DeltaDeltaCompressor DeltaDeltaCompressor;
typedef struct DeltaDeltaCompressed DeltaDeltaCompressed;
typedef struct DeltaDeltaDecompressionIterator DeltaDeltaDecompressionIterator;

extern Compressor *delta_delta_compressor_for_type(Oid element_type);
extern DeltaDeltaCompressor *delta_delta_compressor_alloc(void);
extern void delta_delta_compressor_append_null(DeltaDeltaCompressor *compressor);
extern void delta_delta_compressor_append_value(DeltaDeltaCompressor *compressor, int64 next_val);
extern void *delta_delta_compressor_finish(DeltaDeltaCompressor *compressor);

extern DecompressionIterator *
delta_delta_decompression_iterator_from_datum_forward(Datum deltadelta_compressed,
													  Oid element_type);
extern DecompressionIterator *
delta_delta_decompression_iterator_from_datum_reverse(Datum deltadelta_compressed,
													  Oid element_type);
extern DecompressResult
delta_delta_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern ArrowArray *delta_delta_decompress_all(Datum compressed_data, Oid element_type,
											  MemoryContext dest_mctx);

extern DecompressResult
delta_delta_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

extern void deltadelta_compressed_send(CompressedDataHeader *header, StringInfo buffer);
extern Datum deltadelta_compressed_recv(StringInfo buf);

extern Datum tsl_deltadelta_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_deltadelta_compressor_finish(PG_FUNCTION_ARGS);

#define DELTA_DELTA_ALGORITHM_DEFINITION                                                           \
	{                                                                                              \
		.iterator_init_forward = delta_delta_decompression_iterator_from_datum_forward,            \
		.iterator_init_reverse = delta_delta_decompression_iterator_from_datum_reverse,            \
		.decompress_all = delta_delta_decompress_all,                                              \
		.compressed_data_send = deltadelta_compressed_send,                                        \
		.compressed_data_recv = deltadelta_compressed_recv,                                        \
		.compressor_for_type = delta_delta_compressor_for_type,                                    \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
