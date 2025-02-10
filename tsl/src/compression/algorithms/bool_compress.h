/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * bool_compress is used to encode boolean values using the simple8b_rle algorithm.
 * In essence, it is a simplified version of the delta-delta compression, by removing
 * the delta-delta and zig-zag encoding steps.
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"

typedef struct BoolCompressor BoolCompressor;
typedef struct BoolCompressed BoolCompressed;
typedef struct BoolDecompressionIterator BoolDecompressionIterator;

/*
 * Compressor framework functions and definitions for the bool_compress algorithm.
 */

extern BoolCompressor *bool_compressor_alloc(void);
extern void bool_compressor_append_null(BoolCompressor *compressor);
extern void bool_compressor_append_value(BoolCompressor *compressor, bool next_val);
extern void *bool_compressor_finish(BoolCompressor *compressor);
extern bool bool_compressed_has_nulls(const CompressedDataHeader *header);

extern DecompressResult bool_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern DecompressionIterator *bool_decompression_iterator_from_datum_forward(Datum bool_compressed,
																			 Oid element_type);

extern DecompressResult bool_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

extern DecompressionIterator *bool_decompression_iterator_from_datum_reverse(Datum bool_compressed,
																			 Oid element_type);

extern void bool_compressed_send(CompressedDataHeader *header, StringInfo buffer);

extern Datum bool_compressed_recv(StringInfo buf);

extern Compressor *bool_compressor_for_type(Oid element_type);

#define BOOL_COMPRESS_ALGORITHM_DEFINITION                                                         \
	{                                                                                              \
		.iterator_init_forward = bool_decompression_iterator_from_datum_forward,                   \
		.iterator_init_reverse = bool_decompression_iterator_from_datum_reverse,                   \
		.decompress_all = NULL, .compressed_data_send = bool_compressed_send,                      \
		.compressed_data_recv = bool_compressed_recv,                                              \
		.compressor_for_type = bool_compressor_for_type,                                           \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}

/*
 * Cross-module functions for the bool_compress algorithm.
 */

extern Datum tsl_bool_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_bool_compressor_finish(PG_FUNCTION_ARGS);
