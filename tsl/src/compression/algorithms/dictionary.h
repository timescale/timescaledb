/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * The Dictionary compressions scheme can store any type of data but is optimized for
 * low-cardinality data sets. The dictionary of distinct items is stored as an `array` compressed
 * object. The row->dictionary item mapping is stored as a series of integer-based indexes into the
 * dictionary array ordered by row number (called dictionary_indexes; compressed using
 * `simple8b_rle`).
 */

#include <postgres.h>
#include "compression/compression.h"
#include <lib/stringinfo.h>

#include <fmgr.h>

typedef struct DictionaryCompressor DictionaryCompressor;
typedef struct DictionaryCompressed DictionaryCompressed;
typedef struct DictionaryDecompressionIterator DictionaryDecompressionIterator;

extern bool dictionary_compressed_has_nulls(const CompressedDataHeader *header);
extern Compressor *dictionary_compressor_for_type(Oid element_type);
extern DictionaryCompressor *dictionary_compressor_alloc(Oid type_to_compress);
extern void dictionary_compressor_append_null(DictionaryCompressor *compressor);
extern void dictionary_compressor_append(DictionaryCompressor *compressor, Datum val);
extern void *dictionary_compressor_finish(DictionaryCompressor *compressor);

extern DecompressionIterator *
tsl_dictionary_decompression_iterator_from_datum_forward(Datum dictionary_compressed,
														 Oid element_type);
extern DecompressResult
dictionary_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern DecompressionIterator *
tsl_dictionary_decompression_iterator_from_datum_reverse(Datum dictionary_compressed,
														 Oid element_type);
extern DecompressResult
dictionary_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

extern void dictionary_compressed_send(CompressedDataHeader *header, StringInfo buffer);
extern Datum dictionary_compressed_recv(StringInfo buf);

extern Datum tsl_dictionary_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_dictionary_compressor_finish(PG_FUNCTION_ARGS);

ArrowArray *tsl_text_array_decompress_all(Datum compressed_array, Oid element_type,
										  MemoryContext dest_mctx);

ArrowArray *tsl_text_dictionary_decompress_all(Datum compressed, Oid element_type,
											   MemoryContext dest_mctx);

#define DICTIONARY_ALGORITHM_DEFINITION                                                            \
	{                                                                                              \
		.iterator_init_forward = tsl_dictionary_decompression_iterator_from_datum_forward,         \
		.iterator_init_reverse = tsl_dictionary_decompression_iterator_from_datum_reverse,         \
		.compressed_data_send = dictionary_compressed_send,                                        \
		.compressed_data_recv = dictionary_compressed_recv,                                        \
		.compressor_for_type = dictionary_compressor_for_type,                                     \
		.compressed_data_storage = TOAST_STORAGE_EXTENDED,                                         \
		.decompress_all = tsl_text_dictionary_decompress_all,                                      \
	}
