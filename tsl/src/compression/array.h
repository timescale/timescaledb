/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * The `array` compression method can store any type of data. It simply puts it into an
 * array-like structure and does not compress it. TOAST-based compression should be applied on top.
 *
 * Array compression is are also used as a building block for dictionary compression.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_ARRAY_H
#define TIMESCALEDB_TSL_COMPRESSION_ARRAY_H

#include <c.h>
#include <postgres.h>

#include <fmgr.h>

#include <export.h>
#include "compression/compression.h"

typedef struct ArrayCompressor ArrayCompressor;
typedef struct ArrayCompressed ArrayCompressed;
typedef struct ArrayDecompressionIterator ArrayDecompressionIterator;

extern const Compressor array_compressor;

extern Compressor *array_compressor_for_type(Oid element_type);
extern ArrayCompressor *array_compressor_alloc(Oid type_to_compress);
extern void array_compressor_append_null(ArrayCompressor *compressor);
extern void array_compressor_append(ArrayCompressor *compressor, Datum val);
extern void *array_compressor_finish(ArrayCompressor *compressor);

extern ArrayDecompressionIterator *array_decompression_iterator_alloc(void);
extern DecompressionIterator *
tsl_array_decompression_iterator_from_datum_forward(Datum compressed_array, Oid element_type);
extern DecompressResult array_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern DecompressionIterator *
tsl_array_decompression_iterator_from_datum_reverse(Datum compressed_array, Oid element_type);
extern DecompressResult array_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

/* API for using this as an embedded data structure */
typedef struct ArrayCompressorSerializationInfo ArrayCompressorSerializationInfo;
extern ArrayCompressorSerializationInfo *
array_compressor_get_serialization_info(ArrayCompressor *compressor);
Size array_compression_serialization_size(ArrayCompressorSerializationInfo *info);
uint32 array_compression_serialization_num_elements(ArrayCompressorSerializationInfo *info);
extern char *bytes_serialize_array_compressor_and_advance(char *dst, Size dst_size,
														  ArrayCompressorSerializationInfo *info);
extern DecompressionIterator *
array_decompression_iterator_alloc_forward(const char *serialized_data, Size data_size,
										   Oid element_type, bool has_nulls);

typedef struct StringInfoData StringInfoData;
typedef StringInfoData *StringInfo;

extern ArrayCompressorSerializationInfo *array_compressed_data_recv(StringInfo buffer,
																	Oid element_type);
extern void array_compressed_data_send(StringInfo buffer, const char *serialized_data,
									   Size data_size, Oid element_type, bool has_nulls);

extern Datum array_compressed_recv(StringInfo buffer);
extern void array_compressed_send(CompressedDataHeader *header, StringInfo buffer);

extern Datum tsl_array_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_array_compressor_finish(PG_FUNCTION_ARGS);

#define ARRAY_ALGORITHM_DEFINITION                                                                 \
	{                                                                                              \
		.iterator_init_forward = tsl_array_decompression_iterator_from_datum_forward,              \
		.iterator_init_reverse = tsl_array_decompression_iterator_from_datum_reverse,              \
		.compressed_data_send = array_compressed_send,                                             \
		.compressed_data_recv = array_compressed_recv,                                             \
		.compressor_for_type = array_compressor_for_type,                                          \
		.compressed_data_storage = TOAST_STORAGE_EXTENDED,                                         \
	}

#endif
