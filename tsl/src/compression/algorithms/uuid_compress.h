/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * uuid_compress is used to encode UUID values where there are 4 distinct parts
 * that are encoded separately. The UUID being encoded is optimised for the v7
 * UUID format. The four parts are:
 *
 *  - timestamp : (delta-delta encoding)
 *  - the version number and variant number concatenated : (simple8b_rle)
 *  - rand_a : is encoded as an array of bits
 *  - rand_b : is encoded as an array of bits
 */

#include <postgres.h>
#include "compression/compression.h"
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/uuid.h>

typedef struct UuidCompressor UuidCompressor;
typedef struct UuidCompressed UuidCompressed;
typedef struct UuidDecompressionIterator UuidDecompressionIterator;

/*
 * Compressor framework functions and definitions for the uuid_compress algorithm.
 */

extern UuidCompressor *uuid_compressor_alloc(void);
extern void uuid_compressor_append_null(UuidCompressor *compressor);
extern void uuid_compressor_append_value(UuidCompressor *compressor, pg_uuid_t next_val);
extern void *uuid_compressor_finish(UuidCompressor *compressor);
extern bool uuid_compressed_has_nulls(const CompressedDataHeader *header);

extern DecompressResult uuid_decompression_iterator_try_next_forward(DecompressionIterator *iter);

extern DecompressionIterator *uuid_decompression_iterator_from_datum_forward(Datum uuid_compressed,
																			 Oid element_type);

extern DecompressResult uuid_decompression_iterator_try_next_reverse(DecompressionIterator *iter);

extern DecompressionIterator *uuid_decompression_iterator_from_datum_reverse(Datum uuid_compressed,
																			 Oid element_type);

extern void uuid_compressed_send(CompressedDataHeader *header, StringInfo buffer);

extern Datum uuid_compressed_recv(StringInfo buf);

extern Compressor *uuid_compressor_for_type(Oid element_type);

#define UUID_COMPRESS_ALGORITHM_DEFINITION                                                         \
	{                                                                                              \
		.iterator_init_forward = uuid_decompression_iterator_from_datum_forward,                   \
		.iterator_init_reverse = uuid_decompression_iterator_from_datum_reverse,                   \
		.decompress_all = NULL, .compressed_data_send = uuid_compressed_send,                      \
		.compressed_data_recv = uuid_compressed_recv,                                              \
		.compressor_for_type = uuid_compressor_for_type,                                           \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}

/*
 * Cross-module functions for the uuid_compress algorithm.
 */

extern Datum tsl_uuid_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_uuid_compressor_finish(PG_FUNCTION_ARGS);
