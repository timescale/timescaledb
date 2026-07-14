/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * The `external` compression method delegates batches to a pair of functions
 * supplied by the column's data type:
 *     <schema>.<typename>_compress(<type>[]) RETURNS bytea
 *     <schema>.<typename>_decompress(bytea)  RETURNS <type>[]
 *
 * Both functions must exist in the type's schema for the type to be routed to
 * this compression method (see compression_get_default_algorithm).
 * NULLs are stripped into a bitmap before the compress call and re-added
 * after decompression, so the functions only receive non-null values. The
 * returned bytea is stored as-is in TOAST_STORAGE_EXTERNAL, and is not
 * re-compressed.
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"

/*
 * Look up the compress/decompress function pair for a type by the naming
 * convention:
 *
 *     <schema>.<typename>_compress(<type>[]) RETURNS bytea
 *     <schema>.<typename>_decompress(bytea)  RETURNS <type>[]
 *
 * Returns false if either function is missing.
 * NULL out parameters will be filled if there is a function registered.
 */
extern bool external_codec_lookup(Oid type_oid, Oid *compress_fn, Oid *decompress_fn);

extern const Compressor external_compressor;

extern bool external_compressed_has_nulls(const CompressedDataHeader *header);
extern Compressor *external_compressor_for_type(Oid element_type);

extern DecompressionIterator *
tsl_external_decompression_iterator_from_datum_forward(Datum compressed, Oid element_type);
extern DecompressionIterator *
tsl_external_decompression_iterator_from_datum_reverse(Datum compressed, Oid element_type);

extern void external_compressed_send(CompressedDataHeader *header, StringInfo buffer);
extern Datum external_compressed_recv(StringInfo buffer);

#define EXTERNAL_ALGORITHM_DEFINITION                                                              \
	{                                                                                              \
		.iterator_init_forward = tsl_external_decompression_iterator_from_datum_forward,           \
		.iterator_init_reverse = tsl_external_decompression_iterator_from_datum_reverse,           \
		.compressed_data_send = external_compressed_send,                                          \
		.compressed_data_recv = external_compressed_recv,                                          \
		.compressor_for_type = external_compressor_for_type,                                       \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
