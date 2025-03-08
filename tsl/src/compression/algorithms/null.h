/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * The NULL compression algorithm is a no-op compression algorithm that is only
 * used to signal that all values in a compressed block are NULLs. The compression
 * interface functions are only defined to comply with the framework, but they
 * are not implemented and return an ERROR. Calling these function is a software
 * bug.
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"

/*
 * Compressor framework functions and definitions for the null algorithm.
 */

extern DecompressionIterator *null_decompression_iterator_from_datum_forward(Datum bool_compressed,
																			 Oid element_type);

extern DecompressionIterator *null_decompression_iterator_from_datum_reverse(Datum bool_compressed,
																			 Oid element_type);

extern void null_compressed_send(CompressedDataHeader *header, StringInfo buffer);

extern Datum null_compressed_recv(StringInfo buffer);

extern Compressor *null_compressor_for_type(Oid element_type);

extern void *null_compressor_get_dummy_block(void);

#define NULL_COMPRESS_ALGORITHM_DEFINITION                                                         \
	{                                                                                              \
		.iterator_init_forward = null_decompression_iterator_from_datum_forward,                   \
		.iterator_init_reverse = null_decompression_iterator_from_datum_reverse,                   \
		.decompress_all = NULL, .compressed_data_send = null_compressed_send,                      \
		.compressed_data_recv = null_compressed_recv,                                              \
		.compressor_for_type = null_compressor_for_type,                                           \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
