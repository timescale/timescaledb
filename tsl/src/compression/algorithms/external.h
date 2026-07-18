/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * The `external` compression algorithm delegates batches to a pair of
 * support functions registered to an operator class for the
 * ts_compression_codec access method (see compression_codec_am.h):
 *
 *     CREATE OPERATOR CLASS <name> FOR TYPE <type>
 *         USING ts_compression_codec AS
 *         FUNCTION 1 <compress>(<type>[]),   -- RETURNS bytea
 *         FUNCTION 2 <decompress>(bytea);    -- RETURNS <type>[]
 *
 * A column is routed to this compression method only when it is configured
 * through the timescaledb.compress_column_codec setting. This setting holds
 * the operator class to use (see compression_get_column_algorithm) for
 * compression.
 * NULLs are stripped into a bitmap before the compress call and re-added
 * after decompression, so the functions only receive non-null values. The
 * returned bytea is stored as-is in TOAST_STORAGE_EXTERNAL. It is not
 * re-compressed.
 *
 * Every batch records the schema-qualified name of the operator class that
 * compressed it. Decompression resolves the codec by that recorded name.
 * Several codecs can therefore coexist for one type. The setting decides
 * new writes, while other (possibly decompress-only) opclasses keep the
 * batches they previously wrote readable. Renaming or dropping an operator
 * class while batches still use it makes those batches unreadable until
 * the opclass is restored.
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

#include "compression/compression.h"

extern const Compressor external_compressor;

extern bool external_compressed_has_nulls(const CompressedDataHeader *header);
/*
 * Compressor for batches with the codec for the column's
 * timescaledb.compress_column_codec entry.
 * Errors if the operator class does not satisfy the codec contract
 * for element_type.
 */
extern Compressor *external_compressor_for_opclass(Oid element_type, const char *opclass_qualname);

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
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
