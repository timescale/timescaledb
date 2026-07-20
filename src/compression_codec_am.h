/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

/*
 * The ts_compression_codec access method is a registration namespace, a
 * virtual access method. A codec for the EXTERNAL compression algorithm
 * is registered as an operator class for this access method with two
 * support functions:
 *
 *     FUNCTION 1  <compress>(<type>[]) RETURNS bytea
 *     FUNCTION 2  <decompress>(bytea)  RETURNS <type>[]
 *
 * A column is compressed with a codec when configured by a hypertable's
 * `timescaledb.compress_column_codec` setting.
 * Batches record the operator class that wrote them and decompression
 * resolves opclass by that name. A registration with only FUNCTION 2 is a
 * backward-compatibility state. It cannot be used by the compress_column_codec
 * setting, but allows the batches it wrote to be read still (see the External
 * section of compression/README.md for the migration steps).
 */

#include <postgres.h>

#include "export.h"

#define COMPRESSION_CODEC_AM_NAME "ts_compression_codec"
#define COMPRESSION_CODEC_COMPRESS_PROC 1
#define COMPRESSION_CODEC_DECOMPRESS_PROC 2

extern TSDLLEXPORT char *ts_compression_codec_opclass_qualname(Oid opclass);
extern TSDLLEXPORT void ts_compression_codec_opclass_functions(Oid opclass, Oid element_type,
															   bool require_compress,
															   Oid *compress_fn,
															   Oid *decompress_fn);
extern TSDLLEXPORT Oid ts_compression_codec_opclass_resolve(const char *opclass_name,
															Oid element_type, bool require_compress,
															Oid *compress_fn, Oid *decompress_fn);
