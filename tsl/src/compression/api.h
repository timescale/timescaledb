/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <utils.h>

#include "chunk.h"

extern Datum tsl_create_compressed_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_compress_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_decompress_chunk(PG_FUNCTION_ARGS);
extern Oid tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed, bool recompress);
extern Datum tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS);

extern Datum tsl_get_compressed_chunk_index_for_recompression(
	PG_FUNCTION_ARGS); // arg is oid of uncompressed chunk

extern void compression_chunk_size_catalog_insert(int32 src_chunk_id, const RelationSize *src_size,
												  int32 compress_chunk_id,
												  const RelationSize *compress_size,
												  int64 rowcnt_pre_compression,
												  int64 rowcnt_post_compression,
												  int64 rowcnt_frozen);
