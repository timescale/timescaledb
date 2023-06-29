/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_API_H
#define TIMESCALEDB_TSL_COMPRESSION_API_H

#include <postgres.h>
#include <fmgr.h>

extern Datum tsl_create_compressed_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_create_compressed_chunks_for_hypertable(PG_FUNCTION_ARGS);
extern Datum tsl_compress_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_decompress_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_recompress_chunk(PG_FUNCTION_ARGS);
extern Oid tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed);
extern bool tsl_recompress_chunk_wrapper(Chunk *chunk);
extern Datum tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS);

extern Datum tsl_get_compressed_chunk_index_for_recompression(
	PG_FUNCTION_ARGS); // arg is oid of uncompressed chunk

#endif /* TIMESCALEDB_TSL_COMPRESSION_API_H */
