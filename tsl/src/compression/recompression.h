/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_RECOMPRESSION_H
#define TIMESCALEDB_TSL_COMPRESSION_RECOMPRESSION_H

typedef struct Chunk Chunk;
void recompress_partial_chunks(Chunk *uncompressed_chunk, Chunk *compressed_chunk);

#endif
