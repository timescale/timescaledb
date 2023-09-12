/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_RECOMPRESSION_H
#define TIMESCALEDB_TSL_COMPRESSION_RECOMPRESSION_H

/* build scankeys for segmentby columns */
#define SEGMENTBY_KEYS (1 << 1)
/* build scankeys for orderby columns */
#define ORDERBY_KEYS (1 << 2)
/* build scankeys for orderby columns */
#define OUTOFRANGE_ORDERBY_KEYS (1 << 3)
/* build scankeys for _ts_meta_sequence_num columns */
#define SEQNUM_KEYS (1 << 4)
/* build scankeys for _ts_meta_sequence_num columns with next value */
#define NEXT_SEQNUM_KEYS (1 << 5)

typedef struct Chunk Chunk;
void recompress_partial_chunks(const Chunk *uncompressed_chunk, const Chunk *compressed_chunk);

#endif
