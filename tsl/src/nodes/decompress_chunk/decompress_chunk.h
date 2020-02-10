/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "chunk.h"
#include "hypertable.h"

typedef struct CompressionInfo
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	RangeTblEntry *ht_rte;

	int hypertable_id;
	List *hypertable_compression_info;

	int num_orderby_columns;
	int num_segmentby_columns;

	/* chunk attribute numbers that are segmentby columns */
	Bitmapset *chunk_segmentby_attnos;
	/* chunk attribute numbers that have equality constraint in baserestrictinfo */
	Bitmapset *chunk_segmentby_ri;
	/* compressed chunk attribute numbers for columns that are compressed */
	Bitmapset *compressed_chunk_compressed_attnos;

} CompressionInfo;

typedef struct DecompressChunkPath
{
	CustomPath cpath;
	CompressionInfo *info;
	/*
	 * varattno_map maps targetlist entries of the compressed scan
	 * to tuple attribute number of the uncompressed chunk
	 * negative values are special columns in the compressed scan
	 * that do not have a representation in the uncompressed chunk
	 */
	List *varattno_map;
	List *compressed_pathkeys;
	bool needs_sequence_num;
	bool reverse;
} DecompressChunkPath;

void ts_decompress_chunk_generate_paths(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
										Chunk *chunk);

FormData_hypertable_compression *get_column_compressioninfo(List *hypertable_compression_info,
															char *column_name);
AttrNumber get_compressed_attno(CompressionInfo *info, AttrNumber chunk_attno);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_H */
