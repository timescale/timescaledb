/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_H

#include <postgres.h>
#include <nodes/relation.h>
#include <nodes/extensible.h>

#include "chunk.h"
#include "hypertable.h"

typedef struct DecompressChunkPath
{
	CustomPath cpath;
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	List *varattno_map;
	int hypertable_id;
	List *compression_info;
	bool reverse;
} DecompressChunkPath;

extern Path *ts_decompress_chunk_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
											 Chunk *chunk, Path *subpath);

FormData_hypertable_compression *get_column_compressioninfo(List *hypertable_compression_info,
															char *column_name);
AttrNumber get_compressed_attno(DecompressChunkPath *dcpath, AttrNumber chunk_attno);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_H */
