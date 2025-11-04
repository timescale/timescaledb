/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "chunk.h"
#include "hypertable.h"
#include "ts_catalog/compression_settings.h"

typedef struct CompressionInfo
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RelOptInfo *ht_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	RangeTblEntry *ht_rte;
	Oid compresseddata_oid;

	CompressionSettings *settings;

	int hypertable_id;
	List *hypertable_compression_info;

	int num_orderby_columns;
	int num_segmentby_columns;

	/* chunk attribute numbers that are segmentby columns */
	Bitmapset *chunk_segmentby_attnos;
	/*
	 * Chunk segmentby attribute numbers that are equated to a constant by a
	 * baserestrictinfo.
	 */
	Bitmapset *chunk_const_segmentby;
	/* compressed chunk attribute numbers for columns that are compressed */
	Bitmapset *compressed_attnos_in_compressed_chunk;

	bool single_chunk;	  /* query on explicit chunk */
	bool has_seq_num;	  /* legacy sequence number support */
	Relids parent_relids; /* relids of the parent hypertable and UNION */
} CompressionInfo;

typedef struct ColumnarScanPath
{
	CustomPath custom_path;
	const CompressionInfo *info;

	List *required_compressed_pathkeys;
	bool needs_sequence_num;
	bool reverse;
	bool batch_sorted_merge;
} ColumnarScanPath;

void ts_columnar_scan_generate_paths(PlannerInfo *root, RelOptInfo *rel, const Hypertable *ht,
									 const Chunk *chunk);

extern bool ts_is_columnar_scan_path(Path *path);
extern bool ts_is_columnar_scan_plan(Plan *plan);

ColumnarScanPath *copy_columnar_scan_path(ColumnarScanPath *src);
