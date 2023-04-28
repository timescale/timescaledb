/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPRESSED_SKIP_SCAN_H
#define TIMESCALEDB_COMPRESSED_SKIP_SCAN_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "chunk.h"
#include "hypertable.h"
#include "nodes/nodes_common.h"

typedef struct CompressedSkipScanPath
{
	CustomPath cpath;
	CompressionInfo *info;
	/*
	 * decompression_map maps targetlist entries of the compressed scan to tuple
	 * attribute number of the uncompressed chunk. Negative values are special
	 * columns in the compressed scan that do not have a representation in the
	 * uncompressed chunk, but are still used for decompression.
	 */
	List *decompression_map;
	List *compressed_pathkeys;
	bool needs_sequence_num;
	bool reverse;
} CompressedSkipScanPath;

CompressedSkipScanPath *compressed_skip_scan_path_create(PlannerInfo *, CustomPath *,
														 CompressionInfo *, List *, bool, bool,
														 List *);

#endif /* TIMESCALEDB_COMPRESSED_SKIP_SCAN_H */
