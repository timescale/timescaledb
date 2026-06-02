/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

/*
 * Push qualifying clauses from decompressed chunk scan down to compressed chunk
 * scan. Returns true if every clause was pushed down without needing a recheck after
 * decompression, i.e. the compressed scan can enforce all WHERE clauses on its own.
 */
bool columnar_scan_filter_pushdown(PlannerInfo *root, CompressionSettings *settings,
								   RelOptInfo *chunk_rel, RelOptInfo *compressed_rel,
								   bool chunk_partial);
