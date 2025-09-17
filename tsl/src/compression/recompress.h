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

/* Structure to hold recompression sort information */
typedef struct RecompressContext
{
	int num_segmentby;
	int num_orderby;
	int n_keys;
	AttrNumber *sort_keys;
	Oid *sort_operators;
	Oid *sort_collations;
	bool *nulls_first;
	CompressedSegmentInfo *current_segment;
	ScanKeyData *index_scankeys;
	ScanKeyData *orderby_scankeys;
} RecompressContext;

extern Datum tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS);

Oid recompress_chunk_segmentwise_impl(Chunk *chunk);
bool recompress_chunk_in_memory_impl(Chunk *uncompressed_chunk);

/* Result of matching an uncompressed tuple against a compressed batch */
enum Batch_match_result
{
	Tuple_before = 1,
	Tuple_match,
	Tuple_after,
	_Batch_match_result_max,
};
