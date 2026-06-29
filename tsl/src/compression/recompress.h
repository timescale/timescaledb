/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <utils.h>
#include <utils/sortsupport.h>

#include "chunk.h"

/* Structure to hold recompression sort information */
typedef struct RecompressContext
{
	int num_segmentby;
	int num_orderby;
	int n_keys;
	AttrNumber sort_keys[INDEX_MAX_KEYS];
	Oid sort_operators[INDEX_MAX_KEYS];
	Oid sort_collations[INDEX_MAX_KEYS];
	bool nulls_first[INDEX_MAX_KEYS];
	CompressedSegmentInfo current_segment[INDEX_MAX_KEYS];
	ScanKeyData index_scankeys[INDEX_MAX_KEYS];
	ScanKeyData orderby_scankeys[INDEX_MAX_KEYS * 2]; /* for min and max */
	bool key_byval[INDEX_MAX_KEYS];
	int16 key_typlen[INDEX_MAX_KEYS];

	/* Index attribute numbers of the first-row and last-row orderby metadata
	 * columns per orderby column. Resolved from the chunk's actual index so
	 * that both legacy (last, first) and current (first, last) layouts work. */
	AttrNumber orderby_first_index_attno[INDEX_MAX_KEYS];
	AttrNumber orderby_last_index_attno[INDEX_MAX_KEYS];

	/* Cached sort support per orderby column, used to compare batch boundary
	 * values during compaction. */
	SortSupportData *orderby_ssup;
} RecompressContext;

extern Datum tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS);
extern Datum tsl_compact_chunk(PG_FUNCTION_ARGS);

void recompress_chunk_segmentwise_impl(Chunk *chunk, bool fullrecompress);
Oid compact_chunk_impl(Chunk *chunk);
bool recompress_chunk_in_memory_impl(Chunk *uncompressed_chunk);
void rebuild_sparse_index_impl(Chunk *uncompressed_chunk, bool force);

/* Result of matching an uncompressed tuple against a compressed batch */
enum Batch_match_result
{
	Tuple_before = 1,
	Tuple_match,
	Tuple_after,
	_Batch_match_result_max,
};
