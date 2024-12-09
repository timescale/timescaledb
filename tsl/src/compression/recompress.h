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

extern Datum tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS);

Oid recompress_chunk_segmentwise_impl(Chunk *chunk);

/* Result of matching an uncompressed tuple against a compressed batch */
enum Batch_match_result
{
	Tuple_before = 1,
	Tuple_match,
	Tuple_after,
	_Batch_match_result_max,
};
