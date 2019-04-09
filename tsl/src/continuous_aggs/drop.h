/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_DROP_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_DROP_H

#include <postgres.h>

#include <chunk.h>

extern void ts_continuous_agg_drop_chunks_by_chunk_id(int32 raw_hypertable_id, Chunk **chunks,
													  Size num_chunks);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_DROP_H */
