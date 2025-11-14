/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern void _continuous_aggs_cache_inval_init(void);
extern void _continuous_aggs_cache_inval_fini(void);
extern void continuous_agg_dml_invalidate(int32 hypertable_id, Relation chunk_rel,
										  HeapTuple chunk_tuple, HeapTuple chunk_newtuple,
										  bool update);
