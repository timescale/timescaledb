/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/tuptable.h>

extern void _continuous_aggs_cache_inval_init(void);
extern void _continuous_aggs_cache_inval_fini(void);
extern void continuous_agg_invalidate_range(int32 hypertable_id, Oid chunk_relid, int64 start,
											int64 end, bool tenants_unknown);
extern void continuous_agg_dml_invalidate(int32 hypertable_id, Relation chunk_rel,
										  HeapTuple chunk_tuple, HeapTuple chunk_newtuple,
										  bool update);
extern void continuous_agg_record_tenant_from_slot(int32 hypertable_id, Oid chunk_relid,
												   TupleTableSlot *slot);
