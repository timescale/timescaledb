/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/memutils.h>

/*
 * One decoded tenant set (the tracking rows of one hypertable/seqnum) kept
 * across the batches of a refresh, so the tenant-scoped DELETE can bind the
 * overlapping tenants as an array without rereading the catalog per batch.
 */
typedef struct TenantSetCache TenantSetCache;

extern TenantSetCache *ts_tenant_set_cache_create(MemoryContext parent);
extern void ts_tenant_set_cache_destroy(TenantSetCache *cache);

/*
 * Make the set for (hypertable_id, seqnum) current, reloading it from the
 * tracking table if a different one is loaded.
 */
extern void ts_tenant_set_cache_get(TenantSetCache *cache, int32 hypertable_id, int32 seqnum,
									Oid tenant_typid, int32 tenant_typmod);

/*
 * Array (of the tenant column's array type) of the current set's tenants whose
 * tracked range overlaps [start, end). Returns the element count; 0 leaves
 * *array untouched. The array belongs to the cache: valid until the next call
 * that builds a different selection, or the next reload.
 */
extern int ts_tenant_set_cache_build_array(TenantSetCache *cache, int64 start, int64 end,
										   Datum *array);
extern Oid ts_tenant_set_cache_array_type(const TenantSetCache *cache);
