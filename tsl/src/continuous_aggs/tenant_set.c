/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <fmgr.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>

#include "debug_assert.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "tenant_set.h"
#include "ts_catalog/catalog.h"

struct TenantSetCache
{
	/* Holds everything below; reset when a different seqnum is loaded */
	MemoryContext mcxt;
	int32 hypertable_id;
	int32 seqnum; /* 0 when nothing is loaded */
	Oid typid;
	Oid array_typid;
	int16 typlen;
	bool typbyval;
	char typalign;
	int n;
	Datum *keys;
	int64 *min;
	int64 *max;
	/* Per-batch selection scratch and the array built from the last selection */
	int *sel;
	int *last_sel;
	int last_nsel;
	Datum last_array;
	Datum *elems;
};

TenantSetCache *
ts_tenant_set_cache_create(MemoryContext parent)
{
	TenantSetCache *cache = MemoryContextAllocZero(parent, sizeof(TenantSetCache));

	cache->mcxt = AllocSetContextCreate(parent, "TenantSetCache", ALLOCSET_DEFAULT_SIZES);
	cache->last_nsel = -1;
	return cache;
}

void
ts_tenant_set_cache_destroy(TenantSetCache *cache)
{
	MemoryContextDelete(cache->mcxt);
	pfree(cache);
}

Oid
ts_tenant_set_cache_array_type(const TenantSetCache *cache)
{
	return cache->array_typid;
}

void
ts_tenant_set_cache_get(TenantSetCache *cache, int32 hypertable_id, int32 seqnum, Oid tenant_typid,
						int32 tenant_typmod)
{
	if (cache->seqnum == seqnum && cache->hypertable_id == hypertable_id)
	{
		return;
	}

	MemoryContextReset(cache->mcxt);
	cache->hypertable_id = hypertable_id;
	cache->seqnum = seqnum;
	cache->n = 0;
	cache->keys = NULL;
	cache->min = NULL;
	cache->max = NULL;
	cache->sel = NULL;
	cache->last_sel = NULL;
	cache->last_nsel = -1;
	cache->last_array = (Datum) 0;
	cache->elems = NULL;

	cache->typid = tenant_typid;
	get_typlenbyvalalign(tenant_typid, &cache->typlen, &cache->typbyval, &cache->typalign);
	cache->array_typid = get_array_type(tenant_typid);
	Ensure(OidIsValid(cache->array_typid), "no array type for tenant type %u", tenant_typid);

	Oid infunc;
	Oid ioparam;
	getTypeInputInfo(tenant_typid, &infunc, &ioparam);

	/*
	 * Tracking rows of a drained seqnum are immutable, and the garbage collector
	 * runs once per invocation in batch 1 txn 1, before the first load, so the
	 * set stays valid for every later batch of this refresh. The row count is
	 * bounded by the tracker generation's TENANT_TRACKER_CAPACITY (tenant_tracker.c).
	 */
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_TENANT_TRACKING,
													AccessShareLock,
													CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(),
										   CONTINUOUS_AGGS_TENANT_TRACKING,
										   CONTINUOUS_AGGS_TENANT_TRACKING_IDX);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_aggs_tenant_tracking_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_aggs_tenant_tracking_idx_seqnum,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(seqnum));

	int capacity = 64;
	MemoryContext oldcxt = MemoryContextSwitchTo(cache->mcxt);
	cache->keys = palloc(capacity * sizeof(Datum));
	cache->min = palloc(capacity * sizeof(int64));
	cache->max = palloc(capacity * sizeof(int64));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool isnull;

		Datum tenant =
			slot_getattr(ti->slot, Anum_continuous_aggs_tenant_tracking_tenant_id, &isnull);
		Assert(!isnull);
		if (isnull)
		{
			continue;
		}

		if (cache->n == capacity)
		{
			capacity *= 2;
			cache->keys = repalloc(cache->keys, capacity * sizeof(Datum));
			cache->min = repalloc(cache->min, capacity * sizeof(int64));
			cache->max = repalloc(cache->max, capacity * sizeof(int64));
		}

		char *key_text = TextDatumGetCString(tenant);
		cache->keys[cache->n] = OidInputFunctionCall(infunc, key_text, ioparam, tenant_typmod);
		pfree(key_text);
		cache->min[cache->n] = DatumGetInt64(
			slot_getattr(ti->slot, Anum_continuous_aggs_tenant_tracking_min_timestamp, &isnull));
		cache->max[cache->n] = DatumGetInt64(
			slot_getattr(ti->slot, Anum_continuous_aggs_tenant_tracking_max_timestamp, &isnull));
		cache->n++;
	}
	MemoryContextSwitchTo(oldcxt);
	ts_scan_iterator_close(&iterator);

	int nalloc = Max(cache->n, 1);
	cache->sel = MemoryContextAlloc(cache->mcxt, nalloc * sizeof(int));
	cache->last_sel = MemoryContextAlloc(cache->mcxt, nalloc * sizeof(int));
	cache->elems = MemoryContextAlloc(cache->mcxt, nalloc * sizeof(Datum));

	elog(DEBUG1,
		 "tenant set cache: loaded %d tenants for hypertable %d seqnum %d",
		 cache->n,
		 hypertable_id,
		 seqnum);
}

int
ts_tenant_set_cache_build_array(TenantSetCache *cache, int64 start, int64 end, Datum *array)
{
	int nsel = 0;

	Assert(cache->seqnum != 0);

	for (int i = 0; i < cache->n; i++)
	{
		if (cache->min[i] < end && cache->max[i] >= start)
		{
			cache->sel[nsel++] = i;
		}
	}

	if (nsel == 0)
	{
		return 0;
	}

	if (nsel == cache->last_nsel && memcmp(cache->sel, cache->last_sel, nsel * sizeof(int)) == 0)
	{
		*array = cache->last_array;
		return nsel;
	}

	for (int j = 0; j < nsel; j++)
	{
		cache->elems[j] = cache->keys[cache->sel[j]];
	}

	MemoryContext oldcxt = MemoryContextSwitchTo(cache->mcxt);
	if (cache->last_array != (Datum) 0)
	{
		pfree(DatumGetPointer(cache->last_array));
	}
	cache->last_array = PointerGetDatum(construct_array(cache->elems,
														nsel,
														cache->typid,
														cache->typlen,
														cache->typbyval,
														cache->typalign));
	MemoryContextSwitchTo(oldcxt);

	memcpy(cache->last_sel, cache->sel, nsel * sizeof(int));
	cache->last_nsel = nsel;
	*array = cache->last_array;
	return nsel;
}
