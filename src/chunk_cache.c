#include <postgres.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <storage/lmgr.h>
#include <access/xact.h>
#include <storage/bufmgr.h>
#include <catalog/namespace.h>

#include "chunk_cache.h"
#include "chunk.h"
#include "catalog.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"
#include "scanner.h"

/*
 * Chunk cache.
 *
 * This cache stores information about chunks (and their replicas) in the
 * database. Chunks are stored in the cache by chunk ID.
 *
 * However, since a chunk's ID is generally unknown at lookup time or the chunk
 * may not yet exist, one typically has to scan the chunk table first to find
 * the chunk ID for a specific tuple's time point and partition. The cache
 * therefore mostly serves to store information about a chunk's replicas, which
 * otherwise would require an additional table scan.
 */
static Cache *chunk_cache_current = NULL;

/* Cache entry for chunk replicas */

static Cache *
chunk_cache_create()
{
	/* not used; must be fixed TODO */
	MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext,
							  catalog_get_cache_proxy_name(CACHE_TYPE_CHUNK),
											  ALLOCSET_DEFAULT_SIZES);

	Cache	   *cache = MemoryContextAlloc(ctx, sizeof(Cache));

	Cache		template =
	{
		.hctl =
		{
			.hcxt = ctx,
		},
		.name = "chunk_cache",
	};

	*cache = template;

	cache_init(cache);

	return cache;
}

void
chunk_cache_invalidate_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY chunk cache");
	cache_invalidate(chunk_cache_current);
	chunk_cache_current = chunk_cache_create();
}

extern Cache *
chunk_cache_pin()
{
	return cache_pin(chunk_cache_current);
}

typedef struct ChunkScanCtx
{
	Chunk	   *chunk;
	int32		partition_id;
	int64		starttime,
				endtime,
				timepoint;
	bool		should_lock;
} ChunkScanCtx;

static bool
chunk_tuple_timepoint_filter(TupleInfo *ti, void *arg)
{
	ChunkScanCtx *ctx = arg;
	bool		starttime_is_null,
				endtime_is_null;
	Datum		datum;

	datum = heap_getattr(ti->tuple, Anum_chunk_start_time, ti->desc, &starttime_is_null);
	ctx->starttime = starttime_is_null ? OPEN_START_TIME : DatumGetInt64(datum);
	datum = heap_getattr(ti->tuple, Anum_chunk_end_time, ti->desc, &endtime_is_null);
	ctx->endtime = endtime_is_null ? OPEN_END_TIME : DatumGetInt64(datum);

	if ((starttime_is_null || ctx->timepoint >= ctx->starttime) &&
		(endtime_is_null || ctx->timepoint <= ctx->endtime))
		return true;

	return false;
}

static bool
chunk_tuple_found(TupleInfo *ti, void *arg)
{
	ChunkScanCtx *ctx = arg;

	ctx->chunk = chunk_create(ti->tuple, ti->desc, NULL);
	return false;
}

static Chunk *
chunk_scan(int32 partition_id, int64 timepoint, bool tuplock)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	ChunkScanCtx cq = {
		.partition_id = partition_id,
		.timepoint = timepoint,
	};
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[CHUNK_PARTITION_TIME_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &cq,
		.filter = chunk_tuple_timepoint_filter,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.tuplock = {
			.lockmode = LockTupleShare,
			.enabled = tuplock,
		},
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on epoch ID to find the partitions for the epoch.
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_partition_start_time_end_time_idx_partition_id,
				BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(partition_id));

	scanner_scan(&ctx);

	return cq.chunk;
}

/*
 *	Get chunk cache entry.
 */
Chunk *
chunk_cache_get(Cache *cache, Partition *part, int64 timepoint)
{
	Chunk	   *stub_chunk;

	/* This does not use a cache right now. TODO!!! MUST FIX */
	stub_chunk = chunk_scan(part->id, timepoint, false);

	if (stub_chunk == NULL)
	{
		stub_chunk = chunk_insert_new(part->id, timepoint);
	}

	return stub_chunk;
}

void
_chunk_cache_init(void)
{
	CreateCacheMemoryContext();
	chunk_cache_current = chunk_cache_create();
}

void
_chunk_cache_fini(void)
{
	cache_invalidate(chunk_cache_current);
}
