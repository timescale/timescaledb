#include <postgres.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <storage/lmgr.h>
#include <access/xact.h>
#include <storage/bufmgr.h>

#include "chunk_cache.h"
#include "catalog.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"
#include "scanner.h"

/*
 * Chunk Insert Plan Cache:
 *
 * Hashtable of chunk_id =>  chunk_crn_set_htable_entry.
 *
 * This cache stores plans for the execution of the command for moving stuff
 * from the copy table to the tables associated with the chunk.
 *
 * Retrieval: each chunk has one associated plan. If the chunk's start/end time
 * changes then the old plan is freed and a new plan is regenerated
 *
 * NOTE: chunks themselves do not have a cache since they need to be locked for
 * each insert anyway...
 *
 */
typedef struct chunk_crn_set_htable_entry
{
	int32		chunk_id;
	int64		start_time;
	int64		end_time;
	crn_set    *crns;
} chunk_crn_set_htable_entry;

typedef struct ChunkCacheQueryCtx
{
	CacheQueryCtx cctx;
	int32       chunk_id;
	int64       chunk_start_time;
	int64       chunk_end_time;
} ChunkCacheQueryCtx;

static void *
chunk_crn_set_cache_get_key(CacheQueryCtx *ctx)
{
	return &((ChunkCacheQueryCtx *) ctx)->chunk_id;
}

static void *chunk_crn_set_cache_create_entry(Cache *cache, CacheQueryCtx *ctx);
static void *chunk_crn_set_cache_update_entry(Cache *cache, CacheQueryCtx *ctx);

static Cache *chunk_crn_set_cache_create() {
	MemoryContext ctx =  AllocSetContextCreate(CacheMemoryContext,
											   CHUNK_CACHE_INVAL_PROXY_TABLE,
											   ALLOCSET_DEFAULT_SIZES);

	Cache *cache = MemoryContextAlloc(ctx, sizeof(Cache));
	*cache = (Cache)  {
		.hctl = {
			.keysize = sizeof(int32),
			.entrysize = sizeof(chunk_crn_set_htable_entry),
			.hcxt = ctx,
		},
			.name = CHUNK_CACHE_INVAL_PROXY_TABLE,
			.numelements = 16,
			.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
			.get_key = chunk_crn_set_cache_get_key,
			.create_entry = chunk_crn_set_cache_create_entry,
			.update_entry = chunk_crn_set_cache_update_entry,
	};

	cache_init(cache);

	return cache;
} 

static Cache *chunk_crn_set_cache_current = NULL;

static void *
chunk_crn_set_cache_create_entry(Cache *cache, CacheQueryCtx *ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	chunk_crn_set_htable_entry *pe = ctx->entry;
	MemoryContext old;

	pe->chunk_id = cctx->chunk_id;
	pe->start_time = cctx->chunk_start_time;
	pe->end_time = cctx->chunk_end_time;

	/* TODO: old crn leaks memory here... */
	old = cache_switch_to_memory_context(cache);
	pe->crns = fetch_crn_set(NULL, pe->chunk_id);
	MemoryContextSwitchTo(old);

	return pe;
}

static void *
chunk_crn_set_cache_update_entry(Cache *cache, CacheQueryCtx *ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	chunk_crn_set_htable_entry *pe = ctx->entry;
	MemoryContext old;

	if (pe->start_time == cctx->chunk_start_time &&
		pe->end_time == cctx->chunk_end_time)
		return pe;

	old = cache_switch_to_memory_context(cache);
	pe->crns = fetch_crn_set(NULL, pe->chunk_id);
	MemoryContextSwitchTo(old);

	return pe;
}

void
chunk_crn_set_cache_invalidate_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY chunk_crn_set_cache");
	cache_invalidate(chunk_crn_set_cache_current);
	chunk_crn_set_cache_current = chunk_crn_set_cache_create();
}

static chunk_crn_set_htable_entry *
chunk_crn_set_cache_get_entry(Cache *cache, int32 chunk_id, int64 chunk_start_time, int64 chunk_end_time)
{
	if (cache == NULL)
	{
		cache = chunk_crn_set_cache_current;
	}
	ChunkCacheQueryCtx ctx = {
		.chunk_id = chunk_id,
		.chunk_start_time = chunk_start_time,
		.chunk_end_time = chunk_end_time,
	};

	return cache_fetch(cache, &ctx.cctx);
}

extern Cache *
chunk_crn_set_cache_pin() 
{
	return cache_pin(chunk_crn_set_cache_current);
}


static chunk_row *
chunk_row_create(int32 id, int32 partition_id, int64 starttime, int64 endtime)
{
	chunk_row *chunk;

	chunk = palloc(sizeof(chunk_row));
	chunk->id = id;
	chunk->partition_id = partition_id;
	chunk->start_time = starttime;
	chunk->end_time = endtime;

	return chunk;
}

/* Chunk table column numbers */
#define CHUNK_TBL_COL_ID 1
#define CHUNK_TBL_COL_PARTITION_ID 2
#define CHUNK_TBL_COL_STARTTIME 3
#define CHUNK_TBL_COL_ENDTIME 4

/* Chunk partition ID index columns */
#define CHUNK_IDX_COL_PARTITION_ID 1
#define CHUNK_IDX_COL_STARTTIME 2
#define CHUNK_IDX_COL_ENDTIME 3

typedef struct ChunkScanCtx
{
	chunk_row *chunk;
	Oid chunk_tbl_id;
	int32 partition_id;
	int64 starttime, endtime, timepoint;
	bool should_lock;
} ChunkScanCtx;

static bool
chunk_tuple_timepoint_filter(TupleInfo *ti, void *arg)
{
	ChunkScanCtx *ctx = arg;
	bool starttime_is_null, endtime_is_null;
	Datum datum;

	datum = heap_getattr(ti->tuple, CHUNK_TBL_COL_STARTTIME, ti->desc, &starttime_is_null);
	ctx->starttime = starttime_is_null ? OPEN_START_TIME : DatumGetInt64(datum);
	datum = heap_getattr(ti->tuple, CHUNK_TBL_COL_ENDTIME, ti->desc, &endtime_is_null);
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
	bool is_null;
	Datum id;

	id = heap_getattr(ti->tuple, CHUNK_TBL_COL_ID, ti->desc, &is_null);
	ctx->chunk = chunk_row_create(DatumGetInt32(id), ctx->partition_id,
								  ctx->starttime, ctx->endtime);
	return false;
}

static chunk_row *
chunk_scan(int32 partition_id, int64 timepoint, bool tuplock)
{
	ScanKeyData scankey[1];
	Catalog *catalog = catalog_get();
	ChunkScanCtx cctx = {
		.chunk_tbl_id = catalog->tables[CHUNK].id,
		.partition_id = partition_id,
		.timepoint = timepoint,
	};
	ScannerCtx ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = get_relname_relid(CHUNK_PARTITION_TIME_INDEX_NAME, catalog->schema_id),
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &cctx,
		.filter = chunk_tuple_timepoint_filter,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.tuplock = {
			.lockmode = LockTupleShare,
			.enabled = tuplock,
		},
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on epoch ID to find the partitions for the
	 * epoch. */
	ScanKeyInit(&scankey[0], CHUNK_IDX_COL_PARTITION_ID, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(partition_id));

	scanner_scan(&ctx);

	return cctx.chunk;
}

/*
 *	Get chunk cache entry.
 *	The cache parameter is a chunk_crn_set_cache (can be null to use current cache).
 */
chunk_cache_entry *
get_chunk_cache_entry(Cache *cache, Partition *part, int64 timepoint, bool lock)
{
	chunk_crn_set_htable_entry *chunk_crn_cache;
	chunk_cache_entry *entry;
	chunk_row *chunk;

	chunk = chunk_scan(part->id, timepoint, lock);

	if (chunk == NULL)
	{
		chunk = chunk_row_insert_new(part->id, timepoint, lock);
	}

	entry = palloc(sizeof(chunk_cache_entry));
	entry->chunk = chunk;
	entry->id = chunk->id;
	chunk_crn_cache = chunk_crn_set_cache_get_entry(cache, chunk->id,
													chunk->start_time, chunk->end_time);
	entry->crns = chunk_crn_cache->crns;
	return entry;
}

void
_chunk_cache_init(void)
{
	CreateCacheMemoryContext();
	chunk_crn_set_cache_current = chunk_crn_set_cache_create();
}

void
_chunk_cache_fini(void)
{
	cache_invalidate(chunk_crn_set_cache_current);
}
