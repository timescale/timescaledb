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

typedef struct ChunkCacheQueryCtx
{
	CacheQueryCtx cctx;
	Chunk	   *stub_chunk;
}	ChunkCacheQueryCtx;


static void *
chunk_cache_get_key(CacheQueryCtx * ctx)
{
	return &((ChunkCacheQueryCtx *) ctx)->stub_chunk->id;
}

/* Cache entry for chunk replicas */

static void *chunk_cache_create_entry(Cache * cache, CacheQueryCtx * ctx);
static void *chunk_cache_update_entry(Cache * cache, CacheQueryCtx * ctx);

static Cache *
chunk_cache_create()
{
	MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext,
											  CHUNK_CACHE_INVAL_PROXY_TABLE,
											  ALLOCSET_DEFAULT_SIZES);

	Cache	   *cache = MemoryContextAlloc(ctx, sizeof(Cache));

	Cache		template = {
		.hctl =
		{
			.keysize = sizeof(int32),
			.entrysize = sizeof(Chunk),
			.hcxt = ctx,
		},
		.name = CHUNK_CACHE_INVAL_PROXY_TABLE,
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = chunk_cache_get_key,
		.create_entry = chunk_cache_create_entry,
		.update_entry = chunk_cache_update_entry,
	};

	*cache = template;

	cache_init(cache);

	return cache;
}


typedef struct ReplicaScanCtx
{
	ChunkReplica *replicas;
	int			num_replicas;
}	ReplicaScanCtx;

static bool
chunk_replica_tuple_found(TupleInfo * ti, void *arg)
{
	ReplicaScanCtx *ctx = arg;
	ChunkReplica *cr;
	Datum		values[Natts_chunk_replica_node];
	bool		isnull[Natts_chunk_replica_node];

	cr = &ctx->replicas[--ctx->num_replicas];

	heap_deform_tuple(ti->tuple, ti->desc, values, isnull);

	strncpy(cr->database_name,
	DatumGetCString(DATUM_GET(values, Anum_chunk_replica_node_database_name)),
			NAMEDATALEN);
	strncpy(cr->schema_name,
	 DatumGetCString(DATUM_GET(values, Anum_chunk_replica_node_schema_name)),
			NAMEDATALEN);
	strncpy(cr->table_name,
	  DatumGetCString(DATUM_GET(values, Anum_chunk_replica_node_table_name)),
			NAMEDATALEN);

	cr->schema_id = get_namespace_oid(cr->schema_name, false);
	cr->table_id = get_relname_relid(cr->table_name, cr->schema_id);

	if (ctx->num_replicas == 0)
		return false;

	return true;
}

static ChunkReplica *
chunk_replica_scan(int32 chunk_id, int num_replicas)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	ReplicaScanCtx cctx = {
		.num_replicas = num_replicas,
		.replicas = palloc(sizeof(ChunkReplica) * num_replicas),
	};
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK_REPLICA_NODE].id,
		.index = catalog->tables[CHUNK_REPLICA_NODE].index_ids[CHUNK_REPLICA_NODE_ID_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &cctx,
		.tuple_found = chunk_replica_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on the chunk ID to find all replicas.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_replica_node_pkey_idx_chunk_id, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(chunk_id));

	scanner_scan(&ctx);

	return cctx.replicas;
}

static void *
chunk_cache_create_entry(Cache * cache, CacheQueryCtx * ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	Chunk	   *chunk = ctx->entry;
	MemoryContext old;

	old = cache_switch_to_memory_context(cache);

	chunk->id = cctx->stub_chunk->id;
	chunk->start_time = cctx->stub_chunk->start_time;
	chunk->end_time = cctx->stub_chunk->end_time;
	chunk->num_replicas = cctx->stub_chunk->num_replicas;
	chunk->replicas = chunk_replica_scan(chunk->id, chunk->num_replicas);

	MemoryContextSwitchTo(old);

	return chunk;
}


static void *
chunk_cache_update_entry(Cache * cache, CacheQueryCtx * ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	Chunk	   *chunk = ctx->entry;
	MemoryContext old;

	if (chunk->start_time == cctx->stub_chunk->start_time &&
		chunk->end_time == cctx->stub_chunk->end_time &&
		chunk->replicas != NULL)
	{
		return chunk;
	}

	old = cache_switch_to_memory_context(cache);

	chunk->id = cctx->stub_chunk->id;
	chunk->start_time = cctx->stub_chunk->start_time;
	chunk->end_time = cctx->stub_chunk->end_time;
	chunk->num_replicas = cctx->stub_chunk->num_replicas;

	if (chunk->replicas != NULL && chunk->num_replicas != cctx->stub_chunk->num_replicas)
	{
		pfree(chunk->replicas);
		chunk->replicas = chunk_replica_scan(chunk->id, chunk->num_replicas);
	}

	MemoryContextSwitchTo(old);

	return chunk;
}

void
chunk_cache_invalidate_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY chunk cache");
	cache_invalidate(chunk_cache_current);
	chunk_cache_current = chunk_cache_create();
}

static Chunk *
chunk_cache_get_from_stub(Cache * cache, Chunk * stub_chunk)
{
	ChunkCacheQueryCtx ctx = {
		.stub_chunk = stub_chunk,
	};

	if (cache == NULL)
	{
		cache = chunk_cache_current;
	}

	return cache_fetch(cache, &ctx.cctx);
}

extern Cache *
chunk_cache_pin()
{
	return cache_pin(chunk_cache_current);
}

typedef struct ChunkScanCtx
{
	Chunk	   *chunk;
	Oid			chunk_tbl_id;
	int32		partition_id;
	int64		starttime,
				endtime,
				timepoint;
	int16		num_replicas;
	bool		should_lock;
}	ChunkScanCtx;

static bool
chunk_tuple_timepoint_filter(TupleInfo * ti, void *arg)
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
chunk_tuple_found(TupleInfo * ti, void *arg)
{
	ChunkScanCtx *ctx = arg;
	bool		is_null;
	Datum		id;

	id = heap_getattr(ti->tuple, Anum_chunk_id, ti->desc, &is_null);
	ctx->chunk = chunk_create(DatumGetInt32(id), ctx->partition_id,
							ctx->starttime, ctx->endtime, ctx->num_replicas);
	return false;
}

static Chunk *
chunk_scan(int32 partition_id, int64 timepoint, bool tuplock)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	ChunkScanCtx cctx = {
		.chunk_tbl_id = catalog->tables[CHUNK].id,
		.partition_id = partition_id,
		.timepoint = timepoint,
	};
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[CHUNK_PARTITION_TIME_INDEX],
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

	/*
	 * Perform an index scan on epoch ID to find the partitions for the epoch.
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_partition_start_time_end_time_idx_partition_id,
				BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(partition_id));

	scanner_scan(&ctx);

	return cctx.chunk;
}

/*
 *	Get chunk cache entry.
 */
Chunk *
chunk_cache_get(Cache * cache, Partition * part, int16 num_replicas, int64 timepoint, bool lock)
{
	Chunk	   *stub_chunk;

	/*
	 * Scan for a chunk or create and insert a new one if missing. The
	 * returned chunk will not have replica information and requires a cache
	 * lookup (and potential scan of replica tables in case of cache miss).
	 * The chunk will ultimately be stored in the cache, which provides its
	 * own storage for chunk, which will replace this stub.
	 */
	stub_chunk = chunk_scan(part->id, timepoint, lock);

	if (stub_chunk == NULL)
	{
		stub_chunk = chunk_insert_new(part->id, timepoint, lock);
	}

	stub_chunk->num_replicas = num_replicas;

	return chunk_cache_get_from_stub(cache, stub_chunk);
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
