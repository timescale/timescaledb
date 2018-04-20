#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>

#include "chunk_cache.h"
#include "chunk.h"
#include "catalog.h"
#include "cache.h"
#include "utils.h"

static void *chunk_cache_create_entry(Cache *cache, CacheQuery *query);

typedef struct ChunkCacheQuery
{
	CacheQuery	q;
	Oid			relid;
	int16		num_constraints;
	const char *schema;
	const char *table;
} ChunkCacheQuery;

static void *
chunk_cache_get_key(CacheQuery *query)
{
	return &((ChunkCacheQuery *) query)->relid;
}

typedef struct ChunkCacheEntry
{
	Oid			relid;
	Chunk	   *chunk;
} ChunkCacheEntry;


static Cache *
chunk_cache_create()
{
	MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext,
											  "Chunk cache",
											  ALLOCSET_DEFAULT_SIZES);

	Cache	   *cache = MemoryContextAlloc(ctx, sizeof(Cache));
	Cache		template =
	{
		.hctl =
		{
			.keysize = sizeof(Oid),
			.entrysize = sizeof(ChunkCacheEntry),
			.hcxt = ctx,
		},
		.name = "chunk_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = chunk_cache_get_key,
		.create_entry = chunk_cache_create_entry,
	};

	*cache = template;

	cache_init(cache);

	return cache;
}

static Cache *chunk_cache_current = NULL;

static void *
chunk_cache_create_entry(Cache *cache, CacheQuery *query)
{
	ChunkCacheQuery *q = (ChunkCacheQuery *) query;
	ChunkCacheEntry *cache_entry = query->result;

	if (NULL == q->schema)
		q->schema = get_namespace_name(get_rel_namespace(q->relid));

	if (NULL == q->table)
		q->table = get_rel_name(q->relid);

	cache_entry->chunk = chunk_get_by_name_with_memory_context(q->schema,
															   q->table,
															   q->num_constraints,
															   cache_memory_ctx(cache),
															   false);

	return query->result;
}

void
chunk_cache_invalidate_callback(void)
{
	cache_invalidate(chunk_cache_current);
	chunk_cache_current = chunk_cache_create();
}

Chunk *
chunk_cache_get_by_name(Cache *cache, const char *schema, const char *table, int16 num_constraints)
{
	Oid			nsoid = get_namespace_oid(schema, true);
	ChunkCacheQuery query = {
		.relid = get_relname_relid(table, nsoid),
		.schema = schema,
		.table = table,
		.num_constraints = num_constraints,
	};
	ChunkCacheEntry *entry;

	if (!OidIsValid(nsoid) || !OidIsValid(query.relid))
		return NULL;

	entry = cache_fetch(cache, &query.q);

	return entry->chunk;
}

static Chunk *
chunk_cache_get_by_relid(Cache *cache, Oid relid, int16 num_constraints)
{
	ChunkCacheQuery query = {
		.relid = relid,
		.schema = get_namespace_name(get_rel_namespace(relid)),
		.table = get_rel_name(relid),
		.num_constraints = num_constraints,
	};

	ChunkCacheEntry *entry = cache_fetch(cache, &query.q);

	return entry->chunk;
}

/*
 * Get a chunk from the cache.
 */
Chunk *
chunk_cache_get(Cache *cache, Oid relid, int16 num_constraints)
{
	if (!OidIsValid(relid))
		return NULL;

	return chunk_cache_get_by_relid(cache, relid, num_constraints);
}

Chunk *
chunk_cache_get_or_add(Cache *cache, Chunk *chunk)
{
	ChunkCacheQuery query = {
		.q.enteronly = true,
		.relid = chunk->table_id,
	};
	ChunkCacheEntry *entry = cache_fetch(cache, &query.q);

	if (!OidIsValid(entry->relid))
	{
		MemoryContext old = MemoryContextSwitchTo(cache->hctl.hcxt);

		entry->relid = query.relid;
		entry->chunk = chunk_copy(chunk);
		MemoryContextSwitchTo(old);
	}

	return entry->chunk;
}

Chunk *
chunk_cache_get_entry_rv(Cache *cache, RangeVar *rv, int16 num_constraints)
{
	return chunk_cache_get(cache, RangeVarGetRelid(rv, NoLock, true), num_constraints);
}

extern Cache *
chunk_cache_pin()
{
	return cache_pin(chunk_cache_current);
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
