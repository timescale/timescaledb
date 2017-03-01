#include "cache.h"

void
cache_init(Cache *cache)
{
	MemoryContext ctx, old;
	if (cache->store != NULL)
	{
		elog(ERROR, "Cache %s is already initialized", cache->name);
		return;
	}

	ctx  = AllocSetContextCreate(CacheMemoryContext,
								 cache->name,
								 ALLOCSET_DEFAULT_SIZES);
	old  = MemoryContextSwitchTo(ctx);

	cache->store = palloc(sizeof(CacheStorage));
	
	Assert(cache->hctl.hcxt == NULL);
	cache->hctl.hcxt = ctx;
	cache->store->htab = hash_create(cache->name, cache->numelements,
							  &cache->hctl, cache->flags);
	cache->hctl.hcxt = NULL;

	cache->store->mcxt = ctx;
	cache->store->valid = true;
	cache->store->refcount = 0;
	cache->store->destroy_storage_hook = cache->destroy_storage_hook;

	MemoryContextSwitchTo(old);
}

static void 
storage_destroy(CacheStorage *store) {
	store->valid = false;
	if (store->refcount > 0) {
		//will be destroyed later
		return;
	}

	if (store->destroy_storage_hook != NULL)
		store->destroy_storage_hook(store);

	hash_destroy(store->htab);
	MemoryContextDelete(store->mcxt);
}

void
cache_invalidate(Cache *cache)
{
	if (cache->store == NULL)
		return;

	storage_destroy(cache->store);
	cache->store = NULL;

	cache_init(cache); //start new store
}

/* 
 * Pinning storage is needed if any items returned by the cache
 * may need to survive invalidation events (i.e. AcceptInvalidationMessages() may be called).
 *
 * Invalidation messages may be processed on any internal function that takes a lock (e.g. heap_open.
 *
 * Each call to cache_pin_storage MUST BE paired with a call to cache_release_storage.
 *
 */
extern CacheStorage *cache_pin_storage(Cache *cache)
{
	cache->store->refcount++;
	return cache->store;
}
extern void cache_release_storage(CacheStorage *store)
{
	Assert(store->refcount > 0);
	store->refcount--;
	if (!store->valid) {
		storage_destroy(store);
	}
}


MemoryContext
cache_memory_ctx(Cache *cache)
{
	return cache->store->mcxt;
}

MemoryContext
cache_switch_to_memory_context(Cache *cache)
{
	return MemoryContextSwitchTo(cache->store->mcxt);
}

void *
cache_fetch(Cache *cache, CacheQueryCtx *ctx)
{
	bool		found;

	if (cache->store->htab == NULL)
	{
		elog(ERROR, "Hash %s not initialized", cache->name);
	}

	ctx->entry = hash_search(cache->store->htab, cache->get_key(ctx), HASH_ENTER, &found);

	if (!found && cache->create_entry != NULL)
	{
		MemoryContext old = cache_switch_to_memory_context(cache);

		ctx->entry = cache->create_entry(cache, ctx);
		MemoryContextSwitchTo(old);
	}
	else if (found && cache->update_entry != NULL)
	{
		/* Switch memory context here? */
		/* MemoryContext old = cache_switch_to_memory_context(cache); */
		ctx->entry = cache->update_entry(cache, ctx);
		/* MemoryContextSwitchTo(old); */
	}
	return ctx->entry;
}
