#include "cache.h"

void
cache_init(Cache *cache)
{
	if (cache->htab != NULL)
	{
		elog(ERROR, "Cache %s is already initialized", cache->name);
		return;
	}

	if (cache->hctl.hcxt == NULL)
	{
		cache->hctl.hcxt = AllocSetContextCreate(CacheMemoryContext,
												 cache->name,
												 ALLOCSET_DEFAULT_SIZES);
	}

	cache->htab = hash_create(cache->name, cache->numelements,
							  &cache->hctl, cache->flags);
}

void
cache_invalidate(Cache *cache)
{
	if (cache->htab == NULL)
		return;

	if (cache->pre_invalidate_hook != NULL)
		cache->pre_invalidate_hook(cache);

	hash_destroy(cache->htab);
	cache->htab = NULL;
	MemoryContextDelete(cache->hctl.hcxt);
	cache->hctl.hcxt = NULL;

	if (cache->post_invalidate_hook != NULL)
		cache->post_invalidate_hook(cache);
}

MemoryContext
cache_memory_ctx(Cache *cache)
{
	return cache->hctl.hcxt;
}

MemoryContext
cache_switch_to_memory_context(Cache *cache)
{
	return MemoryContextSwitchTo(cache->hctl.hcxt);
}

void *
cache_fetch(Cache *cache, CacheQueryCtx *ctx)
{
	bool		found;

	if (cache->htab == NULL)
	{
		elog(ERROR, "Hash %s not initialized", cache->name);
	}

	ctx->entry = hash_search(cache->htab, cache->get_key(ctx), HASH_ENTER, &found);

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
