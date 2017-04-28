#include "cache.h"

void
cache_init(Cache *cache)
{
	if (cache->htab != NULL)
	{
		elog(ERROR, "Cache %s is already initialized", cache->name);
		return;
	}

	cache->htab = hash_create(cache->name, cache->numelements,
							  &cache->hctl, cache->flags);
	cache->refcount = 1;
}

static void
cache_destroy(Cache *cache)
{
	if (cache->refcount > 0)
	{
		/* will be destroyed later */
		return;
	}

	if (cache->pre_destroy_hook != NULL)
		cache->pre_destroy_hook(cache);

	hash_destroy(cache->htab);
	cache->htab = NULL;
	MemoryContextDelete(cache->hctl.hcxt);
	cache->hctl.hcxt = NULL;
}

void
cache_invalidate(Cache *cache)
{
	if (cache == NULL)
		return;
	cache->refcount--;
	cache_destroy(cache);
}

/*
 * Pinning is needed if any items returned by the cache
 * may need to survive invalidation events (i.e. AcceptInvalidationMessages() may be called).
 *
 * Invalidation messages may be processed on any internal function that takes a lock (e.g. heap_open).
 *
 * Each call to cache_pin MUST BE paired with a call to cache_release.
 *
 */
extern Cache *
cache_pin(Cache *cache)
{
	cache->refcount++;
	return cache;
}

extern void
cache_release(Cache *cache)
{
	Assert(cache->refcount > 0);
	cache->refcount--;
	cache_destroy(cache);
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
cache_fetch(Cache *cache, CacheQuery *query)
{
	bool		found;
	HASHACTION	action = cache->create_entry == NULL ? HASH_FIND : HASH_ENTER;

	if (cache->htab == NULL)
	{
		elog(ERROR, "Hash %s not initialized", cache->name);
	}

	query->result = hash_search(cache->htab, cache->get_key(query), action, &found);

	if (found)
	{
		cache->stats.hits++;

		if (cache->update_entry != NULL)
		{
			MemoryContext old = cache_switch_to_memory_context(cache);

			query->result = cache->update_entry(cache, query);
			MemoryContextSwitchTo(old);
		}
	}
	else
	{
		cache->stats.misses++;

		if (cache->create_entry != NULL)
		{
			MemoryContext old = cache_switch_to_memory_context(cache);

			query->result = cache->create_entry(cache, query);
			MemoryContextSwitchTo(old);
			cache->stats.numelements++;
		}
	}

	return query->result;
}

bool
cache_remove(Cache *cache, void *key)
{
	bool		found;

	hash_search(cache->htab, key, HASH_REMOVE, &found);

	if (found)
	{
		cache->stats.numelements--;
	}
	return found;
}
