#include <postgres.h>
#include <access/xact.h>

#include "cache.h"

/* List of pinned caches. A cache occurs once in this list for every pin
 * taken */
static List *pinned_caches = NIL;

void
cache_init(Cache *cache)
{
	if (cache->htab != NULL)
	{
		elog(ERROR, "Cache %s is already initialized", cache->name);
		return;
	}

	/*
	 * The cache object should have been created in its own context so that
	 * cache_destroy can just delete the context to free everything.
	 */
	Assert(MemoryContextContains(cache_memory_ctx(cache), cache));

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
	MemoryContextDelete(cache->hctl.hcxt);
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
 * Pinning is needed if any items returned by the cache may need to survive
 * invalidation events (i.e. AcceptInvalidationMessages() may be called).
 *
 * Invalidation messages may be processed on any internal function that takes a
 * lock (e.g. heap_open).
 *
 * Each call to cache_pin MUST BE paired with a call to cache_release.
 *
 */
extern Cache *
cache_pin(Cache *cache)
{
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);

	pinned_caches = lappend(pinned_caches, cache);
	MemoryContextSwitchTo(old);
	cache->refcount++;
	return cache;
}

extern int
cache_release(Cache *cache)
{
	int			refcount = cache->refcount - 1;

	Assert(cache->refcount > 0);
	cache->refcount--;
	pinned_caches = list_delete_ptr(pinned_caches, cache);
	cache_destroy(cache);

	return refcount;
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
		cache->stats.numelements--;

	return found;
}


/*
 * Transaction end callback that cleans up any pinned caches. This is a
 * safeguard that protects against indefinitely pinned caches (memory leaks)
 * that may occur if a transaction ends (normally or abnormally) while a pin is
 * held. Without this, a cache_pin() call always needs to be paired with a
 * cache_release() call and wrapped in a PG_TRY() block to capture and handle
 * any exceptions that occur.
 *
 * Note that this makes cache_release() optional, although timely
 * cache_release() calls are still encouraged to release memory as early as
 * possible during long-running transactions. PG_TRY() blocks are not needed,
 * however.
 */
static void
cache_xact_end(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			{
				ListCell   *lc;

				/*
				 * release once for every occurence of a cache in the pinned
				 * caches list
				 */
				foreach(lc, pinned_caches)
				{
					Cache	   *cache = lfirst(lc);

					cache->refcount--;
					cache_destroy(cache);
				}
			}
		default:
			break;
	}
	list_free(pinned_caches);
	pinned_caches = NIL;
}


void
_cache_init(void)
{
	RegisterXactCallback(cache_xact_end, NULL);
}

void
_cache_fini(void)
{
	UnregisterXactCallback(cache_xact_end, NULL);
}
