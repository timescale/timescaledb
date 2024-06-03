/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <storage/ipc.h>

#include "compat/compat.h"
#include "cache.h"

/* List of pinned caches. A cache occurs once in this list for every pin
 * taken */
static List *pinned_caches = NIL;
static MemoryContext pinned_caches_mctx = NULL;

typedef struct CachePin
{
	Cache *cache;
	SubTransactionId subtxnid;
} CachePin;

static void
cache_reset_pinned_caches(void)
{
	if (NULL != pinned_caches_mctx)
		MemoryContextDelete(pinned_caches_mctx);

	pinned_caches_mctx =
		AllocSetContextCreate(CacheMemoryContext, "Cache pins", ALLOCSET_DEFAULT_SIZES);

	pinned_caches = NIL;
}

void
ts_cache_init(Cache *cache)
{
	if (cache->htab != NULL)
	{
		elog(ERROR, "cache %s is already initialized", cache->name);
		return;
	}

	/*
	 * The cache object should have been created in its own context so that
	 * cache_destroy can just delete the context to free everything.
	 */
	Assert(GetMemoryChunkContext(cache) == ts_cache_memory_ctx(cache));

	/*
	 * We always want to be explicit about the memory context our hash table
	 * ends up in to ensure it's not accidentally put in TopMemoryContext.
	 */
	Assert(cache->flags & HASH_CONTEXT);
	cache->htab = hash_create(cache->name, cache->numelements, &cache->hctl, cache->flags);
	cache->refcount = 1;
	cache->handle_txn_callbacks = true;
	cache->release_on_commit = true;
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
ts_cache_invalidate(Cache *cache)
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
 * lock (e.g. table_open).
 *
 * Each call to cache_pin MUST BE paired with a call to cache_release.
 *
 */
Cache *
ts_cache_pin(Cache *cache)
{
	MemoryContext old = MemoryContextSwitchTo(pinned_caches_mctx);
	CachePin *cp = palloc(sizeof(CachePin));

	cp->cache = cache;
	cp->subtxnid = GetCurrentSubTransactionId();
	if (cache->handle_txn_callbacks)
		pinned_caches = lappend(pinned_caches, cp);
	MemoryContextSwitchTo(old);
	cache->refcount++;
	return cache;
}

static void
remove_pin(Cache *cache, SubTransactionId subtxnid)
{
	ListCell *lc;

	foreach (lc, pinned_caches)
	{
		CachePin *cp = lfirst(lc);

		if (cp->cache == cache && cp->subtxnid == subtxnid)
		{
			pinned_caches = list_delete_cell(pinned_caches, lc);
			pfree(cp);
			return;
		}
	}

	/* should never reach here: there should always be a pin to remove */
	Assert(false);
}

static int
cache_release_subtxn(Cache *cache, SubTransactionId subtxnid)
{
	int refcount = cache->refcount - 1;

	Assert(cache->refcount > 0);
	cache->refcount--;

	if (cache->handle_txn_callbacks)
		remove_pin(cache, subtxnid);
	cache_destroy(cache);

	return refcount;
}

int
ts_cache_release(Cache *cache)
{
	return cache_release_subtxn(cache, GetCurrentSubTransactionId());
}

MemoryContext
ts_cache_memory_ctx(Cache *cache)
{
	return cache->hctl.hcxt;
}

void *
ts_cache_fetch(Cache *cache, CacheQuery *query)
{
	HASHACTION action;
	bool found;

	if (cache->htab == NULL || cache->valid_result == NULL)
		elog(ERROR, "cache \"%s\" is not initialized", cache->name);

	if (query->flags & CACHE_FLAG_NOCREATE)
		action = HASH_FIND;
	else if (cache->create_entry == NULL)
		elog(ERROR, "cache \"%s\" does not support creating new entries", cache->name);
	else
		action = HASH_ENTER;

	query->result = hash_search(cache->htab, cache->get_key(query), action, &found);

	if (found)
	{
		cache->stats.hits++;

		if (cache->update_entry != NULL)
			query->result = cache->update_entry(cache, query);
	}
	else
	{
		cache->stats.misses++;

		if (action == HASH_ENTER)
		{
			cache->stats.numelements++;
			query->result = cache->create_entry(cache, query);
		}
	}

	if (!(query->flags & CACHE_FLAG_MISSING_OK) && !cache->valid_result(query->result))
	{
		if (cache->missing_error != NULL)
			cache->missing_error(cache, query);
		else
			elog(ERROR, "failed to find entry in cache \"%s\"", cache->name);
	}

	return query->result;
}

static void
release_all_pinned_caches()
{
	ListCell *lc;

	/*
	 * release once for every occurrence of a cache in the pinned caches list.
	 * On abort, release irrespective of cache->release_on_commit.
	 */
	foreach (lc, pinned_caches)
	{
		CachePin *cp = lfirst(lc);

		cp->cache->refcount--;
		cache_destroy(cp->cache);
	}

	cache_reset_pinned_caches();
}

static void
release_subtxn_pinned_caches(SubTransactionId subtxnid, bool abort)
{
	ListCell *lc;

	/* Need a copy because cache_release will modify pinned_caches */
	List *pinned_caches_copy = list_copy(pinned_caches);

	/* Only release caches created in subtxn */
	foreach (lc, pinned_caches_copy)
	{
		CachePin *cp = lfirst(lc);

		if (cp->subtxnid == subtxnid)
		{
			/*
			 * This assert makes sure that that we don't have a cache leak
			 * when running with debugging
			 */
			Assert(abort);
			cache_release_subtxn(cp->cache, subtxnid);
		}
	}

	list_free(pinned_caches_copy);
}

/*
 * Transaction end callback that cleans up any pinned caches. This is a
 * safeguard that protects against indefinitely pinned caches (memory leaks)
 * that may occur if a transaction ends (normally or abnormally) while a pin is
 * held. Without this, a ts_cache_pin() call always needs to be paired with a
 * ts_cache_release() call and wrapped in a PG_TRY() block to capture and handle
 * any exceptions that occur.
 *
 * Note that this checks that ts_cache_release() is always called by the end
 * of a non-aborted transaction unless cache->release_on_commit is set to true.
 * */
static void
cache_xact_end(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			release_all_pinned_caches();
			break;
		default:
		{
			/*
			 * Make a copy of the list of pinned caches since
			 * ts_cache_release() can manipulate the original list.
			 */
			List *pinned_caches_copy = list_copy(pinned_caches);
			ListCell *lc;

			/*
			 * Only caches left should be marked as non-released
			 */
			foreach (lc, pinned_caches_copy)
			{
				CachePin *cp = lfirst(lc);

				/*
				 * This assert makes sure that that we don't have a cache
				 * leak when running with debugging
				 */
				Assert(!cp->cache->release_on_commit);

				/*
				 * This may still happen in optimized environments where
				 * Assert is turned off. In that case, release.
				 */
				if (cp->cache->release_on_commit)
					ts_cache_release(cp->cache);
			}

			list_free(pinned_caches_copy);
		}
		break;
	}
}

static void
cache_subxact_abort(SubXactEvent event, SubTransactionId subtxn_id, SubTransactionId parentSubid,
					void *arg)
{
	/*
	 * Note that cache->release_on_commit is irrelevant here since can't have
	 * cross-commit operations in subtxns
	 */
	/*
	 * In subtxns, caches should have already been released, unless an abort
	 * happened. Be careful to only release caches that were created in the
	 * same subtxn.
	 */

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
		case SUBXACT_EVENT_PRE_COMMIT_SUB:
			/* do nothing */
			break;
		case SUBXACT_EVENT_COMMIT_SUB:
			release_subtxn_pinned_caches(subtxn_id, false);
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			release_subtxn_pinned_caches(subtxn_id, true);
			break;
	}
}

void
_cache_init(void)
{
	cache_reset_pinned_caches();
	RegisterXactCallback(cache_xact_end, NULL);
	RegisterSubXactCallback(cache_subxact_abort, NULL);
}

void
_cache_fini(void)
{
	release_all_pinned_caches();
	MemoryContextDelete(pinned_caches_mctx);
	pinned_caches_mctx = NULL;
	pinned_caches = NIL;
	UnregisterXactCallback(cache_xact_end, NULL);
	UnregisterSubXactCallback(cache_subxact_abort, NULL);
}
