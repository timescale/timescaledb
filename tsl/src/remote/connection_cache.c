/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "connection_cache.h"
#include <cache.h>

Cache *connection_cache_current = NULL;

typedef struct ConnectionCacheEntry
{
	TSConnectionId id;
	TSConnection *conn;
} ConnectionCacheEntry;

static void
connection_cache_entry_free(void *gen_entry)
{
	ConnectionCacheEntry *entry = gen_entry;

	if (entry->conn != NULL)
	{
		remote_connection_close(entry->conn);
		entry->conn = NULL;
	}
}

static void
connection_cache_pre_destroy_hook(Cache *cache)
{
	HASH_SEQ_STATUS scan;
	ConnectionCacheEntry *entry;

	hash_seq_init(&scan, cache->htab);
	while ((entry = hash_seq_search(&scan)) != NULL)
	{
		elog(DEBUG3, "closing connection %p for option changes to take effect", entry->conn);

		/*
		 * If we don't do this we will have a memory leak because connections
		 * are allocated using malloc
		 */
		connection_cache_entry_free(entry);
	}
}

static bool
connection_cache_valid_result(const void *result)
{
	if (result == NULL)
		return false;
	return ((ConnectionCacheEntry *) result)->conn != NULL;
}

static void *
connection_cache_get_key(CacheQuery *query)
{
	return (TSConnectionId *) query->data;
}

/*
 * Verify that a connection is still valid.
 */
static bool
connection_cache_check_entry(Cache *cache, CacheQuery *query)
{
	ConnectionCacheEntry *entry = query->result;
	/* If this is set, then an async call was aborted. */
	if (remote_connection_is_processing(entry->conn))
		return false;
	return true;
}

static void *
connection_cache_create_entry(Cache *cache, CacheQuery *query)
{
	TSConnectionId *id = (TSConnectionId *) query->data;
	ConnectionCacheEntry *entry = query->result;

	/*
	 * Protects against errors in remote_connection_open, necessary since this
	 * entry is already in hashtable.
	 */
	entry->conn = NULL;

	/*
	 * Note: we do not use the cache memory context here to allocate a PGconn
	 * because PGconn allocation happens using malloc. Which is why calling
	 * remote_connection_close at cleanup is critical.
	 */
	entry->conn = remote_connection_open_by_id(*id);

	/* Since this connection is managed by the cache, it should not auto-close
	 * at the end of the transaction */
	remote_connection_set_autoclose(entry->conn, false);

	return entry;
}

/*
 * This is called when the connection cache entry is found in the cache and
 * before it is returned. The connection can either be bad, in which case it
 * needs to be recreated, or the settings for the local session might have
 * changed since it was last used. In the latter case, we need to configure
 * the remote session again to ensure that it has the same configuration as
 * the local session.
 */
static void *
connection_cache_update_entry(Cache *cache, CacheQuery *query)
{
	ConnectionCacheEntry *entry = query->result;

	if (!connection_cache_check_entry(cache, query))
	{
		remote_connection_close(entry->conn);
		return connection_cache_create_entry(cache, query);
	}

	remote_connection_configure_if_changed(entry->conn);

	return entry;
}

static Cache *
connection_cache_create()
{
	MemoryContext ctx =
		AllocSetContextCreate(CacheMemoryContext, "Connection cache", ALLOCSET_DEFAULT_SIZES);

	Cache *cache = MemoryContextAlloc(ctx, sizeof(Cache));

	*cache = (Cache)
	{
		.hctl = {
			.keysize = sizeof(TSConnectionId),
			.entrysize = sizeof(ConnectionCacheEntry),
			.hcxt = ctx,
		},
		.name = "connection_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.valid_result = connection_cache_valid_result,
		.get_key = connection_cache_get_key,
		.create_entry = connection_cache_create_entry,
		.remove_entry = connection_cache_entry_free,
		.update_entry = connection_cache_update_entry,
		.pre_destroy_hook = connection_cache_pre_destroy_hook,
	};

	ts_cache_init(cache);
	cache->handle_txn_callbacks = false;

	return cache;
}

Cache *
remote_connection_cache_pin()
{
	return ts_cache_pin(connection_cache_current);
}

TSConnection *
remote_connection_cache_get_connection(Cache *cache, TSConnectionId id)
{
	CacheQuery query = { .data = &id };
	ConnectionCacheEntry *entry = ts_cache_fetch(cache, &query);

	return entry->conn;
}

void
remote_connection_cache_remove(Cache *cache, TSConnectionId id)
{
	ts_cache_remove(cache, &id);
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server catalog entry,
 * mark the cache as invalid.
 */
void
remote_connection_cache_invalidate_callback(void)
{
	ts_cache_invalidate(connection_cache_current);
	connection_cache_current = connection_cache_create();
}

void
_remote_connection_cache_init(void)
{
	connection_cache_current = connection_cache_create();
}

void
_remote_connection_cache_fini(void)
{
	ts_cache_invalidate(connection_cache_current);
	connection_cache_current = NULL;
}
