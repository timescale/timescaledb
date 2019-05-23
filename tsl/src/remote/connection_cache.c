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
	Oid user_mapping_oid;
	TSConnection *conn;
} ConnectionCacheEntry;

typedef struct ConnectionCacheQuery
{
	CacheQuery q;
	UserMapping *user_mapping;
} ConnectionCacheQuery;

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

static void *
connection_cache_get_key(CacheQuery *query)
{
	return &((ConnectionCacheQuery *) query)->user_mapping->umid;
}

static void *
connection_cache_create_entry(Cache *cache, CacheQuery *query)
{
	ConnectionCacheQuery *q = (ConnectionCacheQuery *) query;
	ConnectionCacheEntry *entry = query->result;
	ForeignServer *server = GetForeignServer(q->user_mapping->serverid);

	/*
	 * protects against errors in remote_connection_open, necessary since this
	 * entry is already in hashtable.
	 */
	entry->conn = NULL;

	/*
	 * Note: we do not use the cache memory context here to allocate a PGconn
	 * because PGconn allocation happens using malloc. Which is why calling
	 * remote_connection_close at cleanup is critical.
	 */
	entry->conn =
		remote_connection_open(server->servername, server->options, q->user_mapping->options, true);
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
		.hctl =
		{
			.keysize = sizeof(Oid),
				.entrysize = sizeof(ConnectionCacheEntry),
				.hcxt = ctx,
		},
			.name = "connection_cache",
			.numelements = 16,
			.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
			.get_key = connection_cache_get_key,
			.create_entry = connection_cache_create_entry,
			.remove_entry = connection_cache_entry_free,
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
remote_connection_cache_get_connection(Cache *cache, UserMapping *user_mapping)
{
	ConnectionCacheQuery query = {
		.user_mapping = user_mapping,
	};
	ConnectionCacheEntry *entry = ts_cache_fetch(cache, &query.q);

	return entry->conn;
}

void
remote_connection_cache_remove(Cache *cache, UserMapping *user_mapping)
{
	ts_cache_remove(cache, &user_mapping->umid);
}

void
remote_connection_cache_remove_by_oid(Cache *cache, Oid user_mapping_oid)
{
	ts_cache_remove(cache, &user_mapping_oid);
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
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
