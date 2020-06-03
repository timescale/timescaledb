/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tupdesc.h>
#include <access/htup_details.h>
#include <remote/connection.h>
#include <utils/builtins.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>

#include <cache.h>
#include <compat.h>
#include "connection_cache.h"

static Cache *connection_cache_current = NULL;

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

bool
remote_connection_cache_remove(Cache *cache, TSConnectionId id)
{
	return ts_cache_remove(cache, &id);
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

enum Anum_show_conn
{
	Anum_show_conn_node_name = 1,
	Anum_show_conn_user_name,
	Anum_show_conn_host,
	Anum_show_conn_port,
	Anum_show_conn_db,
	Anum_show_conn_backend_pid,
	Anum_show_conn_status,
	Anum_show_conn_txn_status,
	Anum_show_conn_processing,
	_Anum_show_conn_max,
};

#define Natts_show_conn (_Anum_show_conn_max - 1)

static const char *conn_status_str[] = {
	[CONNECTION_OK] = "OK",
	[CONNECTION_BAD] = "BAD",
	[CONNECTION_STARTED] = "STARTED",
	[CONNECTION_MADE] = "MADE",
	[CONNECTION_AWAITING_RESPONSE] = "AWAITING RESPONSE",
	[CONNECTION_AUTH_OK] = "AUTH_OK",
	[CONNECTION_SETENV] = "SETENV",
	[CONNECTION_SSL_STARTUP] = "SSL STARTUP",
	[CONNECTION_NEEDED] = "CONNECTION NEEDED",
	[CONNECTION_CHECK_WRITABLE] = "CHECK WRITABLE",
	[CONNECTION_CONSUME] = "CONSUME",
#if PG12_GE
	[CONNECTION_GSS_STARTUP] = "GSS STARTUP",
#endif
};

static const char *conn_txn_status_str[] = {
	[PQTRANS_IDLE] = "IDLE",	   [PQTRANS_ACTIVE] = "ACTIVE",   [PQTRANS_INTRANS] = "INTRANS",
	[PQTRANS_INERROR] = "INERROR", [PQTRANS_UNKNOWN] = "UNKNOWN",
};

static HeapTuple
create_tuple_from_conn_entry(const ConnectionCacheEntry *entry, const TupleDesc tupdesc)
{
	Datum values[Natts_show_conn];
	bool nulls[Natts_show_conn] = { false };
	PGconn *pgconn = remote_connection_get_pg_conn(entry->conn);

	values[AttrNumberGetAttrOffset(Anum_show_conn_node_name)] =
		CStringGetDatum(remote_connection_node_name(entry->conn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_user_name)] =
		CStringGetDatum(GetUserNameFromId(entry->id.user_id, false));
	values[AttrNumberGetAttrOffset(Anum_show_conn_host)] = CStringGetTextDatum(PQhost(pgconn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_port)] =
		Int32GetDatum(pg_atoi(PQport(pgconn), sizeof(int32), '\0'));
	values[AttrNumberGetAttrOffset(Anum_show_conn_db)] = CStringGetDatum(PQdb(pgconn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_backend_pid)] =
		Int32GetDatum(PQbackendPID(pgconn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_status)] =
		CStringGetTextDatum(conn_status_str[PQstatus(pgconn)]);
	values[AttrNumberGetAttrOffset(Anum_show_conn_txn_status)] =
		CStringGetTextDatum(conn_txn_status_str[PQtransactionStatus(pgconn)]);
	values[AttrNumberGetAttrOffset(Anum_show_conn_processing)] =
		BoolGetDatum(remote_connection_is_processing(entry->conn));

	return heap_form_tuple(tupdesc, values, nulls);
}

typedef struct ConnCacheShowState
{
	HASH_SEQ_STATUS scan;
	Cache *cache;
} ConnCacheShowState;

Datum
remote_connection_cache_show(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ConnCacheShowState *info;
	const ConnectionCacheEntry *entry;
	HeapTuple tuple;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		info = palloc0(sizeof(ConnCacheShowState));
		info->cache = remote_connection_cache_pin();
		hash_seq_init(&info->scan, info->cache->htab);
		funcctx->user_fctx = info;
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;

	entry = hash_seq_search(&info->scan);

	if (entry == NULL)
	{
		ts_cache_release(info->cache);
		SRF_RETURN_DONE(funcctx);
	}

	tuple = create_tuple_from_conn_entry(entry, funcctx->tuple_desc);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
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
