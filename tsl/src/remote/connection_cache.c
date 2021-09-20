/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tupdesc.h>
#include <access/htup_details.h>
#include <postmaster/postmaster.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/guc.h>
#include <utils/acl.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <libpq-fe.h>

#include <remote/connection.h>
#include <cache.h>
#include <compat/compat.h>
#include "connection_cache.h"

static Cache *connection_cache = NULL;

typedef struct ConnectionCacheEntry
{
	TSConnectionId id;
	TSConnection *conn;
	int32 foreign_server_hashvalue; /* Hash of server OID for cache invalidation */
	int32 role_hashvalue;			/* Hash of role OID for cache invalidation */
	bool invalidated;
} ConnectionCacheEntry;

static void
connection_cache_entry_free(void *gen_entry)
{
	ConnectionCacheEntry *entry = gen_entry;

	if (entry->conn != NULL)
	{
		/* Cannot directly read the Log_connections boolean since it is not
		 * exported on Windows */
		const char *log_conns = GetConfigOption("log_connections", true, false);

		if (NULL != log_conns && strcmp(log_conns, "on") == 0)
		{
			/* Log the connection closing if requested. Only log the
			 * associated user ID since cannot lookup the user name here due
			 * to, potentially, not being in a proper transaction state where
			 * it is possible to use the syscache. */
			elog(LOG,
				 "closing cached connection to \"%s\" [UserId: %d]",
				 remote_connection_node_name(entry->conn),
				 entry->id.user_id);
		}

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
 * Check if a connection needs to be remade.
 *
 * A connection can be in a bad state, in which case we need to
 * fail. Otherwise, the connection could be invalidated and needs to be remade
 * to apply new options. But a connection can only be remade if we are not
 * currently processing a transaction on the connection.
 */
static bool
connection_should_be_remade(const ConnectionCacheEntry *entry)
{
	if (NULL == entry->conn)
		return true;

	if (remote_connection_xact_is_transitioning(entry->conn))
	{
		NameData nodename;

		namestrcpy(&nodename, remote_connection_node_name(entry->conn));

		/* The connection is marked as being in the middle of transaction
		 * state change, which means the transaction was aborted in the middle
		 * of a transition and now we don't know the state of the remote
		 * endpoint. It is not safe to continue on such a connection, so we
		 * remove (and close) the connection and raise an error. */
		remote_connection_cache_remove(entry->id);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("connection to data node \"%s\" was lost", NameStr(nodename))));
	}

	/* Check if the connection has to be refreshed. If processing is set, then
	 * an async call was aborted. The connection could also have been
	 * invalidated, but we only care if we aren't still processing a
	 * transaction. */
	if (remote_connection_get_status(entry->conn) == CONN_PROCESSING ||
		(entry->invalidated && remote_connection_xact_depth_get(entry->conn) == 0))
		return true;

	return false;
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

	/* Set the hash values of the foreign server and role for cache
	 * invalidation purposes */
	entry->foreign_server_hashvalue =
		GetSysCacheHashValue1(FOREIGNSERVEROID, ObjectIdGetDatum(id->server_id));
	entry->role_hashvalue = GetSysCacheHashValue1(AUTHOID, ObjectIdGetDatum(id->user_id));
	entry->invalidated = false;

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
	TSConnectionStatus status;

	if (connection_should_be_remade(entry))
	{
		remote_connection_close(entry->conn);
		return connection_cache_create_entry(cache, query);
	}

	status = remote_connection_get_status(entry->conn);
	Assert(status == CONN_IDLE || status == CONN_COPY_IN);

	if (status == CONN_IDLE)
		remote_connection_configure_if_changed(entry->conn);

	return entry;
}

static Cache *
connection_cache_create(void)
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

TSConnection *
remote_connection_cache_get_connection(TSConnectionId id)
{
	CacheQuery query = { .data = &id };
	ConnectionCacheEntry *entry = ts_cache_fetch(connection_cache, &query);

	return entry->conn;
}

bool
remote_connection_cache_remove(TSConnectionId id)
{
	return ts_cache_remove(connection_cache, &id);
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server catalog entry,
 * mark the cache entry as invalid.
 */
void
remote_connection_cache_invalidate_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnectionCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == AUTHOID);
	hash_seq_init(&scan, connection_cache->htab);

	while ((entry = hash_seq_search(&scan)) != NULL)
	{
		/* hashvalue == 0 means cache reset, so invalidate entire cache */
		if (hashvalue == 0 || (cacheid == AUTHOID && hashvalue == entry->role_hashvalue) ||
			(cacheid == FOREIGNSERVEROID && entry->foreign_server_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}

static bool
is_loopback_host_or_addr(const char *hostaddr)
{
	/* Use strncmp with length to succesfully compare against host address
	 * strings like "127.0.0.1/32" */
	return strcmp("localhost", hostaddr) == 0 || strncmp("127.0.0.1", hostaddr, 9) == 0 ||
		   strncmp("::1", hostaddr, 3) == 0;
}

/*
 * Check if a connection is local.
 *
 * This is an imperfect check, but should work for common cases.
 *
 * It currently doesn't capture being connected on a local network interface
 * address. That would require a platform-independent way to lookup local
 * interface addresses (on UNIX one could use getifaddrs, for instance).
 */
static bool
is_local_connection(const PGconn *conn)
{
	const char *host = PQhost(conn);
	int32 port;

	if (host[0] == '/' /* unix domain socket starts with a slash */)
		return true;

	/* A TCP connection must match both the port and localhost address */
	port = pg_atoi(PQport(conn), sizeof(int32), '\0');

	return (port == PostPortNumber) && is_loopback_host_or_addr(host);
}

/*
 * Remove connections that connect to a local DB, which is being dropped.
 *
 * This function is called when a database is dropped on the local instance
 * and is needed to prevent errors when the database serves as a
 * "same-instance" data node (common in tests). When a data node exists on the
 * local instance (i.e., in a separate local database), we need to close all
 * connections to the data node database in order to drop it. Otherwise, the
 * drop database will fail since the database is being used by the connections
 * in the cache.
 */
void
remote_connection_cache_dropped_db_callback(const char *dbname)
{
	HASH_SEQ_STATUS scan;
	ConnectionCacheEntry *entry;

	hash_seq_init(&scan, connection_cache->htab);

	while ((entry = hash_seq_search(&scan)) != NULL)
	{
		PGconn *pgconn = remote_connection_get_pg_conn(entry->conn);

		/* Remove the connection if it is local and connects to a DB with the
		 * same name as the one being dropped. */
		if (strcmp(dbname, PQdb(pgconn)) == 0 && is_local_connection(pgconn))
			remote_connection_cache_remove(entry->id);
	}
}

/*
 * Remove and close connections that belong to roles that are dropped.
 *
 * Immediately purging such connections should be safe since the DROP command
 * must be executed by different user than the one being dropped.
 */
void
remote_connection_cache_dropped_role_callback(const char *rolename)
{
	HASH_SEQ_STATUS scan;
	ConnectionCacheEntry *entry;
	Oid roleid = get_role_oid(rolename, true);

	if (!OidIsValid(roleid))
		return;

	hash_seq_init(&scan, connection_cache->htab);

	while ((entry = hash_seq_search(&scan)) != NULL)
	{
		if (entry->id.user_id == roleid)
			remote_connection_cache_remove(entry->id);
	}
}
/*
 * Functions and data structures for printing the connection cache.
 */
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
	Anum_show_conn_txn_depth,
	Anum_show_conn_processing,
	Anum_show_conn_invalidated,
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
	[CONNECTION_GSS_STARTUP] = "GSS STARTUP",
#if PG13_GE
	[CONNECTION_CHECK_TARGET] = "CHECK TARGET",
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
	NameData conn_node_name, conn_user_name, conn_db;
	const char *username = GetUserNameFromId(entry->id.user_id, true);

	namestrcpy(&conn_node_name, remote_connection_node_name(entry->conn));

	if (NULL == username)
		pg_snprintf(conn_user_name.data, NAMEDATALEN, "%u", entry->id.user_id);
	else
		namestrcpy(&conn_user_name, username);

	namestrcpy(&conn_db, PQdb(pgconn));

	values[AttrNumberGetAttrOffset(Anum_show_conn_node_name)] = NameGetDatum(&conn_node_name);
	values[AttrNumberGetAttrOffset(Anum_show_conn_user_name)] = NameGetDatum(&conn_user_name);
	values[AttrNumberGetAttrOffset(Anum_show_conn_host)] = CStringGetTextDatum(PQhost(pgconn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_port)] =
		Int32GetDatum(pg_atoi(PQport(pgconn), sizeof(int32), '\0'));
	values[AttrNumberGetAttrOffset(Anum_show_conn_db)] = NameGetDatum(&conn_db);
	values[AttrNumberGetAttrOffset(Anum_show_conn_backend_pid)] =
		Int32GetDatum(PQbackendPID(pgconn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_status)] =
		CStringGetTextDatum(conn_status_str[PQstatus(pgconn)]);
	values[AttrNumberGetAttrOffset(Anum_show_conn_txn_status)] =
		CStringGetTextDatum(conn_txn_status_str[PQtransactionStatus(pgconn)]);
	values[AttrNumberGetAttrOffset(Anum_show_conn_txn_depth)] =
		Int32GetDatum(remote_connection_xact_depth_get(entry->conn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_processing)] =
		BoolGetDatum(remote_connection_is_processing(entry->conn));
	values[AttrNumberGetAttrOffset(Anum_show_conn_invalidated)] = BoolGetDatum(entry->invalidated);

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
		info->cache = ts_cache_pin(connection_cache);
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
	connection_cache = connection_cache_create();
}

void
_remote_connection_cache_fini(void)
{
	ts_cache_invalidate(connection_cache);
	connection_cache = NULL;
}
