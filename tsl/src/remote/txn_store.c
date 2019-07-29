/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/hsearch.h>

#include "txn_store.h"
#include "txn.h"
#include "connection.h"

#include "connection_cache.h"

#define DEFAULT_NUM_ITEMS 100

RemoteTxnStore *
remote_txn_store_create(MemoryContext mctx)
{
	HASHCTL ctl;
	RemoteTxnStore *store = MemoryContextAlloc(mctx, sizeof(RemoteTxnStore));

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TSConnectionId);
	ctl.entrysize = remote_txn_size();
	ctl.hcxt = mctx;
	*store = (RemoteTxnStore){
		.hashtable = hash_create("RemoteTxnStore",
								 DEFAULT_NUM_ITEMS,
								 &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT),
		.mctx = mctx,
		.cache = remote_connection_cache_pin(),
	};
	return store;
}

RemoteTxn *
remote_txn_store_get(RemoteTxnStore *store, TSConnectionId id, bool *found_out)
{
	bool found;
	RemoteTxn *entry;

	entry = hash_search(store->hashtable, &id, HASH_ENTER, &found);

	if (!found)
	{
		TSConnection *conn;
		PGconn *pg_conn;

		PG_TRY();
		{
			conn = remote_connection_cache_get_connection(store->cache, id);
			Assert(conn != NULL);
			pg_conn = remote_connection_get_pg_conn(conn);

			if (PQstatus(pg_conn) != CONNECTION_OK || PQtransactionStatus(pg_conn) != PQTRANS_IDLE)
			{
				/*
				 * Cached connection is sick. A previous transaction may have
				 * encountered an error that didn't remove the connection from
				 * the cache. Instead of trying to remove the sick connection
				 * on error, check on first use (here) and restart the
				 * connection if sick.
				 */
				remote_connection_cache_remove(store->cache, id);
				conn = remote_connection_cache_get_connection(store->cache, id);
			}
		}
		PG_CATCH();
		{
			remote_txn_store_remove(store, id);
			PG_RE_THROW();
		}
		PG_END_TRY();
		remote_txn_init(entry, conn);
	}
	if (found_out != NULL)
		*found_out = found;
	return entry;
}

void
remote_txn_store_remove(RemoteTxnStore *store, TSConnectionId id)
{
	bool found;

	hash_search(store->hashtable, &id, HASH_REMOVE, &found);
	Assert(found);
	remote_connection_cache_remove(store->cache, id);
}

void
remote_txn_store_destroy(RemoteTxnStore *store)
{
#if DEBUG
	RemoteTxn *txn;
	remote_txn_store_foreach(store, txn) { remote_txn_check_for_leaked_prepared_statements(txn); }
#endif
	hash_destroy(store->hashtable);
	store->hashtable = NULL;
	ts_cache_release(store->cache);
	store->cache = NULL;
}
