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
	};
	return store;
}

RemoteTxn *
remote_txn_store_get(RemoteTxnStore *store, TSConnectionId id, bool *found_out)
{
	bool found;
	RemoteTxn *entry;

	entry = hash_search(store->hashtable, &id, HASH_ENTER, &found);

	PG_TRY();
	{
		TSConnection *conn;

		/* Get a connection from the connection cache. We do this even for
		 * existing remote transactions because it will run checks on connections
		 * to ensure they're in a good state. We validate below that connections
		 * aren't remade for existing transactions. Always getting a connection
		 * from the cache avoids having to redo the same checks here and we can
		 * keep connection validation in one place. */
		conn = remote_connection_cache_get_connection(id);

		if (found)
		{
			/* For existing transactions, we'd expect to continue on the same
			 * connection */
			if (remote_txn_get_connection(entry) != conn)
				elog(ERROR,
					 "unexpected connection state for remote transaction on node \"%s\"",
					 remote_connection_node_name(conn));
		}
		else
			remote_txn_init(entry, conn);
	}
	PG_CATCH();
	{
		remote_txn_store_remove(store, id);
		PG_RE_THROW();
	}
	PG_END_TRY();

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
	remote_connection_cache_remove(id);
}

void
remote_txn_store_destroy(RemoteTxnStore *store)
{
#ifdef DEBUG
	RemoteTxn *txn;
	remote_txn_store_foreach(store, txn) { remote_txn_check_for_leaked_prepared_statements(txn); }
#endif
	hash_destroy(store->hashtable);
	store->hashtable = NULL;
}
