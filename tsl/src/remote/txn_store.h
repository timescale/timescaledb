/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_USER_MAPPING_STORE_H
#define TIMESCALEDB_TSL_USER_MAPPING_STORE_H

#include <postgres.h>
#include <utils/hsearch.h>

#include "connection.h"
#include "txn.h"
#include "cache.h"

/* Maps a UserMapping to a RemoteTxn. Used by the distributed txn to store the remote txns
 * associated with a distributed txn. Note that this forces a distributed txn to contain a single
 * RemoteTxn per UserMapping. This is actually required to maintain a consistent snapshot for each
 * local user on a per-remote-server basis. */
typedef struct RemoteTxnStore
{
	HTAB *hashtable;
	MemoryContext mctx;
	HASH_SEQ_STATUS scan;

	Cache *cache;
} RemoteTxnStore;

extern RemoteTxnStore *remote_txn_store_create(MemoryContext mctx);
extern RemoteTxn *remote_txn_store_get(RemoteTxnStore *store, UserMapping *user_mapping,
									   bool *found);
extern void remote_txn_store_remove(RemoteTxnStore *store, Oid user_mapping_oid);
extern void remote_txn_store_destroy(RemoteTxnStore *store);

/* iterators */
#define remote_txn_store_foreach(store, remote_txn)                                                \
	for (hash_seq_init(&store->scan, store->hashtable);                                            \
		 NULL != (remote_txn = (RemoteTxn *) hash_seq_search(&store->scan));)

#define remote_txn_store_foreach_break(store) (hash_seq_term(&ums->scan); break)

#endif /* TIMESCALEDB_TSL_USER_MAPPING_STORE_H */
