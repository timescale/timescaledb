/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TXN_ID_H
#define TIMESCALEDB_TSL_REMOTE_TXN_ID_H

#include <postgres.h>
#include <utils/fmgrprotos.h>

#include "connection.h"

/*
 * This is the data node dist txn id to be used in PREPARE TRANSACTION and friends.
 * From the data node perspective it has to be unique with regard to any concurrent
 * prepared transactions.
 *
 * From the point of view of the access node, given such an id, an access node
 * must be able to decide whether or not the corresponding distributed txn is still
 * in progress or has committed or aborted. Therefore, an id issued by an access node
 * must be unique for each of its connections.
 *
 * Note: a subtle point is that given this identifier we need to tell if the access node's
 * transaction is still ongoing in the resolution logic without consulting the
 * remote_txn table. This is because the remote_txn table is only populated once the txn
 * is committed. Therefore this id contains the acess node's transaction_id directly.
 *
 * The current format is: version;xid;server_id;user_id. Both parts are necessary to
 * guarantee uniqueness from the point of view of the data node. It is also critical to
 * make sure the transaction has completed on the access node.
 *
 * - xid is a unique identifier for the dist txn on the access node.
 *
 * - pair of server_id and user_id dedups the connections made under different
 * TSConnectionId mappings as part of the same access node's distributed txn.
 *
 * Note: When moving to multiple access nodes, we'll need to add a unique prefix for
 * each access node.
 */

typedef struct RemoteTxnId
{
	uint8 version;
	char reserved[3]; /* not currently serialized */
	TransactionId xid;
	TSConnectionId id;
} RemoteTxnId;

extern RemoteTxnId *remote_txn_id_create(TransactionId xid, TSConnectionId id);
extern RemoteTxnId *remote_txn_id_in(const char *gid_string);

extern bool remote_txn_id_matches_prepared_txn(const char *id_string);
extern Datum remote_txn_id_in_pg(PG_FUNCTION_ARGS);
extern const char *remote_txn_id_out(const RemoteTxnId *remote_txn_id);
extern Datum remote_txn_id_out_pg(PG_FUNCTION_ARGS);

extern const char *remote_txn_id_prepare_transaction_sql(RemoteTxnId *);
extern const char *remote_txn_id_commit_prepared_sql(RemoteTxnId *);
extern const char *remote_txn_id_rollback_prepared_sql(RemoteTxnId *);

#endif /* TIMESCALEDB_TSL_REMOTE_TXN_ID_H */
