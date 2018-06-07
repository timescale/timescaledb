/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TXN_ID_H
#define TIMESCALEDB_TSL_REMOTE_TXN_ID_H

#include <postgres.h>
#if !PG96
#include <utils/fmgrprotos.h>
#endif

/* This is the datanode dist txn id to be used in PREPARE TRANSACTION and friends.
   From the datanode perspective it has to be unique with regard to any concurrent
   prepared transactions.

   From the point of view of the frontend, given such an id and the server it came from,
   a frontend must be able to decide whether or not the corresponding distributed txn
   is still in progress or has committed or aborted.

   Note: a subtle point is that given this identifier we need to tell if the
   frontend transaction is still ongoing in the resolution logic without
   consulting the remote_txn table. This is because the
   remote_txn table is only populated once the txn is committed.
   Therefore this id contains the frontend's transaction_id directly.

   The current format is: frontend_id;xid;user_mapping_oid
   All three parts are necessary to guarantee uniqueness from the point of view of the data node.
	- reserved is to give a unique prefix to the id (e.g. for a future frontend_id).
	- xid is a unique identifier for the dist txn on the frontend. It is also critical to to make
   sure the transaction has completed on the frontend node.
	- user_mapping_oid dedups the connections made under different user mappings as part of the same
   frontend distributed txn.
	*/

typedef struct RemoteTxnId
{
	uint8 version;
	char reserved[3]; /* not currently serialized */
	TransactionId xid;
	Oid user_mapping;
} RemoteTxnId;

extern RemoteTxnId *remote_txn_id_create(TransactionId xid, Oid user_mapping);
extern RemoteTxnId *remote_txn_id_in(const char *gid_string);
extern Datum remote_txn_id_in_pg(PG_FUNCTION_ARGS);
extern const char *remote_txn_id_out(const RemoteTxnId *remote_txn_id);
extern Datum remote_txn_id_out_pg(PG_FUNCTION_ARGS);

extern const char *remote_txn_id_prepare_transaction_sql(RemoteTxnId *);
extern const char *remote_txn_id_commit_prepared_sql(RemoteTxnId *);
extern const char *remote_txn_id_rollback_prepared_sql(RemoteTxnId *);

#endif /* TIMESCALEDB_TSL_REMOTE_TXN_ID_H */
