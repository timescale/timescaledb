/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TXN_H
#define TIMESCALEDB_TSL_REMOTE_TXN_H

#include <postgres.h>
#include <access/xact.h>

#include "cache.h"
#include "connection.h"
#include "txn_id.h"

typedef struct RemoteTxn RemoteTxn;

typedef enum
{
	REMOTE_TXN_NO_PREP_STMT = 0,
	REMOTE_TXN_USE_PREP_STMT,
} RemoteTxnPrepStmtOption;

/* actions */
extern void remote_txn_init(RemoteTxn *entry, TSConnection *conn);
extern RemoteTxn *remote_txn_begin_on_connection(TSConnection *conn);
extern void remote_txn_begin(RemoteTxn *entry, int txnlevel);
extern bool remote_txn_abort(RemoteTxn *entry);
extern void remote_txn_write_persistent_record(RemoteTxn *entry);
extern void remote_txn_deallocate_prepared_stmts_if_needed(RemoteTxn *entry);
extern bool remote_txn_sub_txn_abort(RemoteTxn *entry, int curlevel);
extern void remote_txn_sub_txn_pre_commit(RemoteTxn *entry, int curlevel);

/* accessors/info */
extern void remote_txn_set_will_prep_statement(RemoteTxn *entry,
											   RemoteTxnPrepStmtOption prep_stmt_option);
extern TSConnection *remote_txn_get_connection(RemoteTxn *txn);
extern TSConnectionId remote_txn_get_connection_id(RemoteTxn *txn);
extern bool remote_txn_is_still_in_progress_on_access_node(TransactionId access_node_xid);
extern size_t remote_txn_size(void);
extern bool remote_txn_is_at_sub_txn_level(RemoteTxn *entry, int curlevel);
extern bool remote_txn_is_ongoing(RemoteTxn *entry);

/* Messages/communication */
extern AsyncRequest *remote_txn_async_send_commit(RemoteTxn *entry);
extern AsyncRequest *remote_txn_async_send_prepare_transaction(RemoteTxn *entry);
extern AsyncRequest *remote_txn_async_send_commit_prepared(RemoteTxn *entry);
extern void remote_txn_report_prepare_transaction_result(RemoteTxn *txn, bool success);

/* Persitent record */
extern RemoteTxnId *remote_txn_persistent_record_write(TSConnectionId id);
extern bool remote_txn_persistent_record_exists(const RemoteTxnId *gid);
extern int remote_txn_persistent_record_delete_for_data_node(Oid foreign_server_oid,
															 const char *gid);

#ifdef DEBUG
/* Debugging functions used in testing */
extern void remote_txn_check_for_leaked_prepared_statements(RemoteTxn *entry);
#endif

#endif /* TIMESCALEDB_TSL_REMOTE_TXN_H */
