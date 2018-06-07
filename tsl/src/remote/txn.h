/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TXN_H
#define TIMESCALEDB_TSL_REMOTE_TXN_H
#include <postgres.h>

#include "connection.h"

typedef struct RemoteTxn RemoteTxn;

typedef enum
{
	REMOTE_TXN_NO_PREP_STMT = 0,
	REMOTE_TXN_USE_PREP_STMT,
} RemoteTxnPrepStmtOption;

/* actions */
extern void remote_txn_init(RemoteTxn *entry, PGconn *conn, UserMapping *user);
extern void remote_txn_begin(RemoteTxn *entry, int txnlevel);
extern bool remote_txn_abort(RemoteTxn *entry);
extern void remote_txn_deallocate_prepared_stmts_if_needed(RemoteTxn *entry);
extern bool remote_txn_sub_txn_abort(RemoteTxn *entry, int curlevel);
extern void remote_txn_sub_txn_pre_commit(RemoteTxn *entry, int curlevel);

/* accessors/info */
extern void remote_txn_set_will_prep_statement(RemoteTxn *entry,
											   RemoteTxnPrepStmtOption prep_stmt_option);
extern PGconn *remote_txn_get_connection(RemoteTxn *txn);
extern Oid remote_txn_get_user_mapping_oid(RemoteTxn *txn);
extern size_t remote_txn_size(void);
extern bool remote_txn_is_at_sub_txn_level(RemoteTxn *entry, int curlevel);

/* Messages/communication */
extern AsyncRequest *remote_txn_async_send_commit(RemoteTxn *entry);

#if DEBUG
/* Debugging functions used in testing */
extern void remote_txn_check_for_leaked_prepared_statements(RemoteTxn *entry);
#endif

#endif /* TIMESCALEDB_TSL_REMOTE_TXN_H */
