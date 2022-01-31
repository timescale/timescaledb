/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "libpq-fe.h"
#include <postgres.h>
#include <access/xact.h>
#include <storage/procarray.h>
#include <utils/builtins.h>
#include <utils/snapmgr.h>
#include <libpq-fe.h>
#include <miscadmin.h>

#include "remote/async.h"
#include "remote/txn_store.h"
#include "txn.h"
#include "connection.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "txn_id.h"

/* This seemingly long timeout matches what postgres_fdw uses. */
#define DEFAULT_EXEC_CLEANUP_TIMEOUT_MS 30000

/*
 * This RemoteTxn represents one remote end in a distributed txn.
 * Thus, a distributed txn is made up of a collection remote txn.
 * Each remote txn corresponds to one remote connection and there
 * is a unique remote connection per TSConnectionId used in the
 * distributed txn. Because of this uniqueness property,
 * the connection id appears first in the object, to allow
 * it to be a hash key.
 *
 * The "conn" pointer can be NULL if we don't currently have a live connection.
 * When we do have a connection, xact_depth tracks the current depth of
 * transactions and subtransactions open on the remote side.  We need to issue
 * commands at the same nesting depth on the remote as we're executing at
 * ourselves, so that rolling back a subtransaction will kill the right
 * queries and not the wrong ones.
 */

typedef struct RemoteTxn
{
	TSConnectionId id;  /* hash key (must be first) */
	TSConnection *conn; /* connection to data node, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	bool have_prep_stmt;	/* have we prepared any stmts in this xact? */
	bool have_subtxn_error; /* have any subxacts aborted in this xact? */
	RemoteTxnId *remote_txn_id;
} RemoteTxn;

/*
 * Start remote transaction or subtransaction, if it hasn't been
 * already started (e.g. by a previous command in the same txn).
 *
 * We always use at least REPEATABLE READ in the remote session.
 * This is important even for cases where we use the a single connection to
 * a data node. This is because a single command from the access node may cause
 * multiple remote commands to be executed (e.g. a join of two tables on one remote
 * node might not be pushed down and instead two different queries are sent
 * to the remote node, one for each table in the join). Since in READ
 * COMMITED the snapshot is refreshed on each command, the semantics are off
 * when multiple commands are meant to be part of the same one.
 *
 * This isn't great but we have no alternative unless we ensure that each access
 * node command always translates to one data node query or if we had some other way to
 * control which remote queries share a snapshot or when a snapshot is refreshed.
 *
 * NOTE: this does not guarantee any kind of snapshot isolation to different connections
 * to the same data node. That only happens if we use multiple connection ids to the same data node
 * in one access node transaction. Thus, such connections that use different users will potentially
 * see inconsistent results. To solve this problem of inconsistent results, we could export the
 * snapshot of the first connection to a remote node using pg_export_snapshot() and then use that
 * using SET TRANSACTION SNAPSHOT xxxx across all other connections to that node during the
 * transaction. However, given that we currently don't have snapshot isolation across different
 * nodes, we don't want to commit to the overhead of exporting snapshots at this time.
 */
void
remote_txn_begin(RemoteTxn *entry, int curlevel)
{
	int xact_depth = remote_connection_xact_depth_get(entry->conn);

	/* Start main transaction if we haven't yet */
	if (xact_depth == 0)
	{
		StringInfoData sql;
		char *xactReadOnly;

		Assert(remote_connection_get_status(entry->conn) == CONN_IDLE);
		elog(DEBUG3, "starting remote transaction on connection %p", entry->conn);

		initStringInfo(&sql);
		appendStringInfo(&sql, "%s", "START TRANSACTION ISOLATION LEVEL");
		if (IsolationIsSerializable())
			appendStringInfo(&sql, "%s", " SERIALIZABLE");
		else
			appendStringInfo(&sql, "%s", " REPEATABLE READ");

		/*
		 * Windows MSVC builds have linking issues for GUC variables from postgres for
		 * use inside this extension. So we use GetConfigOptionByName
		 */
		xactReadOnly = GetConfigOptionByName("transaction_read_only", NULL, false);

		/*
		 * If we are initiating connection from a standby (of an AN for example),
		 * then the remote connection transaction needs to be also set up as a
		 * READ ONLY one. This will catch any commands that are sent from the
		 * read only AN to datanodes but which could have potential read-write
		 * side effects on data nodes.
		 *
		 * Note that when the STANDBY gets promoted then the ongoing transaction
		 * will remain READ ONLY till its completion. New transactions will be
		 * suitably READ WRITE. This is a slight change in behavior as compared to
		 * regular Postgres, but promotion is not a routine activity, so it should
		 * be acceptable and typically users would be reconnecting to the new
		 * promoted AN anyways.
		 *
		 * Note that the below will also handle the case when primary AN has a
		 * transaction which does an explicit "BEGIN TRANSACTION READ ONLY;". The
		 * treatment is the same, mark the remote DN transaction as READ ONLY
		 */
		if (strncmp(xactReadOnly, "on", sizeof("on")) == 0)
			appendStringInfo(&sql, "%s", " READ ONLY");

		remote_connection_xact_transition_begin(entry->conn);
		remote_connection_cmd_ok(entry->conn, sql.data);
		remote_connection_xact_transition_end(entry->conn);
		xact_depth = remote_connection_xact_depth_inc(entry->conn);
		pfree(sql.data);
	}
	/* If the connection is in COPY mode, then exit out of it */
	else if (remote_connection_get_status(entry->conn) == CONN_COPY_IN)
	{
		TSConnectionError err;

		if (!remote_connection_end_copy(entry->conn, &err))
			remote_connection_error_elog(&err, ERROR);
	}

	/*
	 * If we're in a subtransaction, stack up savepoints to match our level.
	 * This ensures we can rollback just the desired effects when a
	 * subtransaction aborts.
	 */
	while (xact_depth < curlevel)
	{
		remote_connection_xact_transition_begin(entry->conn);
		remote_connection_cmdf_ok(entry->conn, "SAVEPOINT s%d", xact_depth + 1);
		remote_connection_xact_transition_end(entry->conn);
		xact_depth = remote_connection_xact_depth_inc(entry->conn);
	}
}

/*
 * Check if the access node transaction which is driving the 2PC on the datanodes is
 * still in progress.
 */
bool
remote_txn_is_still_in_progress_on_access_node(TransactionId access_node_xid)
{
	if (TransactionIdIsCurrentTransactionId(access_node_xid))
		elog(ERROR, "checking if a commit is still in progress on same txn");

	return TransactionIdIsInProgress(access_node_xid);
}

size_t
remote_txn_size()
{
	return sizeof(RemoteTxn);
}

void
remote_txn_init(RemoteTxn *entry, TSConnection *conn)
{
	Assert(NULL != conn);
	Assert(remote_connection_xact_depth_get(conn) == 0);

	/* Reset all transient state fields, to be sure all are clean */
	entry->have_prep_stmt = false;
	entry->have_subtxn_error = false;
	entry->remote_txn_id = NULL;

	/* Now try to make the connection */
	/* in connection  */
	entry->conn = conn;

	elog(DEBUG3,
		 "new connection %p for data node \"%s\" (server "
		 "oid %u, userid %u)",
		 entry->conn,
		 remote_connection_node_name(conn),
		 entry->id.server_id,
		 entry->id.user_id);
}

RemoteTxn *
remote_txn_begin_on_connection(TSConnection *conn)
{
	RemoteTxn *txn = palloc0(sizeof(RemoteTxn));

	remote_txn_init(txn, conn);
	remote_txn_begin(txn, GetCurrentTransactionNestLevel());

	return txn;
}

void
remote_txn_set_will_prep_statement(RemoteTxn *entry, RemoteTxnPrepStmtOption prep_stmt_option)
{
	bool will_prep_stmt = (prep_stmt_option == REMOTE_TXN_USE_PREP_STMT);

	entry->have_prep_stmt |= will_prep_stmt;
}

TSConnection *
remote_txn_get_connection(RemoteTxn *txn)
{
	return txn->conn;
}

TSConnectionId
remote_txn_get_connection_id(RemoteTxn *txn)
{
	return txn->id;
}

void
remote_txn_report_prepare_transaction_result(RemoteTxn *txn, bool success)
{
	if (!success)
		txn->remote_txn_id = NULL;
}

/*
 * This function submits commands to remote nodes during (sub)abort processing.
 * Because remote nodes can be in a weird state and at the same time errors should
 * not be thrown here, the processing here is a bit different.
 *
 * We submit a query during and wait up to 30 seconds for the result. All errors
 * are reported as WARNINGS into the log.
 *
 * If the query is executed without error, the return value is true.
 * If the query can't be sent, errors out, or times out, the return value is false.
 */
static bool
exec_cleanup_command(TSConnection *conn, const char *query)
{
	TimestampTz end_time;
	AsyncRequest *req;
	AsyncResponseResult *result;
	AsyncResponse *response = NULL;
	PGresult *pg_result;
	bool success = false;

	/*
	 * If it takes too long to execute a cleanup query, assume the connection
	 * is dead.  It's fairly likely that this is why we aborted in the first
	 * place (e.g. statement timeout, user cancel), so the timeout shouldn't
	 * be too long.
	 */
	end_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), DEFAULT_EXEC_CLEANUP_TIMEOUT_MS);

	/*
	 * Send the query. Since we don't use non-blocking mode, this also can
	 * block. But its risk is relatively small, so we ignore that for now.
	 */
	req = async_request_send_with_error(conn, query, WARNING);

	if (req == NULL)
		return false;

	/* Wait until the command completes or there is a timeout or error */
	response = async_request_cleanup_result(req, end_time);
	Assert(response != NULL);

	switch (async_response_get_type(response))
	{
		case RESPONSE_TIMEOUT:
			elog(DEBUG3, "abort processing: timeout executing %s", query);
			success = false;
			break;
		case RESPONSE_COMMUNICATION_ERROR:
			elog(DEBUG3, "abort processing: communication error executing %s", query);
			success = false;
			break;
		case RESPONSE_ERROR:
			elog(DEBUG3, "abort processing: error while executing %s", query);
			success = false;
			break;
		case RESPONSE_RESULT:
			result = (AsyncResponseResult *) response;
			pg_result = async_response_result_get_pg_result(result);
			if (PQresultStatus(pg_result) != PGRES_COMMAND_OK)
			{
				elog(DEBUG3, "abort processing: error in result executing %s", query);
				success = false;
			}
			else
				success = true;
			break;
		case RESPONSE_ROW:
			elog(DEBUG3,
				 "abort processing: unexpected response type %d while executing %s",
				 async_response_get_type(response),
				 query);
			success = false;
			break;
	}

	if (!success)
		async_response_report_error(response, WARNING);

	async_response_close(response);

	return success;
}

#ifdef DEBUG
/* Prepared statements can leak if the were created during a subtxn
 * and the subtxn rolled back before the prepared stmt was deallocated.
 * This function checks for such leaks inside of tests (thus only compiled
 * in DEBUG mode). It can be quite expensive so not run under normal operations.
 */
void
remote_txn_check_for_leaked_prepared_statements(RemoteTxn *entry)
{
	PGresult *res;
	char *count_string;
	ExecStatusType status;

	if (PQTRANS_IDLE != PQtransactionStatus(remote_connection_get_pg_conn(entry->conn)))
		return;

	res = remote_connection_exec(entry->conn, "SELECT count(*) FROM pg_prepared_statements");

	status = PQresultStatus(res);

	switch (status)
	{
		case PGRES_TUPLES_OK:
			if (PQntuples(res) == 1 && PQnfields(res) == 1)
			{
				count_string = PQgetvalue(res, 0, 0);
				if (strcmp("0", count_string) != 0)
					elog(WARNING, "leak check: connection leaked prepared statement");
			}
			else
				elog(ERROR, "leak check: unexpected number of rows or columns returned");
			break;
		case PGRES_FATAL_ERROR:
		case PGRES_NONFATAL_ERROR:
			elog(WARNING, "leak check: ERROR [\"%s\"]", PQresultErrorMessage(res));
			break;
		default:
			elog(WARNING, "leak check: unexpected result state %u", status);
			break;
	}

	remote_result_close(res);
}
#endif

bool
remote_txn_abort(RemoteTxn *entry)
{
	const char *abort_sql;
	bool success = true;

	if (entry->remote_txn_id == NULL)
	{
		/* Rollback a regular (non two-phase commit) transaction */
		abort_sql = "ROLLBACK TRANSACTION";
	}
	else
	{
		/* Rollback a transaction prepared for two-phase commit (PREPARE
		 * TRANSACTION) */
		abort_sql = remote_txn_id_rollback_prepared_sql(entry->remote_txn_id);
	}

	entry->remote_txn_id = NULL;

	Assert(entry->conn != NULL);
	Assert(remote_connection_xact_depth_get(entry->conn) > 0);

	elog(DEBUG3, "aborting remote transaction on connection %p", entry->conn);

	/* Already in bad state */
	if (remote_connection_xact_is_transitioning(entry->conn))
		return false;
	else if (in_error_recursion_trouble() ||
			 PQstatus(remote_connection_get_pg_conn(entry->conn)) == CONNECTION_BAD)
	{
		/*
		 * Don't try to recover the connection if we're already in error
		 * recursion trouble or the connection is bad. Instead, mark it as a
		 * failed transition. This is a really bad case and so controlled
		 * cleanup cannot happen here. The calling function will instead break
		 * this ongoing connection and so no cleanup is necessary.
		 */
		remote_connection_xact_transition_begin(entry->conn);
		return false;
	}

	/* Mark the connection as transitioning to new transaction state */
	remote_connection_xact_transition_begin(entry->conn);

	/*
	 * Check if a command has been submitted to the data node by using an
	 * asynchronous execution function and the command had not yet completed.
	 * If so, request cancellation of the command.
	 */
	if (PQtransactionStatus(remote_connection_get_pg_conn(entry->conn)) == PQTRANS_ACTIVE)
		success = remote_connection_cancel_query(entry->conn);

	if (success)
	{
		/* At this point any on going queries should have completed */
		remote_connection_set_status(entry->conn, CONN_IDLE);
		success = exec_cleanup_command(entry->conn, abort_sql);
	}

	/*
	 * Assume we might may have not deallocated all the prepared statements we
	 * created because the deallocation would have happened after the abort.
	 *
	 * prepared stmts are per session not per transaction. But we don't want
	 * prepared_stmts to survive transactions in our use case.
	 */
	if (success && entry->have_prep_stmt)
		success = exec_cleanup_command(entry->conn, "DEALLOCATE ALL");

	if (success)
	{
		entry->have_prep_stmt = false;
		entry->have_subtxn_error = false;

		/* Everything succeeded, so we have finished transitioning */
		remote_connection_xact_transition_end(entry->conn);
	}

	return success;
}

/* Check if there is ongoing transaction on the remote node */
bool
remote_txn_is_ongoing(RemoteTxn *entry)
{
	Assert(remote_connection_xact_depth_get(entry->conn) >= 0);
	return remote_connection_xact_depth_get(entry->conn) > 0;
}

/*
 * If there were any errors in subtransactions, and we made prepared
 * statements, those prepared statements may not have been cleared
 * because of the subtxn error. Thus, do a DEALLOCATE ALL to make sure
 * we get rid of all prepared statements.
 *
 * This is annoying and not terribly bulletproof, but it's
 * probably not worth trying harder.
 */
void
remote_txn_deallocate_prepared_stmts_if_needed(RemoteTxn *entry)
{
	Assert(entry->conn != NULL && remote_connection_xact_depth_get(entry->conn) > 0);

	if (entry->have_prep_stmt && entry->have_subtxn_error)
	{
		AsyncRequestSet *set = async_request_set_create();
		AsyncResponse *response;

		async_request_set_add(set, async_request_send(entry->conn, "DEALLOCATE ALL"));
		response = async_request_set_wait_any_response(set);
		async_response_report_error_or_close(response, WARNING);
		response = async_request_set_wait_any_response(set);
		Assert(response == NULL);
	}
	entry->have_prep_stmt = false;
	entry->have_subtxn_error = false;
}

/*
 * Ensure state changes are marked successful when a remote transaction
 * completes asynchronously and successfully.
 *
 * We do this in a callback which is guaranteed to be called when a reponse is
 * received or a timeout occurs.
 *
 * There is no decision on whether to fail or not in this callback; this is
 * only to guarantee that we're always updating the internal connection
 * state. Someone still has to handle the responses elsewehere.
 */
static bool
on_remote_txn_response(AsyncRequest *req, AsyncResponse *rsp)
{
	TSConnection *conn = async_request_get_connection(req);
	bool success = false;

	if (async_response_get_type(rsp) == RESPONSE_RESULT)
	{
		AsyncResponseResult *res = (AsyncResponseResult *) rsp;
		PGresult *pgres = async_response_result_get_pg_result(res);

		if (PQresultStatus(pgres) == PGRES_COMMAND_OK)
		{
			remote_connection_xact_transition_end(conn);
			success = true;
		}
	}

	return success;
}

static void
on_commit_or_commit_prepared_response(AsyncRequest *req, AsyncResponse *rsp, void *data)
{
	on_remote_txn_response(req, rsp);
}

AsyncRequest *
remote_txn_async_send_commit(RemoteTxn *entry)
{
	AsyncRequest *req;

	Assert(entry->conn != NULL);
	Assert(remote_connection_xact_depth_get(entry->conn) > 0);

	elog(DEBUG3, "committing remote transaction on connection %p", entry->conn);

	remote_connection_xact_transition_begin(entry->conn);
	req = async_request_send(entry->conn, "COMMIT TRANSACTION");
	async_request_set_response_callback(req, on_commit_or_commit_prepared_response, entry);

	return req;
}

void
remote_txn_write_persistent_record(RemoteTxn *entry)
{
	entry->remote_txn_id = remote_txn_persistent_record_write(entry->id);
}

static void
on_prepare_transaction_response(AsyncRequest *req, AsyncResponse *rsp, void *data)
{
	bool success = on_remote_txn_response(req, rsp);

	if (!success)
	{
		RemoteTxn *txn = data;

		/* If the prepare is not successful, reset the remote transaction ID
		 * to indicate we need to do a rollback */
		txn->remote_txn_id = NULL;
	}
}

AsyncRequest *
remote_txn_async_send_prepare_transaction(RemoteTxn *entry)
{
	AsyncRequest *req;

	Assert(entry->conn != NULL);
	Assert(remote_connection_xact_depth_get(entry->conn) > 0);
	Assert(entry->remote_txn_id != NULL);

	elog(DEBUG3,
		 "2pc: preparing remote transaction on connection %p: %s",
		 entry->conn,
		 remote_txn_id_out(entry->remote_txn_id));

	remote_connection_xact_transition_begin(entry->conn);
	req = async_request_send(entry->conn,
							 remote_txn_id_prepare_transaction_sql(entry->remote_txn_id));
	async_request_set_response_callback(req, on_prepare_transaction_response, entry);

	return req;
}

AsyncRequest *
remote_txn_async_send_commit_prepared(RemoteTxn *entry)
{
	AsyncRequest *req;

	Assert(entry->conn != NULL);
	Assert(entry->remote_txn_id != NULL);

	elog(DEBUG3,
		 "2pc: commiting remote transaction on connection %p: '%s'",
		 entry->conn,
		 remote_txn_id_out(entry->remote_txn_id));

	remote_connection_xact_transition_begin(entry->conn);

	req = async_request_send_with_error(entry->conn,
										remote_txn_id_commit_prepared_sql(entry->remote_txn_id),
										WARNING);
	async_request_set_response_callback(req, on_commit_or_commit_prepared_response, entry);

	return req;
}

/*
 * Rollback a subtransaction to a given savepoint.
 */
bool
remote_txn_sub_txn_abort(RemoteTxn *entry, int curlevel)
{
	PGconn *pg_conn = remote_connection_get_pg_conn(entry->conn);
	bool success = false;

	Assert(remote_connection_xact_depth_get(entry->conn) == curlevel);
	Assert(remote_connection_xact_depth_get(entry->conn) > 1);

	if (in_error_recursion_trouble() && remote_connection_xact_is_transitioning(entry->conn))
		remote_connection_xact_transition_begin(entry->conn);

	if (!remote_connection_xact_is_transitioning(entry->conn))
	{
		StringInfoData sql;

		initStringInfo(&sql);
		entry->have_subtxn_error = true;
		remote_connection_xact_transition_begin(entry->conn);

		/*
		 * If a command has been submitted to the data node by using an
		 * asynchronous execution function, the command might not have yet
		 * completed. Check to see if a command is still being processed by the
		 * data node, and if so, request cancellation of the command.
		 */
		if (PQtransactionStatus(pg_conn) == PQTRANS_ACTIVE &&
			!remote_connection_cancel_query(entry->conn))
			success = false;
		else
		{
			/* Rollback all remote subtransactions during abort */
			appendStringInfo(&sql, "ROLLBACK TO SAVEPOINT s%d", curlevel);
			success = exec_cleanup_command(entry->conn, sql.data);

			if (success)
			{
				resetStringInfo(&sql);
				appendStringInfo(&sql, "RELEASE SAVEPOINT s%d", curlevel);
				success = exec_cleanup_command(entry->conn, sql.data);
			}
		}

		if (success)
			remote_connection_xact_transition_end(entry->conn);
	}

	Assert(remote_connection_xact_depth_get(entry->conn) > 0);

	return success;
}

bool
remote_txn_is_at_sub_txn_level(RemoteTxn *entry, int curlevel)
{
	int xact_depth;

	/*
	 * We only care about connections with open remote subtransactions of the
	 * current level.
	 */
	Assert(entry->conn != NULL);

	xact_depth = remote_connection_xact_depth_get(entry->conn);

	if (xact_depth < curlevel)
		return false;

	if (xact_depth > curlevel)
		elog(ERROR, "missed cleaning up remote subtransaction at level %d", xact_depth);

	Assert(xact_depth == curlevel);

	return true;
}

void
remote_txn_sub_txn_pre_commit(RemoteTxn *entry, int curlevel)
{
	Assert(remote_connection_xact_depth_get(entry->conn) == curlevel);
	Assert(remote_connection_xact_depth_get(entry->conn) > 0);
	Assert(!remote_connection_xact_is_transitioning(entry->conn));

	remote_connection_xact_transition_begin(entry->conn);
	remote_connection_cmdf_ok(entry->conn, "RELEASE SAVEPOINT s%d", curlevel);
	remote_connection_xact_transition_end(entry->conn);
}

/*
 * Functions for storing a persistent transaction records for two-phase
 * commit.
 */
static int
persistent_record_pkey_scan(const RemoteTxnId *id, tuple_found_func tuple_found, LOCKMODE lock_mode)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog->tables[REMOTE_TXN].id,
		.index = catalog_get_index(catalog, REMOTE_TXN, REMOTE_TXN_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.lockmode = lock_mode,
		.limit = 1,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_remote_txn_pkey_idx_remote_transaction_id,
				BTEqualStrategyNumber,
				F_TEXTEQ,
				CStringGetTextDatum(remote_txn_id_out(id)));

	return ts_scanner_scan(&scanctx);
}

bool
remote_txn_persistent_record_exists(const RemoteTxnId *parsed)
{
	return persistent_record_pkey_scan(parsed, NULL, AccessShareLock) > 0;
}

static ScanTupleResult
persistent_record_tuple_delete(TupleInfo *ti, void *data)
{
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	return SCAN_CONTINUE;
}

/* If gid is NULL, then delete all entries belonging to the provided datanode.  */
int
remote_txn_persistent_record_delete_for_data_node(Oid foreign_server_oid, const char *gid)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx;
	int scanidx;
	ForeignServer *server = GetForeignServer(foreign_server_oid);

	if (gid == NULL)
	{
		ScanKeyInit(&scankey[0],
					Anum_remote_txn_data_node_name_idx_data_node_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					CStringGetDatum(server->servername));
		scanidx = REMOTE_TXN_DATA_NODE_NAME_IDX;
	}
	else
	{
		ScanKeyInit(&scankey[0],
					Anum_remote_txn_pkey_idx_remote_transaction_id,
					BTEqualStrategyNumber,
					F_TEXTEQ,
					CStringGetTextDatum(gid));
		scanidx = REMOTE_TXN_PKEY_IDX;
	}

	scanctx = (ScannerCtx){
		.table = catalog->tables[REMOTE_TXN].id,
		.index = catalog_get_index(catalog, REMOTE_TXN, scanidx),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = persistent_record_tuple_delete,
		.lockmode = RowExclusiveLock,
		.snapshot = GetTransactionSnapshot(),
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan(&scanctx);
}

static void
persistent_record_insert_relation(Relation rel, RemoteTxnId *id)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_remote_txn];
	bool nulls[Natts_remote_txn] = { false };
	CatalogSecurityContext sec_ctx;
	ForeignServer *server = GetForeignServer(id->id.server_id);

	values[AttrNumberGetAttrOffset(Anum_remote_txn_data_node_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(server->servername));
	values[AttrNumberGetAttrOffset(Anum_remote_txn_remote_transaction_id)] =
		CStringGetTextDatum(remote_txn_id_out(id));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

/*
 * Add a commit record to catalog.
 */
RemoteTxnId *
remote_txn_persistent_record_write(TSConnectionId cid)
{
	RemoteTxnId *id = remote_txn_id_create(GetTopTransactionId(), cid);
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = table_open(catalog->tables[REMOTE_TXN].id, RowExclusiveLock);
	persistent_record_insert_relation(rel, id);

	/* Keep the table lock until transaction completes in order to
	 * synchronize with distributed restore point creation */
	table_close(rel, NoLock);
	return id;
}
