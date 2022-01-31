/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/xlog.h>
#include <storage/lmgr.h>
#include <utils/hsearch.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include "dist_txn.h"
#include "ts_catalog/catalog.h"
#include "connection.h"
#include "async.h"
#include "errors.h"
#include "txn.h"
#include "txn_store.h"
#include "guc.h"

#ifdef DEBUG

static const DistTransactionEventHandler *event_handler = NULL;
static const char *eventnames[MAX_DTXN_EVENT] = {
	[DTXN_EVENT_ANY] = "any",
	[DTXN_EVENT_PRE_COMMIT] = "pre-commit",
	[DTXN_EVENT_WAIT_COMMIT] = "waiting-commit",
	[DTXN_EVENT_PRE_ABORT] = "pre-abort",
	[DTXN_EVENT_PRE_PREPARE] = "pre-prepare-transaction",
	[DTXN_EVENT_WAIT_PREPARE] = "waiting-prepare-transaction",
	[DTXN_EVENT_POST_PREPARE] = "post-prepare-transaction",
	[DTXN_EVENT_PRE_COMMIT_PREPARED] = "pre-commit-prepared",
	[DTXN_EVENT_WAIT_COMMIT_PREPARED] = "waiting-commit-prepared",
	[DTXN_EVENT_SUB_XACT_ABORT] = "subxact-abort",
};

void
remote_dist_txn_set_event_handler(const DistTransactionEventHandler *handler)
{
	event_handler = handler;
}

static inline void
eventcallback(const DistTransactionEvent event)
{
	if (NULL != event_handler && NULL != event_handler->handler)
		event_handler->handler(event, event_handler->data);
}

DistTransactionEvent
remote_dist_txn_event_from_name(const char *eventname)
{
	int i;

	for (i = 0; i < MAX_DTXN_EVENT; i++)
	{
		if (strcmp(eventname, eventnames[i]) == 0)
			return i;
	}

	elog(ERROR, "invalid event name");
	pg_unreachable();
}

const char *
remote_dist_txn_event_name(const DistTransactionEvent event)
{
	return eventnames[event];
}

#else
#define eventcallback(event)                                                                       \
	do                                                                                             \
	{                                                                                              \
	} while (0)
#endif

static RemoteTxnStore *store = NULL;

/*
 * Get a connection which can be used to execute queries on the remote PostgreSQL
 * data node with the user's authorization.  A new connection is established
 * if we don't already have a suitable one, and a transaction is opened at
 * the right subtransaction nesting depth if we didn't do that already.
 *
 * will_prep_stmt must be true if caller intends to create any prepared
 * statements.  Since those don't go away automatically at transaction end
 * (not even on error), we need this flag to cue manual cleanup.
 */
TSConnection *
remote_dist_txn_get_connection(TSConnectionId id, RemoteTxnPrepStmtOption prep_stmt_opt)
{
	bool found;
	RemoteTxn *remote_txn;

	/* First time through, initialize the remote_txn_store */
	if (store == NULL)
		store = remote_txn_store_create(TopTransactionContext);

	remote_txn = remote_txn_store_get(store, id, &found);
	remote_txn_begin(remote_txn, GetCurrentTransactionNestLevel());
	remote_txn_set_will_prep_statement(remote_txn, prep_stmt_opt);

	return remote_txn_get_connection(remote_txn);
}

/* This potentially deallocates prepared statements that were created in a subtxn
 * that aborted before it deallocated the statement.
 */
static void
dist_txn_deallocate_prepared_stmts_if_needed()
{
	RemoteTxn *remote_txn;

	/* below deallocate only happens on error so not worth making async */
	remote_txn_store_foreach(store, remote_txn)
	{
		remote_txn_deallocate_prepared_stmts_if_needed(remote_txn);
	}
}

/* Perform actions on one-phase pre-commit.
 * Mainly just send a COMMIT to all remote nodes and wait for successes.
 */
static void
dist_txn_xact_callback_1pc_pre_commit()
{
	RemoteTxn *remote_txn;
	Catalog *catalog = ts_catalog_get();
	AsyncRequestSet *ars = async_request_set_create();

	eventcallback(DTXN_EVENT_PRE_COMMIT);

	/*
	 * In 1PC, we don't need to add entries to the remote_txn table. However
	 * we do need to take a SHARE lock on it to interlock with any distributed
	 * restore point activity that might be happening in parallel.
	 *
	 * The catalog table lock is kept until the transaction completes in order to
	 * synchronize with distributed restore point creation
	 */
	LockRelationOid(catalog->tables[REMOTE_TXN].id, AccessShareLock);

	/* send a commit to all connections */
	remote_txn_store_foreach(store, remote_txn)
	{
		Assert(remote_connection_xact_depth_get(remote_txn_get_connection(remote_txn)) > 0);

		/* Commit all remote transactions during pre-commit */
		async_request_set_add(ars, remote_txn_async_send_commit(remote_txn));
	}

	eventcallback(DTXN_EVENT_WAIT_COMMIT);

	/* async collect all the replies */
	async_request_set_wait_all_ok_commands(ars);
	dist_txn_deallocate_prepared_stmts_if_needed();
}

/*
 * Abort on the access node.
 *
 * The access node needs to send aborts to all of the remote endpoints. This
 * code should not throw errors itself, since we are already in abort due to a
 * previous error. Instead, we try to emit errors as warnings. For safety, we
 * should probaby try-catch and swallow any potential lower-layer errors given
 * that we're doing remote calls over the network. But the semantics for
 * capturing and proceeding after such recursive errors are unclear.
 */
static void
dist_txn_xact_callback_abort()
{
	RemoteTxn *remote_txn;

	eventcallback(DTXN_EVENT_PRE_ABORT);

	remote_txn_store_foreach(store, remote_txn)
	{
		if (remote_txn_is_ongoing(remote_txn) && !remote_txn_abort(remote_txn))
			elog(WARNING,
				 "transaction rollback on data node \"%s\" failed",
				 remote_connection_node_name(remote_txn_get_connection(remote_txn)));
	}
}

/*
 * Reject transactions that didn't successfully complete a transaction
 * transition at some point.
 */
static void
reject_transaction_with_incomplete_transition(RemoteTxn *remote_txn)
{
	const TSConnection *conn = remote_txn_get_connection(remote_txn);

	if (remote_connection_xact_is_transitioning(conn))
	{
		NameData nodename;

		namestrcpy(&nodename, remote_connection_node_name(conn));
		remote_txn_store_remove(store, remote_txn_get_connection_id(remote_txn));

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("connection to data node \"%s\" was lost", NameStr(nodename))));
	}
}

static void
reject_transactions_with_incomplete_transitions(void)
{
	RemoteTxn *remote_txn;

	remote_txn_store_foreach(store, remote_txn)
	{
		reject_transaction_with_incomplete_transition(remote_txn);
	}
}

static void
cleanup_at_end_of_transaction(void)
{
	RemoteTxn *remote_txn;

	remote_txn_store_foreach(store, remote_txn)
	{
		TSConnection *conn = remote_txn_get_connection(remote_txn);

		/* The connection could have failed at START TRANSACTION, in which
		 * case the depth is 0. Otherwise, we'd expect depth 1. */
		if (remote_connection_xact_depth_get(conn) > 0)
		{
			PGconn *pgconn = remote_connection_get_pg_conn(conn);

			/* Indicate we're out of the transaction */
			Assert(remote_connection_xact_depth_get(conn) == 1);
			remote_connection_xact_depth_dec(conn);

			/* Cleanup connections with failed transactions */
			if (PQstatus(pgconn) != CONNECTION_OK || PQtransactionStatus(pgconn) != PQTRANS_IDLE ||
				remote_connection_xact_is_transitioning(conn))
			{
				elog(DEBUG3, "discarding connection %p", conn);
				remote_txn_store_remove(store, remote_txn_get_connection_id(remote_txn));
			}
		}
	}

	remote_txn_store_destroy(store);
	store = NULL;

	/*
	 * cursor are per-connection and txn so it's safe to reset at the end of
	 * the txn.
	 */
	remote_connection_reset_cursor_number();
}

/*
 * Transaction callback for one-phase commits.
 *
 * With one-phase commits, we send a remote commit during local pre-commit or
 * a remote abort during local abort.
 */
static void
dist_txn_xact_callback_1pc(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			reject_transactions_with_incomplete_transitions();
			dist_txn_xact_callback_1pc_pre_commit();
			break;
		case XACT_EVENT_PRE_PREPARE:

			/*
			 * Cannot prepare stuff on the access node.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot prepare a transaction that modified "
							"remote tables")));
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			/* Pre-commit should have closed the open transaction in 1pc */
			elog(ERROR, "missed cleaning up connection during pre-commit");
			break;
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
			dist_txn_xact_callback_abort();
			break;
	}

	/* In one-phase commit, we're done irrespective of event */
	cleanup_at_end_of_transaction();
}

static void
dist_txn_send_prepare_transaction()
{
	RemoteTxn *remote_txn;
	AsyncRequestSet *ars = async_request_set_create();
	AsyncResponse *error_response = NULL;
	AsyncResponse *res;

	eventcallback(DTXN_EVENT_PRE_PREPARE);

	/* send a prepare transaction to all connections */
	remote_txn_store_foreach(store, remote_txn)
	{
		AsyncRequest *req;

		remote_txn_write_persistent_record(remote_txn);
		req = remote_txn_async_send_prepare_transaction(remote_txn);
		async_request_set_add(ars, req);
	}

	eventcallback(DTXN_EVENT_WAIT_PREPARE);

	/*
	 * async collect the replies. Since errors in PREPARE TRANSACTION are not
	 * uncommon, handle them gracefully: delay throwing errors in results
	 * until all responses collected since you need to mark
	 * changing_xact_state correctly. So throw errors on connection errors but
	 * not errors in results.
	 */
	error_response = NULL;
	while ((res = async_request_set_wait_any_response(ars)))
	{
		switch (async_response_get_type(res))
		{
			case RESPONSE_COMMUNICATION_ERROR:
			case RESPONSE_ERROR:
			case RESPONSE_ROW:
			case RESPONSE_TIMEOUT:
				elog(DEBUG3, "error during second phase of two-phase commit");
				async_response_report_error(res, ERROR);
				continue;
			case RESPONSE_RESULT:
			{
				AsyncResponseResult *response_result = (AsyncResponseResult *) res;
				bool success =
					PQresultStatus(async_response_result_get_pg_result(response_result)) ==
					PGRES_COMMAND_OK;

				if (!success)
				{
					/* save first error, warn about subsequent errors */
					if (error_response == NULL)
						error_response = (AsyncResponse *) response_result;
					else
						async_response_report_error((AsyncResponse *) response_result, WARNING);
				}
				else
					async_response_close(res);
				break;
			}
		}
	}

	if (error_response != NULL)
		async_response_report_error(error_response, ERROR);

	eventcallback(DTXN_EVENT_POST_PREPARE);
}

static void
dist_txn_send_commit_prepared_transaction()
{
	RemoteTxn *remote_txn;
	AsyncRequestSet *ars = async_request_set_create();
	AsyncResponse *res;

	/*
	 * send a commit transaction to all connections and asynchronously collect
	 * the replies
	 */
	remote_txn_store_foreach(store, remote_txn)
	{
		AsyncRequest *req;

		req = remote_txn_async_send_commit_prepared(remote_txn);

		if (req == NULL)
		{
			elog(DEBUG3, "error during second phase of two-phase commit");
			continue;
		}

		async_request_set_add(ars, req);
	}

	eventcallback(DTXN_EVENT_WAIT_COMMIT_PREPARED);

	/* async collect the replies */
	while ((res = async_request_set_wait_any_response(ars)))
	{
		/* throw WARNINGS not ERRORS here */
		/*
		 * NOTE: warnings make sure that all data nodes get a commit prepared.
		 * But, there is arguably some weirdness here in terms of RYOW if
		 * there is an error.
		 */
		AsyncResponseResult *response_result;

		switch (async_response_get_type(res))
		{
			case RESPONSE_COMMUNICATION_ERROR:
			case RESPONSE_ERROR:
			case RESPONSE_ROW:
			case RESPONSE_TIMEOUT:
				elog(DEBUG3, "error during second phase of two-phase commit");
				async_response_report_error(res, WARNING);
				continue;
			case RESPONSE_RESULT:
				response_result = (AsyncResponseResult *) res;
				if (PQresultStatus(async_response_result_get_pg_result(response_result)) !=
					PGRES_COMMAND_OK)
					async_response_report_error(res, WARNING);
				else
					async_response_close(res);
				break;
		}
	}
}

/*
 * Transaction callback for two-phase commit.
 *
 * With two-phase commits, we write a persistent record and send a remote
 * PREPARE TRANSACTION during local pre-commit. After commit we send a remote
 * COMMIT TRANSACTION.
 */
static void
dist_txn_xact_callback_2pc(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
			reject_transactions_with_incomplete_transitions();
			dist_txn_send_prepare_transaction();
			dist_txn_deallocate_prepared_stmts_if_needed();
			break;
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:

			/*
			 * Cannot prepare stuff on the access node.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot prepare a transaction that modified "
							"remote tables")));
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_COMMIT:
			eventcallback(DTXN_EVENT_PRE_COMMIT_PREPARED);

			/*
			 * We send a commit here so that future commands on this
			 * connection get read-your-own-writes semantics. Later, we can
			 * optimize latency on connections by doing this in a background
			 * process and using IPC to assure RYOW
			 */
			dist_txn_send_commit_prepared_transaction();

			/*
			 * NOTE: You cannot delete the remote_txn_persistent_record here
			 * because you are out of transaction. Therefore cleanup of those
			 * entries has to happen in a background process or manually.
			 */
			cleanup_at_end_of_transaction();
			break;
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
			dist_txn_xact_callback_abort();
			cleanup_at_end_of_transaction();
			break;
	}
}

static void
dist_txn_xact_callback(XactEvent event, void *arg)
{
	bool use_2pc;
	char *xactReadOnly;

	/* Quick exit if no connections were touched in this transaction. */
	if (store == NULL)
		return;

	/*
	 * Windows MSVC builds have linking issues for GUC variables from postgres for
	 * use inside this extension. So we use GetConfigOptionByName
	 */
	xactReadOnly = GetConfigOptionByName("transaction_read_only", NULL, false);

	/*
	 * The decision to use 2PC rests on multiple factors:
	 *
	 * 1) if ts_guc_enable_2pc is enabled and it's a regular backend use it
	 *
	 * 2) if ts_guc_enable_2pc is enabled but we are running a read only txn, don't use it
	 *
	 * We might be tempted to use 1PC if just one DN is involved in the transaction.
	 * However, it's possible that a transaction which involves data on AN and the one DN could get
	 * a failure at the end of the COMMIT processing on the AN due to issues in local AN data. In
	 * such a case since we send a COMMIT at "XACT_EVENT_PRE_COMMIT" event time to the DN, we might
	 * end up with a COMMITTED DN but an aborted AN! Hence this optimization is not possible to
	 * guarantee transactional semantics.
	 */
	use_2pc = (ts_guc_enable_2pc && strncmp(xactReadOnly, "on", sizeof("on")) != 0);
#ifdef TS_DEBUG
	ereport(DEBUG3, (errmsg("use 2PC: %s", use_2pc ? "true" : "false")));
#endif

	if (use_2pc)
		dist_txn_xact_callback_2pc(event, arg);
	else
		dist_txn_xact_callback_1pc(event, arg);
}

/*
 * Subtransaction callback handler.
 *
 * If the subtxn was committed, send a RELEASE SAVEPOINT to the remote nodes.
 * If the subtxn was aborted, send a ROLLBACK SAVEPOINT and set a deferred
 * error if that fails.
 */
static void
dist_txn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	RemoteTxn *remote_txn;
	int curlevel;

	/* Quick exit if no connections were touched in this transaction. */
	if (store == NULL)
		return;

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
		case SUBXACT_EVENT_COMMIT_SUB:
			/* Nothing to do at subxact start, nor after commit. */
			return;
		case SUBXACT_EVENT_PRE_COMMIT_SUB:
			reject_transactions_with_incomplete_transitions();
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			eventcallback(DTXN_EVENT_SUB_XACT_ABORT);
			break;
	}

	curlevel = GetCurrentTransactionNestLevel();

	remote_txn_store_foreach(store, remote_txn)
	{
		TSConnection *conn = remote_txn_get_connection(remote_txn);

		if (!remote_txn_is_at_sub_txn_level(remote_txn, curlevel))
			continue;

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			reject_transaction_with_incomplete_transition(remote_txn);
			remote_txn_sub_txn_pre_commit(remote_txn, curlevel);
		}
		else
		{
			Assert(event == SUBXACT_EVENT_ABORT_SUB);
			remote_txn_sub_txn_abort(remote_txn, curlevel);
		}

		remote_connection_xact_depth_dec(conn);
	}
}

void
_remote_dist_txn_init()
{
	RegisterXactCallback(dist_txn_xact_callback, NULL);
	RegisterSubXactCallback(dist_txn_subxact_callback, NULL);
}

void
_remote_dist_txn_fini()
{
	/* can't unregister callbacks */
	if (NULL != store)
	{
		remote_txn_store_destroy(store);
		store = NULL;
	}
}
