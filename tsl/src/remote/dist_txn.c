/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <utils/hsearch.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "dist_txn.h"
#include "connection.h"
#include "async.h"
#include "txn.h"
#include "txn_store.h"
#include "guc.h"

#ifdef DEBUG

void (*testing_callback_call_hook)(const char *event) = NULL;

#define testing_callback_call(event)                                                               \
	do                                                                                             \
	{                                                                                              \
		if (testing_callback_call_hook != NULL)                                                    \
			testing_callback_call_hook(event);                                                     \
	} while (0)
#else
#define testing_callback_call(event)                                                               \
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
	AsyncRequestSet *ars = async_request_set_create();

	testing_callback_call("pre-commit");

	/* send a commit to all connections */
	remote_txn_store_foreach(store, remote_txn)
	{
		Assert(remote_connection_xact_depth_get(remote_txn_get_connection(remote_txn)) > 0);

		/* Commit all remote transactions during pre-commit */
		async_request_set_add(ars, remote_txn_async_send_commit(remote_txn));
	}

	testing_callback_call("waiting-commit");

	/* async collect all the replies */
	async_request_set_wait_all_ok_commands(ars);

	dist_txn_deallocate_prepared_stmts_if_needed();
}

/* On abort on the frontend send aborts to all of the remote endpoints */
static void
dist_txn_xact_callback_abort()
{
	RemoteTxn *remote_txn;

	testing_callback_call("pre-abort");

	remote_txn_store_foreach(store, remote_txn)
	{
		if (remote_txn_is_ongoing(remote_txn) && !remote_txn_abort(remote_txn))
			elog(WARNING, "failure aborting remote transaction during local abort");
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
			 * Cannot prepare stuff on the frontend.
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
	AsyncResponseResult *response_result;

	testing_callback_call("pre-prepare-transaction");

	/* send a prepare transaction to all connections */
	remote_txn_store_foreach(store, remote_txn)
	{
		AsyncRequest *req;

		remote_txn_write_persistent_record(remote_txn);
		req = remote_txn_async_send_prepare_transaction(remote_txn);
		async_request_set_add(ars, req);
	}

	testing_callback_call("waiting-prepare-transaction");

	/*
	 * async collect the replies. Since errors in PREPARE TRANSACTION are not
	 * uncommon, handle them gracefully: delay throwing errors in results
	 * until all responses collected since you need to mark
	 * changing_xact_state correctly. So throw errors on connection errors but
	 * not errors in results.
	 */
	error_response = NULL;
	while ((response_result = async_request_set_wait_any_result(ars)))
	{
		bool success = PQresultStatus(async_response_result_get_pg_result(response_result)) ==
					   PGRES_COMMAND_OK;

		if (!success)
		{
			/* save first error, warn about subsequent errors */
			if (error_response == NULL)
				error_response = (AsyncResponse *) response_result;
			else
				async_response_report_error((AsyncResponse *) response_result, WARNING);
		}
	}

	if (error_response != NULL)
		async_response_report_error(error_response, ERROR);

	testing_callback_call("post-prepare-transaction");
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
			elog(WARNING, "error while performing second phase of 2-pc");
			continue;
		}

		async_request_set_add(ars, req);
	}

	testing_callback_call("waiting-commit-prepared");

	/* async collect the replies */
	while ((res = async_request_set_wait_any_response(ars, WARNING)))
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
			case RESPONSE_ROW:
			case RESPONSE_TIMEOUT:
				elog(WARNING, "error while performing second phase of 2-pc");
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
			testing_callback_call("pre-commit-prepared");

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
	/* Quick exit if no connections were touched in this transaction. */
	if (store == NULL)
		return;

	if (ts_guc_enable_2pc)
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

	/* Nothing to do at subxact start, nor after commit. */
	if (!(event == SUBXACT_EVENT_PRE_COMMIT_SUB || event == SUBXACT_EVENT_ABORT_SUB))
		return;

	if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		reject_transactions_with_incomplete_transitions();

	if (event == SUBXACT_EVENT_ABORT_SUB)
		testing_callback_call("subxact-abort");

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
