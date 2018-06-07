/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <utils/hsearch.h>
#include <access/htup_details.h>
#include <catalog/pg_user_mapping.h>
#include <access/xact.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "dist_txn.h"
#include "connection.h"
#include "async.h"
#include "txn.h"
#include "txn_store.h"

#ifdef DEBUG

void (*testing_callback_call_hook)(const char *event) = NULL;
#define testing_callback_call(event)                                                               \
	(testing_callback_call_hook != NULL ? testing_callback_call_hook(event) : NULL)

#else

#define testing_callback_call(event)

#endif

/* prototypes of private functions */
static void dist_txn_xact_callback(XactEvent event, void *arg);
static void dist_txn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);
typedef struct DistTxnState
{
	bool sub_txn_abort_failure;
	Oid sub_txn_abort_user_mapping_oid;
	RemoteTxnStore *store;
} DistTxnState;

static DistTxnState state;

static void
dist_txn_state_reset()
{
	if (state.store != NULL)
		remote_txn_store_destroy(state.store);

	state = (DistTxnState){
		.sub_txn_abort_failure = false,
		.sub_txn_abort_user_mapping_oid = InvalidOid,
		.store = NULL,
	};
}

static void
dist_txn_state_mark_subtxn_error(Oid user_mapping_oid)
{
	state.sub_txn_abort_failure = true;
	state.sub_txn_abort_user_mapping_oid = user_mapping_oid;
}

/* This is for checking for a deferred txn error that could not be thrown when they occurred
 *  This happens when there is an error during subtxn abort since you should not throw
 *  errors during subxact abort. Thus we have to check for such errors sometime later. We try
 *  to have these checks as soon as is practical, but we have to throw before pre-commit. */
static void
dist_txn_state_throw_deferred_error()
{
	HeapTuple tup;
	Form_pg_user_mapping umform;
	ForeignServer *server;

	/* no deferred error */
	if (!state.sub_txn_abort_failure)
		return;

	/* find server name to be shown in the message below */
	tup = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(state.sub_txn_abort_user_mapping_oid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR,
			 "cache lookup failed for user mapping %u",
			 state.sub_txn_abort_user_mapping_oid);
	umform = (Form_pg_user_mapping) GETSTRUCT(tup);
	server = GetForeignServer(umform->umserver);
	ReleaseSysCache(tup);

	ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("connection to server \"%s\" was lost", server->servername)));
}

/*
 * Get a PGconn which can be used to execute queries on the remote PostgreSQL
 * server with the user's authorization.  A new connection is established
 * if we don't already have a suitable one, and a transaction is opened at
 * the right subtransaction nesting depth if we didn't do that already.
 *
 * will_prep_stmt must be true if caller intends to create any prepared
 * statements.  Since those don't go away automatically at transaction end
 * (not even on error), we need this flag to cue manual cleanup.
 */
PGconn *
remote_dist_txn_get_connection(UserMapping *user, RemoteTxnPrepStmtOption prep_stmt_opt)
{
	bool found;
	RemoteTxn *remote_txn;

	/* First time through, initialize the remote_txn_store */
	if (state.store == NULL)
		state.store = remote_txn_store_create(TopTransactionContext);

	/* Not critical: but raises error earlier */
	dist_txn_state_throw_deferred_error();

	remote_txn = remote_txn_store_get(state.store, user, &found);

	/*
	 * Start a new transaction or subtransaction if it hasn't yet been started
	 * by a previous command in the same txn.
	 */
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
	remote_txn_store_foreach(state.store, remote_txn)
	{
		remote_txn_deallocate_prepared_stmts_if_needed(remote_txn);
	}
}

static void
dist_txn_on_txn_end()
{
	dist_txn_state_reset();

	/*
	 * cursor are per-connection and txn so it's safe to reset at the end of
	 * the txn.
	 */
	remote_connection_reset_cursor_number();
}

/* Perform actions on 1-pc pre-commit.
 * Mainly just send a COMMIT to all remote nodes and wait for successes.
 */
static void
dist_txn_xact_callback_1pc_pre_commit()
{
	RemoteTxn *remote_txn;
	AsyncRequestSet *ars = async_request_set_create();

	testing_callback_call("pre-commit");

	/*
	 * This is critical to make sure no txn that had an error in subtxn abort
	 * ever gets committed. Remember that those failed connections are no
	 * longer in the store and so this is our fail-safe to make sure we abort
	 * such txns
	 */
	dist_txn_state_throw_deferred_error();

	/* send a commit to all connections */
	remote_txn_store_foreach(state.store, remote_txn)
	{
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
dist_txn_xact_callback_1pc_abort()
{
	RemoteTxn *remote_txn;

	testing_callback_call("pre-abort");
	remote_txn_store_foreach(state.store, remote_txn)
	{
		if (!remote_txn_abort(remote_txn))
		{
			elog(WARNING, "failure aborting remote transaction during local abort");
			remote_txn_store_remove(state.store, remote_txn_get_user_mapping_oid(remote_txn));
		}
	}
}

/*
 * remote_dist_txn_xact_callback_1pc --- cleanup at main-transaction end.
 * With 1 pc commits, we send a remote commit during local pre-commit
 * or a remote abort during local abort.
 */
static void
dist_txn_xact_callback_1pc(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
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
			dist_txn_xact_callback_1pc_abort();
			break;
	}
	dist_txn_on_txn_end();
}
static void
dist_txn_xact_callback(XactEvent event, void *arg)
{
	/* Quick exit if no connections were touched in this transaction. */
	if (state.store == NULL)
		return;

	dist_txn_xact_callback_1pc(event, arg);
}

/*
 * remote_dist_txn_subxact_callback --- cleanup at subtransaction end.
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

	/* Nothing to do at subxact start, nor after commit. */
	if (!(event == SUBXACT_EVENT_PRE_COMMIT_SUB || event == SUBXACT_EVENT_ABORT_SUB))
		return;

	/* Quick exit if no connections were touched in this transaction. */
	if (state.store == NULL)
		return;

	if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		/* This is not critical but allows errors to be raised earlier */
		dist_txn_state_throw_deferred_error();

	if (event == SUBXACT_EVENT_ABORT_SUB)
		testing_callback_call("subxact-abort");

	curlevel = GetCurrentTransactionNestLevel();
	remote_txn_store_foreach(state.store, remote_txn)
	{
		if (!remote_txn_is_at_sub_txn_level(remote_txn, curlevel))
			continue;

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
			remote_txn_sub_txn_pre_commit(remote_txn, curlevel);
		else
		{
			Assert(event == SUBXACT_EVENT_ABORT_SUB);
			if (!remote_txn_sub_txn_abort(remote_txn, curlevel))
			{
				dist_txn_state_mark_subtxn_error(remote_txn_get_user_mapping_oid(remote_txn));
				remote_txn_store_remove(state.store, remote_txn_get_user_mapping_oid(remote_txn));
			}
		}
	}
}

void
_remote_dist_txn_init()
{
	RegisterXactCallback(dist_txn_xact_callback, NULL);
	RegisterSubXactCallback(dist_txn_subxact_callback, NULL);
	dist_txn_state_reset();
}

void
_remote_dist_txn_fini()
{
	/* can't unregister callbacks */
	dist_txn_state_reset();
}
