/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <storage/procarray.h>
#include <foreign/foreign.h>
#include <miscadmin.h>

#include "node_killer.h"
#include "connection.h"
#include "guc.h"
#include "export.h"
#include "remote/connection.h"
#include "remote/dist_txn.h"

#include "test_utils.h"
#include "connection.h"

static RemoteNodeKiller rnk_event = {
	.conn = NULL,
	.pid = -1,
	.killevent = DTXN_EVENT_ANY,
};

static void
kill_backend(const DistTransactionEvent event, void *data)
{
	RemoteNodeKiller *rnk = data;

	if (event == rnk->killevent || rnk->killevent == DTXN_EVENT_ANY)
	{
		elog(WARNING, "kill event: %s", remote_dist_txn_event_name(rnk->killevent));
		remote_node_killer_kill(&rnk_event);
		remote_dist_txn_set_event_handler(NULL);
	}
}

static DistTransactionEventHandler eventhandler = {
	.handler = kill_backend,
	.data = &rnk_event,
};

TS_FUNCTION_INFO_V1(ts_remote_node_killer_set_event);

void
remote_node_killer_init(RemoteNodeKiller *rnk, const TSConnection *conn,
						const DistTransactionEvent event)
{
	int remote_pid = remote_connection_get_remote_pid(conn);

	if (remote_pid == -1)
		elog(ERROR, "could not get PID of remote backend process");

	MemSet(rnk, 0, sizeof(*rnk));
	rnk->conn = conn;
	rnk->pid = remote_pid;
	rnk->killevent = event;
}

static int
kill_backends(pid_t pid, int sig)
{
#ifdef HAVE_SETSID
	/* If PostgreSQL is compiled with setsid support, then the running
	 * PostgreSQL processes are in a process group and the negative pid kills
	 * all the other processes in the same group. */
	return kill(-pid, SIGTERM);
#else
	return kill(pid, SIGTERM);
#endif
}

/*
 * Kill a remote process (data node).
 *
 * This only works if the process is on the same instance, which is true for
 * regression tests.
 *
 * The function should not throw errors since it might be invoked from a
 * transaction abort handler, which itself is invoked by errors. Throwing an
 * error could cause unexpected recursion, and crash.
 */
void
remote_node_killer_kill(RemoteNodeKiller *rnk)
{
	PGPROC *proc;
	int ret;

	if (rnk->num_kills > 0)
		elog(WARNING, "cannot kill backend twice on the same event");

	/*
	 * do not use pg_terminate_backend here because that does permission
	 * checks through the catalog which requires you to be in a transaction
	 */
	proc = BackendPidGetProc(rnk->pid);

	if (proc == NULL)
		ereport(WARNING, (errmsg("PID %d is not a PostgreSQL server process", rnk->pid)));

	rnk->num_kills++;

	ret = kill_backends(rnk->pid, SIGTERM);

	if (ret != 0)
		ereport(WARNING, (errmsg("could not send signal to process %d: %m", rnk->pid)));
	else
	{
		/* Ensure that the backend is dead before proceeding. Otherwise, we
		 * might end up in a race condition where we check the status of the
		 * backend before it has actually exited, leading to flaky testing. */
		unsigned wait_count = 300;

		while (BackendPidGetProc(rnk->pid) != NULL)
		{
			if (wait_count == 0)
			{
				elog(WARNING, "timeout while waiting for killed backend to exit");
				break;
			}
			wait_count--;
			pg_usleep(100L);
		}
		/* Once PG registered the backend as gone, wait some additional time
		 * for it to really exit */
		pg_usleep(500L);
	}
}

Datum
ts_remote_node_killer_set_event(PG_FUNCTION_ARGS)
{
	const char *event_name = PG_ARGISNULL(0) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(0));
	const char *server_name = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(1));
	DistTransactionEvent event;
	TSConnectionId id;
	TSConnection *conn;

	if (NULL == event_name || NULL == server_name)
		elog(ERROR, "invalid argument");

	remote_dist_txn_set_event_handler(&eventhandler);

	remote_connection_id_set(&id,
							 GetForeignServerByName(server_name, false)->serverid,
							 GetUserId());

	conn = remote_dist_txn_get_connection(id, REMOTE_TXN_NO_PREP_STMT);
	Assert(conn);
	event = remote_dist_txn_event_from_name(event_name);
	remote_node_killer_init(&rnk_event, conn, event);

	PG_RETURN_VOID();
}
