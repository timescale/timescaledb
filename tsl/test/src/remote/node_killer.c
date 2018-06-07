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
#include "guc.h"
#include "export.h"
#include "connection.h"
#include "remote/dist_txn.h"

typedef struct RemoteNodeKiller
{
	pid_t pid;
	PGconn *conn;
} RemoteNodeKiller;

static char *kill_event = NULL;
static RemoteNodeKiller *rnk_event = NULL;

TS_FUNCTION_INFO_V1(ts_remote_node_killer_set_event);

RemoteNodeKiller *
remote_node_killer_init(PGconn *conn)
{
	RemoteNodeKiller *rnk = palloc(sizeof(RemoteNodeKiller));

	rnk->conn = conn;
	rnk->pid = remote_connecton_get_remote_pid(conn);

	/* do not throw error here on pid = 0 to avoid recursive abort */
	/* remote_connection_report_error(ERROR, res, conn, false, sql);  */
	return rnk;
}

void
remote_node_killer_kill(RemoteNodeKiller *rnk)
{
	/*
	 * do not use pg_terminate_backend here because that does permission
	 * checks through the catalog which requires you to be in a transaction
	 */
	PGPROC *proc = BackendPidGetProc(rnk->pid);

	if (proc == NULL)
		ereport(WARNING, (errmsg("PID %d is not a PostgreSQL server process", rnk->pid)));
	kill_event = NULL;
#ifdef HAVE_SETSID
	if (kill(-rnk->pid, SIGTERM))
#else
	if (kill(rnk->pid, SIGTERM))
#endif
		ereport(WARNING, (errmsg("could not send signal to process %d: %m", rnk->pid)));
}

Datum
ts_remote_node_killer_set_event(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	Datum arg2 = PG_GETARG_DATUM(1);
	char *event;
	char *server_name;
	ForeignServer *foreign_server;
	UserMapping *um;
	MemoryContext ctx;

	testing_callback_call_hook = remote_node_killer_kill_on_event;

	ctx = MemoryContextSwitchTo(TopTransactionContext);

	event = TextDatumGetCString(arg1);
	kill_event = event;

	server_name = TextDatumGetCString(arg2);
	foreign_server = GetForeignServerByName(server_name, false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	rnk_event =
		remote_node_killer_init(remote_dist_txn_get_connection(um, REMOTE_TXN_NO_PREP_STMT));

	MemoryContextSwitchTo(ctx);
	PG_RETURN_VOID();
}

extern void
remote_node_killer_kill_on_event(const char *event)
{
	if (kill_event != NULL && strcmp(kill_event, event) == 0)
	{
		elog(WARNING, "kill event: %s", event);
		remote_node_killer_kill(rnk_event);
	}
}
