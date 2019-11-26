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
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <access/reloptions.h>
#include <access/xact.h>
#include <catalog/pg_foreign_server.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <utils/guc.h>
#include <utils/inval.h>

#include "connection.h"
#include "remote/connection.h"
#include "remote/connection_cache.h"
#include "remote/txn.h"
#include "export.h"
#include "connection.h"
#include "test_utils.h"
#include "connection.h"

TS_FUNCTION_INFO_V1(ts_test_remote_connection_cache);
TS_FUNCTION_INFO_V1(ts_test_alter_data_node);

static void
test_basic_cache()
{
	TSConnectionId id_1;
	TSConnectionId id_2;
	TSConnection *conn_1;
	TSConnection *conn_2;
	pid_t pid_1;
	pid_t pid_2;
	pid_t pid_prime;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());
	remote_connection_id_set(&id_2,
							 GetForeignServerByName("loopback_2", false)->serverid,
							 GetUserId());

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_1 = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != 0);

	conn_2 = remote_connection_cache_get_connection(id_2);
	pid_2 = remote_connection_get_remote_pid(conn_2);
	TestAssertTrue(pid_2 != 0);

	TestAssertTrue(pid_1 != pid_2);

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 == pid_prime);
}

static void
set_data_node_application_name(const char *nodename, DefElemAction action)
{
	/* Generate a new name. Use PID of current backend since every alter
	 * happens on an new connection. */
	char *appname = psprintf("app%d", MyProcPid);
	AlterForeignServerStmt stmt = {
		.type = T_AlterForeignServerStmt,
		.servername = pstrdup(nodename),
		.options = list_make1(makeDefElemExtended(NULL,
												  "application_name",
												  (Node *) makeString(appname),
												  action,
												  -1)),
		.version = NULL,
		.has_version = false,
	};

	AlterForeignServer(&stmt);
}

/*
 * Alter a server for testing invalidation.  We directly call
 * AlterForeignServer() instead of issuing ALTER SERVER to bypass
 * process_utility blocks on altering servers.
 */
Datum
ts_test_alter_data_node(PG_FUNCTION_ARGS)
{
	const char *nodename = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	DefElemAction action = DEFELEM_ADD;
	ForeignServer *server;
	ListCell *lc;

	if (!nodename)
		PG_RETURN_BOOL(true);

	server = GetForeignServerByName(nodename, false);

	foreach (lc, server->options)
	{
		DefElem *de = lfirst(lc);

		if (strcmp(de->defname, "application_name") == 0)
		{
			action = DEFELEM_SET;
			break;
		}
	}

	set_data_node_application_name(nodename, action);
	PG_RETURN_BOOL(false);
}

/* This alters the server on the local (test) database in a separate backend */
static void
invalidate_server()
{
	TSConnection *conn_modify = get_connection();
	remote_connection_query_ok(conn_modify,
							   "SELECT _timescaledb_internal.test_alter_data_node('loopback_1')");
	AcceptInvalidationMessages();
	remote_connection_close(conn_modify);
}

static void
test_invalidate_server()
{
	TSConnectionId id_1;
	TSConnection *conn_1;
	pid_t pid_1;
	pid_t pid_prime;
	char *original_application_name;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_1 = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != 0);

	/* simulate an invalidation in another backend */
	invalidate_server();

	/* Should get a different connection since we invalidated the foreign
	 * server and didn't yet start a transaction on the remote node. */
	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);

	/* Test that connections remain despite invalidations during ongoing
	 * remote transaction */
	pid_1 = pid_prime;
	original_application_name = remote_connection_get_application_name(conn_1);
	BeginInternalSubTransaction("sub1");

	/* Start a remote transaction on the connection */
	remote_txn_begin_on_connection(conn_1);
	invalidate_server();

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);

	/* Connection should be the same despite invalidation since we're in a
	 * transaction */
	TestAssertTrue(pid_1 == pid_prime);
	TestAssertTrue(
		strcmp(original_application_name, remote_connection_get_application_name(conn_1)) == 0);

	RollbackAndReleaseCurrentSubTransaction();

	/* After rollback, we're still in a remote transaction. Connection should
	 * be the same. */
	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 == pid_prime);
	pid_1 = pid_prime;
}

static void
test_remove()
{
	TSConnectionId id_1;
	TSConnection *conn_1;
	pid_t pid_1;
	pid_t pid_prime;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_1 = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != 0);

	remote_connection_cache_remove(id_1);

	/* even using the same pin, get new connection */
	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);

	pid_1 = pid_prime;

	/* test the by_oid variant */
	remote_connection_cache_remove(id_1);

	conn_1 = remote_connection_cache_get_connection(id_1);
	pid_prime = remote_connection_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);
}

Datum
ts_test_remote_connection_cache(PG_FUNCTION_ARGS)
{
	test_basic_cache();
	test_invalidate_server();
	test_remove();

	PG_RETURN_VOID();
}
