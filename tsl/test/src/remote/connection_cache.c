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
	Cache *cache;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());
	remote_connection_id_set(&id_2,
							 GetForeignServerByName("loopback_2", false)->serverid,
							 GetUserId());

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != 0);

	conn_2 = remote_connection_cache_get_connection(cache, id_2);
	pid_2 = remote_connecton_get_remote_pid(conn_2);
	TestAssertTrue(pid_2 != 0);

	TestAssertTrue(pid_1 != pid_2);

	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 == pid_prime);

	ts_cache_release(cache);

	/* since there was no invalidation should be same across cache pins */
	cache = remote_connection_cache_pin();
	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 == pid_prime);

	ts_cache_release(cache);
}

#define NEW_APPLICATION_NAME "app2"

/*
 * Alter a server for testing invalidation.  We directly call
 * AlterForeignServer() instead of issuing ALTER SERVER to bypass
 * process_utility blocks on altering servers.
 */
Datum
ts_test_alter_data_node(PG_FUNCTION_ARGS)
{
	AlterForeignServerStmt stmt = {
		.type = T_AlterForeignServerStmt,
		.servername = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0),
		.version = NULL,
		.options = list_make1(makeDefElemExtended(NULL,
												  "application_name",
												  (Node *) makeString(NEW_APPLICATION_NAME),
												  DEFELEM_ADD,
												  -1)),
		.has_version = false,
	};

	if (NULL != stmt.servername)
	{
		AlterForeignServer(&stmt);
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
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
	Cache *cache;
	char *original_application_name;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	original_application_name = remote_connecton_get_application_name(conn_1);
	TestAssertTrue(pid_1 != 0);

	/* simulate an invalidation in another backend */
	invalidate_server();

	/* using the same pin, still getting the same connection */
	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 == pid_prime);
	TestAssertTrue(
		strcmp(original_application_name, remote_connecton_get_application_name(conn_1)) == 0);

	ts_cache_release(cache);

	/* using a new cache pin, getting a new connection */
	cache = remote_connection_cache_pin();
	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);
	TestAssertTrue(
		strcmp(original_application_name, remote_connecton_get_application_name(conn_1)) != 0);
	TestAssertTrue(strcmp(NEW_APPLICATION_NAME, remote_connecton_get_application_name(conn_1)) ==
				   0);

	ts_cache_release(cache);
}

static void
test_remove()
{
	TSConnectionId id_1;
	TSConnection *conn_1;
	pid_t pid_1;
	pid_t pid_prime;
	Cache *cache;

	remote_connection_id_set(&id_1,
							 GetForeignServerByName("loopback_1", false)->serverid,
							 GetUserId());

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != 0);

	remote_connection_cache_remove(cache, id_1);

	/* even using the same pin, get new connection */
	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);

	pid_1 = pid_prime;

	/* test the by_oid variant */
	remote_connection_cache_remove(cache, id_1);

	conn_1 = remote_connection_cache_get_connection(cache, id_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	TestAssertTrue(pid_1 != pid_prime);

	ts_cache_release(cache);
}

Datum
ts_test_remote_connection_cache(PG_FUNCTION_ARGS)
{
	test_basic_cache();
	test_invalidate_server();
	test_remove();

	PG_RETURN_VOID();
}
