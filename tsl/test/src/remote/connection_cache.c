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
#include <catalog/pg_user_mapping.h>
#include <catalog/pg_foreign_server.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <utils/guc.h>
#include <utils/inval.h>

#include "remote/connection.h"
#include "connection.h"
#include "remote/connection_cache.h"
#include "export.h"
#include "test_utils.h"

#define NEW_APPLICATION_NAME "app2"

TS_FUNCTION_INFO_V1(tsl_test_remote_connection_cache);

static void
test_basic_cache()
{
	ForeignServer *foreign_server_1;
	ForeignServer *foreign_server_2;
	UserMapping *um_1;
	UserMapping *um_2;
	PGconn *conn_1;
	PGconn *conn_2;
	pid_t pid_1;
	pid_t pid_2;
	pid_t pid_prime;
	Cache *cache;

	foreign_server_1 = GetForeignServerByName("loopback_1", false);
	um_1 = GetUserMapping(GetUserId(), foreign_server_1->serverid);

	foreign_server_2 = GetForeignServerByName("loopback_2", false);
	um_2 = GetUserMapping(GetUserId(), foreign_server_2->serverid);

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != 0);

	conn_2 = remote_connection_cache_get_connection(cache, um_2);
	pid_2 = remote_connecton_get_remote_pid(conn_2);
	Assert(pid_2 != 0);

	Assert(pid_1 != pid_2);

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 == pid_prime);

	ts_cache_release(cache);

	/* since there was no invalidation should be same across cache pins */
	cache = remote_connection_cache_pin();
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 == pid_prime);

	ts_cache_release(cache);
}

/* This alters the server on the local (test) database in a separate backend */
static void
invalidate_server()
{
	PGconn *conn_modify = get_connection();
	remote_connection_exec_ok_command(conn_modify,
									  "ALTER SERVER loopback_1 OPTIONS (application_name "
									  "'" NEW_APPLICATION_NAME "')");
	AcceptInvalidationMessages();
	remote_connection_close(conn_modify);
}

/* This alters the user on the local (test) database in a separate backend */
static void
invalidate_user()
{
	PGconn *conn_modify = get_connection();
	remote_connection_exec_ok_command(conn_modify,
									  "ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_1 "
									  "OPTIONS (test_option 'false')");
	AcceptInvalidationMessages();
	remote_connection_close(conn_modify);
}

static void
test_invalidate_server()
{
	ForeignServer *foreign_server_1;
	UserMapping *um_1;
	PGconn *conn_1;
	pid_t pid_1;
	pid_t pid_prime;
	Cache *cache;
	char *original_application_name;

	foreign_server_1 = GetForeignServerByName("loopback_1", false);
	um_1 = GetUserMapping(GetUserId(), foreign_server_1->serverid);

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	original_application_name = remote_connecton_get_application_name(conn_1);
	Assert(pid_1 != 0);

	/* simulate an invalidation in another backend */
	invalidate_server();

	/* using the same pin, still getting the same connection */
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 == pid_prime);
	Assert(strcmp(original_application_name, remote_connecton_get_application_name(conn_1)) == 0);

	ts_cache_release(cache);

	/* using a new cache pin, getting a new connection */
	cache = remote_connection_cache_pin();
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != pid_prime);
	Assert(strcmp(original_application_name, remote_connecton_get_application_name(conn_1)) != 0);
	Assert(strcmp(NEW_APPLICATION_NAME, remote_connecton_get_application_name(conn_1)) == 0);

	ts_cache_release(cache);
}

static void
test_invalidate_user()
{
	ForeignServer *foreign_server_1;
	UserMapping *um_1;
	PGconn *conn_1;
	pid_t pid_1;
	pid_t pid_prime;
	Cache *cache;

	foreign_server_1 = GetForeignServerByName("loopback_1", false);
	um_1 = GetUserMapping(GetUserId(), foreign_server_1->serverid);

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != 0);

	/* simulate an invalidation in another backend */
	invalidate_user();

	/* using the same pin, still getting the same connection */
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 == pid_prime);

	ts_cache_release(cache);

	/* using a new cache pin, getting a new connection */
	cache = remote_connection_cache_pin();
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != pid_prime);

	ts_cache_release(cache);
}

static void
test_remove()
{
	ForeignServer *foreign_server_1;
	UserMapping *um_1;
	PGconn *conn_1;
	pid_t pid_1;
	pid_t pid_prime;
	Cache *cache;

	foreign_server_1 = GetForeignServerByName("loopback_1", false);
	um_1 = GetUserMapping(GetUserId(), foreign_server_1->serverid);

	cache = remote_connection_cache_pin();

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_1 = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != 0);

	remote_connection_cache_remove(cache, um_1);

	/* even using the same pin, get new connection */
	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != pid_prime);

	pid_1 = pid_prime;

	/* test the by_oid variant */
	remote_connection_cache_remove_by_oid(cache, um_1->umid);

	conn_1 = remote_connection_cache_get_connection(cache, um_1);
	pid_prime = remote_connecton_get_remote_pid(conn_1);
	Assert(pid_1 != pid_prime);

	ts_cache_release(cache);
}

Datum
tsl_test_remote_connection_cache(PG_FUNCTION_ARGS)
{
	test_basic_cache();
	test_invalidate_server();
	test_invalidate_user();
	test_remove();

	PG_RETURN_VOID();
}
