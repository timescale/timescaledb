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

TS_FUNCTION_INFO_V1(tsl_test_remote_connection_cache);

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
tsl_test_remote_connection_cache(PG_FUNCTION_ARGS)
{
	test_basic_cache();
	test_remove();

	PG_RETURN_VOID();
}
