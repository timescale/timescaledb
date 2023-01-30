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
#include <access/htup_details.h>
#include <catalog/pg_foreign_server.h>
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <utils/guc.h>
#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "test_utils.h"
#include "connection.h"

TSConnection *
get_connection()
{
	return remote_connection_open_session("testdb",
										  list_make4(makeDefElem("user",
																 (Node *) makeString(
																	 GetUserNameFromId(GetUserId(),
																					   false)),
																 -1),
													 makeDefElem("host",
																 (Node *) makeString("localhost"),
																 -1),
													 makeDefElem("dbname",
																 (Node *) makeString(
																	 get_database_name(
																		 MyDatabaseId)),
																 -1),
													 makeDefElem("port",
																 (Node *) makeString(pstrdup(
																	 GetConfigOption("port",
																					 false,
																					 false))),
																 -1)),
										  false);
}

static void
test_options()
{
	TestAssertTrue(remote_connection_valid_user_option("user"));
	TestAssertTrue(!remote_connection_valid_user_option("port"));
	TestAssertTrue(!remote_connection_valid_user_option("xxx"));
	TestAssertTrue(!remote_connection_valid_user_option("fallback_application_name"));

	TestAssertTrue(remote_connection_valid_node_option("port"));
	TestAssertTrue(!remote_connection_valid_node_option("user"));
	TestAssertTrue(!remote_connection_valid_node_option("xxx"));
	TestAssertTrue(!remote_connection_valid_node_option("fallback_application_name"));
}

static void
test_numbers_associated_with_connections()
{
	TSConnection *conn = get_connection();
	TestAssertTrue(remote_connection_get_cursor_number() == 1);
	TestAssertTrue(remote_connection_get_cursor_number() == 2);
	TestAssertTrue(remote_connection_get_cursor_number() == 3);
	remote_connection_reset_cursor_number();
	TestAssertTrue(remote_connection_get_cursor_number() == 1);
	TestAssertTrue(remote_connection_get_cursor_number() == 2);

	TestAssertTrue(remote_connection_get_prep_stmt_number() == 1);
	TestAssertTrue(remote_connection_get_prep_stmt_number() == 2);
	TestAssertTrue(remote_connection_get_prep_stmt_number() == 3);
	remote_connection_close(conn);
}

static void
test_simple_queries()
{
	TSConnection *conn = get_connection();
	PGresult *res;
	remote_connection_exec(conn, "SELECT 1");
	remote_connection_exec(conn, "SET search_path = pg_catalog");

	res = remote_connection_exec(conn, "SELECT 1");
	TestAssertTrue(PQresultStatus(res) == PGRES_TUPLES_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SELECT abc");
	TestAssertTrue(PQresultStatus(res) != PGRES_TUPLES_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SET search_path = pg_catalog");
	TestAssertTrue(PQresultStatus(res) == PGRES_COMMAND_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SET 123 = 123");
	TestAssertTrue(PQresultStatus(res) != PGRES_COMMAND_OK);
	PQclear(res);

	remote_connection_cmd_ok(conn, "SET search_path = pg_catalog");
	/* not a command */
	TestEnsureError(remote_connection_cmd_ok(conn, "SELECT 1"));
	remote_connection_close(conn);
}

#define ASSERT_NUM_OPEN_CONNECTIONS(stats, num)                                                    \
	TestAssertTrue((((stats)->connections_created - (stats)->connections_closed) == (num)))
#define ASSERT_NUM_OPEN_RESULTS(stats, num)                                                        \
	TestAssertTrue((((stats)->results_created - (stats)->results_cleared) == (num)))

static void
test_connection_and_result_leaks()
{
	TSConnection *conn, *subconn, *subconn2;
	PGresult *res;
	RemoteConnectionStats *stats;
	MemoryContext old;
	MemoryContext mcxt;

	mcxt = AllocSetContextCreate(CurrentMemoryContext, "test", ALLOCSET_SMALL_SIZES);

	old = MemoryContextSwitchTo(mcxt);
	stats = remote_connection_stats_get();
	remote_connection_stats_reset();

	conn = get_connection();
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 1);

	res = remote_connection_exec(conn, "SELECT 1");
	remote_connection_close(conn);

	ASSERT_NUM_OPEN_CONNECTIONS(stats, 0);
	ASSERT_NUM_OPEN_RESULTS(stats, 0);

	conn = get_connection();

	ASSERT_NUM_OPEN_CONNECTIONS(stats, 1);

	BeginInternalSubTransaction("conn leak test");

	/* This connection is created on the subtransaction memory context */
	Assert(CurrentMemoryContext == CurTransactionContext);
	subconn = get_connection();
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 2);

	remote_connection_exec(conn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 1);

	BeginInternalSubTransaction("conn leak test 2");

	subconn2 = get_connection();
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 3);
	res = remote_connection_exec(subconn2, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 2);

	/* Explicitly close one result */
	remote_result_close(res);

	ASSERT_NUM_OPEN_RESULTS(stats, 1);

	remote_connection_exec(subconn, "SELECT 1");
	remote_connection_exec(conn, "SELECT 1");

	ASSERT_NUM_OPEN_RESULTS(stats, 3);

	RollbackAndReleaseCurrentSubTransaction();
	/* The connection created in the rolled back transaction should be
	 * destroyed */
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 2);

	remote_connection_exec(subconn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 4);

	/* Note that releasing/committing the subtransaction does not delete the memory */
	ReleaseCurrentSubTransaction();

	/* The original and subconn still exists */
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 2);
	ASSERT_NUM_OPEN_RESULTS(stats, 4);

	remote_connection_exec(conn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 5);

	MemoryContextSwitchTo(old);
	MemoryContextDelete(mcxt);

	/* Original connection should be cleaned up along with its 3 results. The
	 * subconn object was created on a subtransaction memory context that will
	 * be cleared when the main transaction ends. */
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 1);
	ASSERT_NUM_OPEN_RESULTS(stats, 2);

	remote_connection_stats_reset();
}

TS_FUNCTION_INFO_V1(ts_test_bad_remote_query);

/* Send a bad query that throws an exception without cleaning up connection or
 * results. Together with get_connection_stats(), this should show that
 * connections and results are automatically cleaned up. */
Datum
ts_test_bad_remote_query(PG_FUNCTION_ARGS)
{
	TSConnection *conn;
	PGresult *result;

	conn = get_connection();
	result = remote_connection_exec(conn, "BADY QUERY SHOULD THROW ERROR");
	TestAssertTrue(PQresultStatus(result) == PGRES_FATAL_ERROR);
	elog(ERROR, "bad query error thrown from test");

	PG_RETURN_VOID();
}

enum Anum_connection_stats
{
	Anum_connection_stats_connections_created = 1,
	Anum_connection_stats_connections_closed,
	Anum_connection_stats_results_created,
	Anum_connection_stats_results_cleared,
	Anum_connection_stats_max,
};

TS_FUNCTION_INFO_V1(ts_test_get_connection_stats);

Datum
ts_test_get_connection_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	RemoteConnectionStats *stats = remote_connection_stats_get();
	Datum values[Anum_connection_stats_max];
	bool nulls[Anum_connection_stats_max] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	values[Anum_connection_stats_connections_created - 1] =
		Int64GetDatum((int64) stats->connections_created);
	values[Anum_connection_stats_connections_closed - 1] =
		Int64GetDatum((int64) stats->connections_closed);
	values[Anum_connection_stats_results_created - 1] =
		Int64GetDatum((int64) stats->results_created);
	values[Anum_connection_stats_results_cleared - 1] =
		Int64GetDatum((int64) stats->results_cleared);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

TS_FUNCTION_INFO_V1(ts_test_remote_connection);

Datum
ts_test_remote_connection(PG_FUNCTION_ARGS)
{
	test_options();
	test_numbers_associated_with_connections();
	test_simple_queries();
	test_connection_and_result_leaks();

	PG_RETURN_VOID();
}

pid_t
remote_connection_get_remote_pid(const TSConnection *conn)
{
	PGresult *res;
	char *pid_string;
	unsigned long pid_long;

	res = PQexec(remote_connection_get_pg_conn(conn), "SELECT pg_backend_pid()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		return -1;

	Assert(1 == PQntuples(res));
	Assert(1 == PQnfields(res));

	pid_string = PQgetvalue(res, 0, 0);
	pid_long = strtol(pid_string, NULL, 10);

	PQclear(res);
	return pid_long;
}

char *
remote_connection_get_application_name(const TSConnection *conn)
{
	PGresult *res;
	char *app_name;

	res = PQexec(remote_connection_get_pg_conn(conn),
				 "SELECT application_name "
				 "FROM pg_stat_activity "
				 "WHERE pid = pg_backend_pid()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		return 0;

	Assert(1 == PQntuples(res));
	Assert(1 == PQnfields(res));

	app_name = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	return app_name;
}
