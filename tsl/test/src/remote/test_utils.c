/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <common/username.h>

#include "test_utils.h"
#include "remote/connection.h"
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <miscadmin.h>

static const char *sql_get_backend_pid = "SELECT pg_backend_pid()";
static const char *sql_get_application_name =
	"SELECT application_name from  pg_stat_activity where pid = pg_backend_pid()";

TSConnection *
get_connection()
{
	return remote_connection_open("testdb",
								  list_make3(makeDefElem("user",
														 (Node *) makeString(
															 GetUserNameFromId(GetUserId(), false)),
														 -1),
											 makeDefElem("dbname",
														 (Node *) makeString(
															 get_database_name(MyDatabaseId)),
														 -1),
											 makeDefElem("port",
														 (Node *) makeString(
															 pstrdup(GetConfigOption("port",
																					 false,
																					 false))),
														 -1)),
								  NIL,
								  false);
}

pid_t
remote_connecton_get_remote_pid(TSConnection *conn)
{
	PGresult *res;
	char *pid_string;
	unsigned long pid_long;

	res = PQexec(remote_connection_get_pg_conn(conn), sql_get_backend_pid);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		return 0;

	Assert(1 == PQntuples(res));
	Assert(1 == PQnfields(res));

	pid_string = PQgetvalue(res, 0, 0);
	pid_long = strtol(pid_string, NULL, 10);

	PQclear(res);
	return pid_long;
}

char *
remote_connecton_get_application_name(TSConnection *conn)
{
	PGresult *res;
	char *app_name;

	res = PQexec(remote_connection_get_pg_conn(conn), sql_get_application_name);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		return 0;

	Assert(1 == PQntuples(res));
	Assert(1 == PQnfields(res));

	app_name = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	return app_name;
}
