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

#include "export.h"
#include "remote/dist_txn.h"
#include "remote/connection.h"

TS_FUNCTION_INFO_V1(ts_remote_exec);

Datum
ts_remote_exec(PG_FUNCTION_ARGS)
{
	Name server_name = DatumGetName(PG_GETARG_DATUM(0));
	char *sql = TextDatumGetCString(PG_GETARG_DATUM(1));
	ForeignServer *foreign_server;
	UserMapping *um;
	PGconn *conn;

	foreign_server = GetForeignServerByName(NameStr(*server_name), false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	conn = remote_dist_txn_get_connection(um, REMOTE_TXN_USE_PREP_STMT);

	remote_connection_exec_ok_command(conn, sql);

	PG_RETURN_VOID();
}
