/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <utils/tqual.h>
#include <utils/snapmgr.h>
#include <utils/fmgroids.h>
#include <access/xact.h>
#include <access/transam.h>
#include <miscadmin.h>

#include "txn_resolve.h"
#include "connection.h"
#include "compat.h"
#include "txn.h"

RemoteTxnResolution
remote_txn_resolution(Oid foreign_server, const RemoteTxnId *transaction_id)
{
	if (remote_txn_is_still_in_progress(transaction_id->xid))
		/* transaction still ongoing; don't know it's state */
		return REMOTE_TXN_RESOLUTION_UNKNOWN;

	if (remote_txn_persistent_record_exists(transaction_id))
		return REMOTE_TXN_RESOLUTION_COMMT;

	return REMOTE_TXN_RESOLUTION_ABORT;
}

/* Resolve any unresolved 2-pc transaction on a remote server.
   Since the remote_txn log can be long, and most txn there
   will have been resolved, do not iterate that list.
   Instead query the remote server for the list of unresolved txns
   via the pg_prepared_xacts view. Using that list, then check
   remote_txn. Use this as an opportunity to clean up remote_txn
   as well.
*/
#define GET_PREPARED_XACT_SQL "SELECT gid FROM pg_prepared_xacts"

Datum
remote_txn_heal_server(PG_FUNCTION_ARGS)
{
	Oid foreign_server_oid = PG_GETARG_OID(0);
	UserMapping *user = GetUserMapping(GetUserId(), foreign_server_oid);
	ForeignServer *server = GetForeignServer(foreign_server_oid);
	int resolved = 0;

	/*
	 * Use a raw connection since you need to be out of transaction to do
	 * COMMIT/ROLLBACK PREPARED
	 */
	TSConnection *conn =
		remote_connection_open(server->servername, server->options, user->options, true);
	PGresult *res;
	int row;
	List *unknown_txn_gid = NIL;
	int non_ts_txns = 0;

	/*
	 * This function cannot be called inside a transaction block since effects
	 * cannot be rolled back
	 */
	PreventInTransactionBlock(true, "remote_txn_heal_server");

	res = remote_connection_query_ok_result(conn, GET_PREPARED_XACT_SQL);

	Assert(1 == PQnfields(res));
	for (row = 0; row < PQntuples(res); row++)
	{
		const char *id_string = PQgetvalue(res, row, 0);
		RemoteTxnId *tpc_gid;
		RemoteTxnResolution resolution;

		if (!remote_txn_id_matches_prepared_txn(id_string))
		{
			non_ts_txns++;
			continue;
		}

		tpc_gid = remote_txn_id_in(id_string);
		resolution = remote_txn_resolution(foreign_server_oid, tpc_gid);

		switch (resolution)
		{
			case REMOTE_TXN_RESOLUTION_COMMT:
				remote_connection_exec_ok_command(conn, remote_txn_id_commit_prepared_sql(tpc_gid));
				resolved++;
				break;
			case REMOTE_TXN_RESOLUTION_ABORT:
				remote_connection_exec_ok_command(conn,
												  remote_txn_id_rollback_prepared_sql(tpc_gid));
				resolved++;
				break;
			case REMOTE_TXN_RESOLUTION_UNKNOWN:
				unknown_txn_gid = lappend(unknown_txn_gid, tpc_gid);
				break;
		}
	}

	if (non_ts_txns > 0)
		elog(NOTICE, "skipping %d non-TimescaleDB prepared transaction", non_ts_txns);

	remote_connection_result_close(res);

	/*
	 * Perform cleanup of all records if there are no unknown txns.
	 */
	if (list_length(unknown_txn_gid) == 0)
		remote_txn_persistent_record_delete_for_server(foreign_server_oid);

	remote_connection_close(conn);
	PG_RETURN_INT32(resolved);
}
