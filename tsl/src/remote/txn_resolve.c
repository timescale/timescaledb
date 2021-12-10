/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <utils/guc.h>
#include <utils/snapmgr.h>
#include <utils/fmgroids.h>
#include <access/xact.h>
#include <access/transam.h>
#include <miscadmin.h>

#include "txn_resolve.h"
#include "connection.h"
#include "txn.h"

RemoteTxnResolution
remote_txn_resolution(Oid foreign_server, const RemoteTxnId *transaction_id)
{
	if (remote_txn_is_still_in_progress_on_access_node(transaction_id->xid))
		/* transaction still ongoing; don't know its state */
		return REMOTE_TXN_RESOLUTION_IN_PROGRESS;

	/*
	 * If an entry exists in the "remote_txn" table and is visible then it means
	 * that the transaction committed on the AN
	 */
	if (remote_txn_persistent_record_exists(transaction_id))
		return REMOTE_TXN_RESOLUTION_COMMIT;

	/*
	 * If the txn is not in progress and is not committed as per the "remote_txn"
	 * table then it's presumed to be aborted.
	 *
	 * We could ask PG machinery to confirm the abort but as long as we are sticking
	 * to one uniform behavior consistently it should be ok for now.
	 */
	return REMOTE_TXN_RESOLUTION_ABORT;
}

/*
 * Resolve any unresolved 2-pc transaction on a data node.
 * Since the remote_txn log can be long, and most txn there
 * will have been resolved, do not iterate that list.
 *
 * Instead query the data node for the list of unresolved txns
 * via the pg_prepared_xacts view. Using that list, then check
 * remote_txn. Use this as an opportunity to clean up remote_txn
 * as well.
 *
 * Note that pg_prepared_xacts shared across other databases which
 * also could be distributed. Right now we interested only in
 * the current one.
 */
#define GET_PREPARED_XACT_SQL                                                                      \
	"SELECT gid FROM pg_prepared_xacts WHERE database = current_database()"

Datum
remote_txn_heal_data_node(PG_FUNCTION_ARGS)
{
	Oid foreign_server_oid = PG_GETARG_OID(0);
	TSConnection *conn = remote_connection_open(foreign_server_oid, GetUserId());
	int resolved = 0;

	/*
	 * Use a raw connection since you need to be out of transaction to do
	 * COMMIT/ROLLBACK PREPARED
	 */
	PGresult *res;
	int row;
	List *in_progress_txn_gids = NIL, *healed_txn_gids = NIL;
	int non_ts_txns = 0, ntuples;
#ifdef TS_DEBUG
	int n_gid_errors = 0; /* how many errors to induce? */
#endif

	/*
	 * This function cannot be called inside a transaction block since effects
	 * cannot be rolled back
	 */
	PreventInTransactionBlock(true, "remote_txn_heal_data_node");

	res = remote_connection_query_ok(conn, GET_PREPARED_XACT_SQL);

	Assert(1 == PQnfields(res));
	ntuples = PQntuples(res);
	for (row = 0; row < ntuples; row++)
	{
		char *id_string = PQgetvalue(res, row, 0);
		RemoteTxnId *tpc_gid;
		RemoteTxnResolution resolution;

		if (!remote_txn_id_matches_prepared_txn(id_string))
		{
			non_ts_txns++;
			continue;
		}

		tpc_gid = remote_txn_id_in(id_string);
		resolution = remote_txn_resolution(foreign_server_oid, tpc_gid);

#ifdef TS_DEBUG
		/*
		 * Induce an error in the GID so that the remote side errors out when it tries
		 * to heal it.
		 *
		 * We inject the error by checking the value of the below session variable. Not
		 * a full GUC, just a tool to allow us to randomly inject error for testing
		 * purposes. Depending on the value we will inject an error in the GID and also
		 * additionally change the resolution as per the accepted value:
		 *
		 * "commit"  : change GID + set resolution as COMMITTED
		 * "abort"   : change GID + set resolution as ABORTED
		 * "inprogress" : set resolution as IN_PROGRESS
		 *
		 * Any other setting will not have any effect
		 *
		 * We currently induce error in one GID processing. If needed this can be
		 * changed in the future via another session variable to set to a specific
		 * number of errors to induce. Note that this variable is incremented only
		 * for valid values of "timescaledb.debug_inject_gid_error.
		 *
		 * Current logic also means that the first GID being processed will always
		 * induce a change in resolution behavior. But that's ok, we could randomize
		 * it later to any arbitrary integer value less than ntuples in the future.
		 */
		if (n_gid_errors < 1)
		{
			const char *inject_gid_error =
				GetConfigOption("timescaledb.debug_inject_gid_error", true, false);

			/* increment the user_id field to cause mismatch in GID */
			if (inject_gid_error)
			{
				if (strcmp(inject_gid_error, "abort") == 0)
				{
					tpc_gid->id.user_id++;
					resolution = REMOTE_TXN_RESOLUTION_ABORT;
					n_gid_errors++;
				}
				else if (strcmp(inject_gid_error, "commit") == 0)
				{
					tpc_gid->id.user_id++;
					resolution = REMOTE_TXN_RESOLUTION_COMMIT;
					n_gid_errors++;
				}
				else if (strcmp(inject_gid_error, "inprogress") == 0)
				{
					resolution = REMOTE_TXN_RESOLUTION_IN_PROGRESS;
					n_gid_errors++;
				}
				/* any other value is simply ignored, n_gid_errors is also not incremented */
			}
		}
#endif
		/*
		 * We don't expect these commands to fail, but if they do, continue and move on to
		 * healing up the next GID in the list. The ones that failed will get retried if
		 * they are still around on the datanodes the next time over.
		 */
		switch (resolution)
		{
			case REMOTE_TXN_RESOLUTION_COMMIT:
				if (PQresultStatus(
						remote_connection_exec(conn, remote_txn_id_commit_prepared_sql(tpc_gid))) ==
					PGRES_COMMAND_OK)
				{
					healed_txn_gids = lappend(healed_txn_gids, id_string);
					resolved++;
				}
				else
					ereport(WARNING,
							(errmsg("could not commit prepared transaction on data node \"%s\"",
									remote_connection_node_name(conn)),
							 errhint("To retry, manually run \"COMMIT PREPARED %s\" on the data "
									 "node or run the healing function again.",
									 id_string)));
				break;
			case REMOTE_TXN_RESOLUTION_ABORT:
				if (PQresultStatus(remote_connection_exec(conn,
														  remote_txn_id_rollback_prepared_sql(
															  tpc_gid))) == PGRES_COMMAND_OK)
				{
					healed_txn_gids = lappend(healed_txn_gids, id_string);
					resolved++;
				}
				else
					ereport(WARNING,
							(errmsg("could not roll back prepared transaction on data node \"%s\"",
									remote_connection_node_name(conn)),
							 errhint("To retry, manually run \"ROLLBACK PREPARED %s\" on the data "
									 "node or run the healing function again.",
									 id_string)));
				break;
			case REMOTE_TXN_RESOLUTION_IN_PROGRESS:
				in_progress_txn_gids = lappend(in_progress_txn_gids, id_string);
				break;
		}
	}

	if (non_ts_txns > 0)
		elog(NOTICE, "skipping %d non-TimescaleDB prepared transaction", non_ts_txns);

	/*
	 * Perform cleanup of all records if there are no in progress txns and if the number of
	 * resolved entities is same as the number of rows obtained from the datanode.
	 *
	 * In a heavily loaded system there's a possibility of ongoing transactions always being
	 * present in which case we will never get a chance to clean up entries in "remote_txn"
	 * table. So, we track healed gids in a list and delete those specific rows to keep the
	 * "remote_txn" table from growing up indefinitely.
	 */
	if (list_length(in_progress_txn_gids) == 0 && resolved == ntuples)
		remote_txn_persistent_record_delete_for_data_node(foreign_server_oid, NULL);
	else if (resolved)
	{
		ListCell *lc;
		Assert(healed_txn_gids != NIL);

		foreach (lc, healed_txn_gids)
			remote_txn_persistent_record_delete_for_data_node(foreign_server_oid, lfirst(lc));
	}

	remote_result_close(res);
	remote_connection_close(conn);
	PG_RETURN_INT32(resolved);
}
