/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>

#include "remote/txn.h"
#include "export.h"
#include "test_utils.h"

TS_FUNCTION_INFO_V1(tsl_test_remote_txn_resolve_create_records);
TS_FUNCTION_INFO_V1(tsl_test_remote_txn_resolve_create_records_with_concurrent_heal);

static RemoteTxn *
prepared_txn(UserMapping *um, const char *sql)
{
	RemoteTxn *tx = palloc0(remote_txn_size());
	memcpy(tx, &um->umid, sizeof(Oid));
	remote_txn_init(tx, get_connection(), um);
	remote_txn_begin(tx, 1);
	remote_connection_query_ok_result(remote_txn_get_connection(tx), sql);
	remote_txn_write_persistent_record(tx);
	async_request_wait_ok_command(remote_txn_async_send_prepare_transaction(tx));
	return tx;
}

static void
create_commited_txn(UserMapping *um)
{
	RemoteTxn *tx =
		prepared_txn(um, "INSERT INTO public.table_modified_by_txns VALUES ('committed');");
	async_request_wait_ok_command(remote_txn_async_send_commit_prepared(tx));
}

static void
create_prepared_txn(UserMapping *um)
{
	prepared_txn(um, "INSERT INTO public.table_modified_by_txns VALUES ('prepared not comitted');");
}

static void
create_rollback_prepared_txn(UserMapping *um)
{
	RemoteTxn *tx =
		prepared_txn(um, "INSERT INTO public.table_modified_by_txns VALUES ('rollback prepared');");
	remote_txn_abort(tx);
}

Datum
tsl_test_remote_txn_resolve_create_records(PG_FUNCTION_ARGS)
{
	ForeignServer *foreign_server;
	UserMapping *um;

	foreign_server = GetForeignServerByName("loopback", false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	create_commited_txn(um);

	foreign_server = GetForeignServerByName("loopback2", false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	create_prepared_txn(um);

	foreign_server = GetForeignServerByName("loopback3", false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	create_rollback_prepared_txn(um);

	PG_RETURN_VOID();
}

static void
send_heal()
{
	remote_connection_query_ok_result(get_connection(),
									  "SELECT _timescaledb_internal.remote_txn_heal_server((SELECT "
									  "OID FROM pg_foreign_server WHERE srvname = 'loopback'))");
}

static void
create_commited_txn_with_concurrent_heal(UserMapping *um)
{
	RemoteTxn *tx = palloc0(remote_txn_size());
	memcpy(tx, &um->umid, sizeof(Oid));
	remote_txn_init(tx, get_connection(), um);
	remote_txn_begin(tx, 1);
	remote_connection_query_ok_result(remote_txn_get_connection(tx),
									  "INSERT INTO public.table_modified_by_txns VALUES "
									  "('committed with concurrent heal');");
	send_heal();
	remote_txn_write_persistent_record(tx);
	send_heal();
	async_request_wait_ok_command(remote_txn_async_send_prepare_transaction(tx));
	send_heal();
	async_request_wait_ok_command(remote_txn_async_send_commit_prepared(tx));
	send_heal();
}

Datum
tsl_test_remote_txn_resolve_create_records_with_concurrent_heal(PG_FUNCTION_ARGS)
{
	ForeignServer *foreign_server;
	UserMapping *um;

	foreign_server = GetForeignServerByName("loopback2", false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);
	create_commited_txn_with_concurrent_heal(um);

	PG_RETURN_VOID();
}
