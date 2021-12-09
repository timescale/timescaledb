/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <miscadmin.h>

#include "remote/txn.h"
#include "export.h"
#include "connection.h"
#include "test_utils.h"

TS_FUNCTION_INFO_V1(ts_test_remote_txn_resolve_create_records);
TS_FUNCTION_INFO_V1(ts_test_remote_txn_resolve_create_prepared_record);
TS_FUNCTION_INFO_V1(ts_test_remote_txn_resolve_create_records_with_concurrent_heal);

static RemoteTxn *
prepared_txn(TSConnectionId *id, const char *sql)
{
	RemoteTxn *tx = palloc0(remote_txn_size());
	memcpy(tx, id, sizeof(*id));
	remote_txn_init(tx, get_connection());
	remote_txn_begin(tx, 1);
	remote_connection_cmd_ok(remote_txn_get_connection(tx), sql);
	remote_txn_write_persistent_record(tx);
	async_request_wait_ok_command(remote_txn_async_send_prepare_transaction(tx));
	return tx;
}

static void
create_commited_txn(TSConnectionId *id)
{
	RemoteTxn *tx =
		prepared_txn(id, "INSERT INTO public.table_modified_by_txns VALUES ('committed');");
	async_request_wait_ok_command(remote_txn_async_send_commit_prepared(tx));
}

static void
create_prepared_txn(TSConnectionId *id)
{
	prepared_txn(id, "INSERT INTO public.table_modified_by_txns VALUES ('prepared not comitted');");
}

static void
create_rollback_prepared_txn(TSConnectionId *id)
{
	RemoteTxn *tx =
		prepared_txn(id, "INSERT INTO public.table_modified_by_txns VALUES ('rollback prepared');");
	remote_txn_abort(tx);
}

Datum
ts_test_remote_txn_resolve_create_records(PG_FUNCTION_ARGS)
{
	TSConnectionId id;

	id.server_id = GetForeignServerByName("loopback", false)->serverid;
	id.user_id = GetUserId();
	create_commited_txn(&id);

	id.server_id = GetForeignServerByName("loopback2", false)->serverid;
	create_prepared_txn(&id);

	id.server_id = GetForeignServerByName("loopback3", false)->serverid;
	create_rollback_prepared_txn(&id);

	PG_RETURN_VOID();
}

/* create an additional prepared gid in a separate transaction */
Datum
ts_test_remote_txn_resolve_create_prepared_record(PG_FUNCTION_ARGS)
{
	TSConnectionId id;

	id.server_id = GetForeignServerByName("loopback", false)->serverid;
	create_prepared_txn(&id);

	PG_RETURN_VOID();
}

static void
send_heal()
{
	remote_connection_query_ok(get_connection(),
							   "SELECT "
							   "_timescaledb_internal.remote_txn_heal_data_node((SELECT "
							   "OID FROM pg_foreign_server WHERE srvname = 'loopback'))");
}

static void
create_commited_txn_with_concurrent_heal(TSConnectionId *id)
{
	RemoteTxn *tx = palloc0(remote_txn_size());
	memcpy(tx, id, sizeof(*id));
	remote_txn_init(tx, get_connection());
	remote_txn_begin(tx, 1);
	remote_connection_cmd_ok(remote_txn_get_connection(tx),
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
ts_test_remote_txn_resolve_create_records_with_concurrent_heal(PG_FUNCTION_ARGS)
{
	TSConnectionId id = { .server_id = GetForeignServerByName("loopback2", false)->serverid,
						  .user_id = GetUserId() };

	create_commited_txn_with_concurrent_heal(&id);

	PG_RETURN_VOID();
}
