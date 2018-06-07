/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <access/xact.h>

#include "remote/txn_id.h"
#include "export.h"
#include "test_utils.h"
#include "remote/txn.h"

TS_FUNCTION_INFO_V1(tsl_test_remote_txn_persistent_record);
static void
test_basic_persistent_record(Oid server_oid, Oid user_mapping_oid)
{
	RemoteTxnId *id = remote_txn_id_create(GetTopTransactionId(), user_mapping_oid);

	Assert(!remote_txn_persistent_record_exists(server_oid, id));
	remote_txn_persistent_record_write(server_oid, user_mapping_oid);
	Assert(remote_txn_persistent_record_exists(server_oid, id));

	remote_txn_persistent_record_delete_for_server(server_oid);
	Assert(!remote_txn_persistent_record_exists(server_oid, id));
}

Datum
tsl_test_remote_txn_persistent_record(PG_FUNCTION_ARGS)
{
	Name server_name = DatumGetName(PG_GETARG_DATUM(0));
	ForeignServer *foreign_server;
	UserMapping *um;

	foreign_server = GetForeignServerByName(NameStr(*server_name), false);
	um = GetUserMapping(GetUserId(), foreign_server->serverid);

	test_basic_persistent_record(foreign_server->serverid, um->umid);
	PG_RETURN_VOID();
}
