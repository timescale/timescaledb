/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <access/xact.h>
#include <miscadmin.h>

#include "remote/connection.h"
#include "remote/txn_id.h"
#include "export.h"
#include "connection.h"
#include "test_utils.h"
#include "remote/txn.h"

TS_FUNCTION_INFO_V1(ts_test_remote_txn_persistent_record);

static void
test_basic_persistent_record(TSConnectionId cid)
{
	RemoteTxnId *id = remote_txn_id_create(GetTopTransactionId(), cid);

	TestAssertTrue(!remote_txn_persistent_record_exists(id));

	remote_txn_persistent_record_write(cid);
	TestAssertTrue(remote_txn_persistent_record_exists(id));
	/* delete by just specifying the data node */
	remote_txn_persistent_record_delete_for_data_node(cid.server_id, NULL);
	TestAssertTrue(!remote_txn_persistent_record_exists(id));

	remote_txn_persistent_record_write(cid);
	TestAssertTrue(remote_txn_persistent_record_exists(id));
	/* delete by specifying the exact GID */
	remote_txn_persistent_record_delete_for_data_node(cid.server_id, remote_txn_id_out(id));
	TestAssertTrue(!remote_txn_persistent_record_exists(id));
}

Datum
ts_test_remote_txn_persistent_record(PG_FUNCTION_ARGS)
{
	Name server_name = DatumGetName(PG_GETARG_DATUM(0));
	TSConnectionId id =
		remote_connection_id(GetForeignServerByName(NameStr(*server_name), false)->serverid,
							 GetUserId());

	test_basic_persistent_record(id);
	PG_RETURN_VOID();
}
