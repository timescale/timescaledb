/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>

#include "remote/txn_id.h"
#include "export.h"
#include "test_utils.h"

TS_FUNCTION_INFO_V1(tsl_test_remote_txn_id);
static void
test_basic_in_out()
{
	RemoteTxnId *id = remote_txn_id_create(10, 20);

	Assert(id->user_mapping == 20);
	Assert(id->xid = 10);

	id = remote_txn_id_in(remote_txn_id_out(id));
	Assert(id->user_mapping == 20);
	Assert(id->xid = 10);

	Assert(strcmp(remote_txn_id_prepare_transaction_sql(id),
				  "PREPARE TRANSACTION \'ts-1-10-20\'") == 0);
	Assert(strcmp(remote_txn_id_commit_prepared_sql(id), "COMMIT PREPARED \'ts-1-10-20\'") == 0);
	Assert(strcmp(remote_txn_id_rollback_prepared_sql(id), "ROLLBACK PREPARED \'ts-1-10-20\'") ==
		   0);
}

Datum
tsl_test_remote_txn_id(PG_FUNCTION_ARGS)
{
	test_basic_in_out();
	PG_RETURN_VOID();
}
