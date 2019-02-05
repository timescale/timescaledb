/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <extension.h>

#include "../../src/hypertable.h"

TS_FUNCTION_INFO_V1(tsl_test_assign_server);

Datum
tsl_test_assign_server(PG_FUNCTION_ARGS)
{
	Oid table_id = PG_GETARG_OID(0);
	const char *server_name = PG_GETARG_NAME(1)->data;
	Hypertable *ht = ts_hypertable_get_by_id(ts_hypertable_relid_to_id(table_id));

	hypertable_assign_servers(ht->fd.id, list_make1((char *) server_name));
	PG_RETURN_VOID();
}
