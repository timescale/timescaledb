/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <access/htup_details.h>

#include "tuptable.h"

HeapTuple
ts_exec_fetch_slot_heap_tuple(TupleTableSlot *slot, bool materialize, bool *should_free)
{
	if (should_free)
		*should_free = false;

	return ExecFetchSlotTuple(slot);
}

void
ts_tuptableslot_set_table_oid(TupleTableSlot *slot, Oid table_oid)
{
	if (table_oid != InvalidOid)
	{
		HeapTuple tuple = ExecFetchSlotTuple(slot);
		tuple->t_tableOid = table_oid;
	}
}
