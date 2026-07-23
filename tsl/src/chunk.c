/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/table.h>
#include <catalog/catalog.h>
#include <catalog/heap.h>
#include <catalog/namespace.h>
#include <catalog/pg_class.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <fmgr.h>
#include <foreign/foreign.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/lockoptions.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/value.h>
#include <parser/parse_coerce.h>
#include <parser/parse_func.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "chunk.h"
#include "compression/compression.h"
#include "debug_point.h"
#include "utils.h"

/* Data in a frozen chunk cannot be modified. So any operation
 * that rewrites data for a frozen chunk will be blocked.
 * Note that a frozen chunk can still be dropped.
 */
Datum
chunk_freeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on tiered chunk \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (ts_chunk_is_frozen(chunk))
	{
		PG_RETURN_BOOL(true);
	}
	/* get Share lock. will wait for other concurrent transactions that are
	 * modifying the chunk. Does not block SELECTs on the chunk.
	 * Does not block other DDL on the chunk table.
	 */
	DEBUG_WAITPOINT("freeze_chunk_before_lock");
	LockRelationOid(chunk_relid, ShareLock);
	bool ret = ts_chunk_set_frozen(chunk);
	PG_RETURN_BOOL(ret);
}

Datum
chunk_unfreeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on foreign table \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (!ts_chunk_is_frozen(chunk))
	{
		PG_RETURN_BOOL(true);
	}
	/* This is a previously frozen chunk. Only selects are permitted on this chunk.
	 * This changes the status in the catalog to allow previously blocked operations.
	 */
	bool ret = ts_chunk_unset_frozen(chunk);
	PG_RETURN_BOOL(ret);
}
