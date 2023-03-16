/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>

#include "export.h"
#include "chunk.h"

TS_FUNCTION_INFO_V1(ts_test_merge_chunks_on_dimension);

Datum
ts_test_merge_chunks_on_dimension(PG_FUNCTION_ARGS)
{
	Oid chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);

	Oid merge_chunk_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);

	int32 dimension_id = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);

	Chunk *chunk = ts_chunk_get_by_relid(chunk_id, true);
	Chunk *merge_chunk = ts_chunk_get_by_relid(merge_chunk_id, true);
	Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
	ts_chunk_merge_on_dimension(ht, chunk, merge_chunk, dimension_id);

	PG_RETURN_VOID();
}
