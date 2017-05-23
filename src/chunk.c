#include <postgres.h>
#include <catalog/namespace.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>

#include "chunk.h"
#include "catalog.h"
#include "partitioning.h"

bool
chunk_timepoint_is_member(const Chunk *chunk, const int64 time_pt)
{
	return chunk->start_time <= time_pt && chunk->end_time >= time_pt;
}

Chunk *
chunk_create(HeapTuple tuple, TupleDesc tupdesc, MemoryContext ctx)
{
	Chunk	   *chunk;
	bool		is_null;
	Datum		datum;


	MemoryContext old;

	if (ctx != NULL)
	{
		old = MemoryContextSwitchTo(ctx);
	}

	chunk = palloc(sizeof(Chunk));

	datum = heap_getattr(tuple, Anum_chunk_id, tupdesc, &is_null);
	Assert(!is_null);
	chunk->id = DatumGetInt32(datum);

	datum = heap_getattr(tuple, Anum_chunk_start_time, tupdesc, &is_null);
	chunk->start_time = is_null ? OPEN_START_TIME : DatumGetInt64(datum);

	datum = heap_getattr(tuple, Anum_chunk_end_time, tupdesc, &is_null);
	chunk->end_time = is_null ? OPEN_END_TIME : DatumGetInt64(datum);

	datum = heap_getattr(tuple, Anum_chunk_schema_name, tupdesc, &is_null);
	Assert(!is_null);
	strncpy(chunk->schema_name, DatumGetCString(datum), NAMEDATALEN);

	datum = heap_getattr(tuple, Anum_chunk_table_name, tupdesc, &is_null);
	Assert(!is_null);
	strncpy(chunk->table_name, DatumGetCString(datum), NAMEDATALEN);

	chunk->table_id = get_relname_relid(chunk->table_name, get_namespace_oid(chunk->schema_name, false));
	Assert(OidIsValid(chunk->table_id));

	if (ctx != NULL)
	{
		MemoryContextSwitchTo(old);
	}

	return chunk;
}
