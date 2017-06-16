#include <postgres.h>
#include <catalog/namespace.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>

#include "chunk.h"
#include "catalog.h"
#include "partitioning.h"



Chunk *
chunk_create(HeapTuple tuple, TupleDesc tupdesc, MemoryContext ctx)
{
	Chunk	   *chunk;
	MemoryContext old;

	if (ctx != NULL)
	{
		old = MemoryContextSwitchTo(ctx);
	}

	chunk = palloc0(sizeof(Chunk));

	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));

    chunk->table_id = get_relname_relid(chunk->fd.table_name.data, get_namespace_oid(chunk->fd.schema_name.data, false));

	if (ctx != NULL)
	{
		MemoryContextSwitchTo(old);
	}

	return chunk;
}
