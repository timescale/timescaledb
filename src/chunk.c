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
	MemoryContext old;

	if (ctx != NULL)
	{
		old = MemoryContextSwitchTo(ctx);
	}

	chunk = palloc0(sizeof(Chunk));

	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));

	if (ctx != NULL)
	{
		MemoryContextSwitchTo(old);
	}

	return chunk;
}
