#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>

typedef struct Chunk
{
	int32		id;
	int32		partition_id;
	int64		start_time;
	int64		end_time;
	char		schema_name[NAMEDATALEN];
	char		table_name[NAMEDATALEN];
	Oid			table_id;
} Chunk;

extern bool chunk_timepoint_is_member(const Chunk *row, const int64 time_pt);
extern Chunk *chunk_create(HeapTuple tuple, TupleDesc tupdesc, MemoryContext ctx);

#endif   /* TIMESCALEDB_CHUNK_H */
