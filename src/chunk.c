#include <postgres.h>
#include <catalog/namespace.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

#include "chunk.h"

static Datum
chunk_replica_size_bytes_internal(const char *schema, const char *table)
{
	Oid			relOid = get_relname_relid(table, get_namespace_oid(schema, false));

	return DirectFunctionCall1(pg_table_size, ObjectIdGetDatum(relOid));
}

int64
chunk_replica_size_bytes(ChunkReplica * cr)
{
	Datum		size = chunk_replica_size_bytes_internal(cr->schema_name, cr->table_name);

	return DatumGetInt64(size);
}

Datum
			local_chunk_size(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(local_chunk_size);
Datum
local_chunk_size(PG_FUNCTION_ARGS)
{
	Name		schema = PG_GETARG_NAME(0);
	Name		table = PG_GETARG_NAME(1);

	return chunk_replica_size_bytes_internal(schema->data, table->data);
}

bool
chunk_timepoint_is_member(const Chunk * chunk, const int64 time_pt)
{
	return chunk->start_time <= time_pt && chunk->end_time >= time_pt;
}

extern ChunkReplica *
chunk_get_replica(Chunk * chunk, const char *dbname)
{
	int			i;

	for (i = 0; i < chunk->num_replicas; i++)
	{
		ChunkReplica *cr = &chunk->replicas[i];

		if (strncmp(cr->database_name, dbname, NAMEDATALEN) == 0)
		{
			return cr;
		}
	}
	return NULL;
}

Chunk *
chunk_create(int32 id, int32 partition_id, int64 starttime, int64 endtime, int16 num_replicas)
{
	Chunk	   *chunk;

	chunk = palloc(sizeof(Chunk));
	chunk->id = id;
	chunk->partition_id = partition_id;
	chunk->start_time = starttime;
	chunk->end_time = endtime;
	chunk->num_replicas = num_replicas;

	return chunk;
}
