#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>

typedef struct ChunkReplica
{
	Oid			schema_id;
	Oid			table_id;
	char		database_name[NAMEDATALEN];
	char		schema_name[NAMEDATALEN];
	char		table_name[NAMEDATALEN];
}	ChunkReplica;

typedef struct Chunk
{
	int32		id;
	int32		partition_id;
	int64		start_time;
	int64		end_time;
	int16		num_replicas;
	ChunkReplica *replicas;
}	Chunk;

extern bool chunk_timepoint_is_member(const Chunk * row, const int64 time_pt);
extern int64 chunk_replica_size_bytes(ChunkReplica * cr);
extern ChunkReplica *chunk_get_replica(Chunk * cunk, const char *dbname);
extern Chunk *chunk_create(int32 id, int32 partition_id, int64 starttime, int64 endtime, int16 num_replicas);

#endif   /* TIMESCALEDB_CHUNK_H */
