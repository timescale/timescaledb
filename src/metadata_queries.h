#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>

typedef struct Chunk Chunk;

Chunk *
			chunk_insert_new(int32 partition_id, int64 timepoint, bool lock);

#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
