#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>

typedef struct Chunk Chunk;

extern Chunk *
chunk_get_or_create(int32 time_dimension_id, int64 time_value,
				 int32 space_dimension_id, int64 space_value);
#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
