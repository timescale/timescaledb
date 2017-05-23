#ifndef TIMESCALEDB_CHUNK_CACHE_H
#define TIMESCALEDB_CHUNK_CACHE_H

#include <postgres.h>
#include <executor/spi.h>

#include "metadata_queries.h"
#include "cache.h"

typedef struct Partition Partition;
typedef struct Chunk Chunk;

extern Chunk *chunk_cache_get(Cache *cache, Partition *part, int64 timepoint);
extern Cache *chunk_cache_pin(void);
extern void chunk_cache_invalidate_callback(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

#endif   /* TIMESCALEDB_CHUNK_CACHE_H */
