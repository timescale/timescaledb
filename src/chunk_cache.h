#ifndef TIMESCALEDB_CHUNK_CACHE_H
#define TIMESCALEDB_CHUNK_CACHE_H

#include <postgres.h>
#include <executor/spi.h>

#include "metadata_queries.h"
#include "cache.h"

#define CHUNK_CACHE_INVAL_PROXY_TABLE "cache_inval_chunk"
#define CHUNK_CACHE_INVAL_PROXY_OID										\
	get_relname_relid(CHUNK_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID)

typedef struct Partition Partition;
typedef struct Chunk Chunk;

extern Chunk *chunk_cache_get(Cache *cache, Partition *part, int16 num_replicas,
				int64 timepoint, bool lock);
extern Cache *chunk_cache_pin(void);
extern void chunk_cache_invalidate_callback(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

#endif   /* TIMESCALEDB_CHUNK_CACHE_H */
