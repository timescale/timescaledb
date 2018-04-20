#ifndef TIMESCALEDB_CHUNK_CACHE_H
#define TIMESCALEDB_CHUNK_CACHE_H

#include <postgres.h>
#include "cache.h"
#include "chunk.h"

extern Chunk *chunk_cache_get(Cache *cache, Oid relid, int16 num_constraints);
extern Chunk *chunk_cache_get_entry_rv(Cache *cache, RangeVar *rv, int16 num_constraints);
extern Chunk *chunk_cache_get_by_name(Cache *cache, const char *schema, const char *table, int16 num_constraints);
extern Chunk *chunk_cache_get_or_add(Cache *cache, Chunk *chunk);

extern void chunk_cache_invalidate_callback(void);

extern Cache *chunk_cache_pin(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

#endif							/* TIMESCALEDB_CHUNK_CACHE_H */
