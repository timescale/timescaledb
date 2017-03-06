#ifndef TIMESCALEDB_CHUNK_CACHE_H
#define TIMESCALEDB_CHUNK_CACHE_H

#include <postgres.h>
#include <executor/spi.h>

#include "metadata_queries.h"
#include "cache.h"

#define CHUNK_CACHE_INVAL_PROXY_TABLE "cache_inval_chunk"
#define CHUNK_CACHE_INVAL_PROXY_OID                                     \
	get_relname_relid(CHUNK_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID)

typedef struct hypertable_cache_entry hypertable_cache_entry;
typedef struct epoch_and_partitions_set epoch_and_partitions_set;
typedef struct Partition Partition;
typedef struct chunk_row chunk_row;

typedef struct chunk_cache_entry
{
	int32       id;
	chunk_row  *chunk;
	crn_set    *crns;
} chunk_cache_entry;
	
	
extern chunk_cache_entry *get_chunk_cache_entry(Cache *cache, Partition *part, int64 timepoint, bool lock);

extern void chunk_crn_set_cache_invalidate_callback(void);

extern Cache *chunk_crn_set_cache_pin(void); 
extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

#endif   /* TIMESCALEDB_CHUNK_CACHE_H */
